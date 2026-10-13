from __future__ import annotations

from contextlib import contextmanager, redirect_stdout
from copy import deepcopy
from io import StringIO
import json
import os
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

import dataset_production_store as store
from tools import build_canonical_root_inventory as inventory
from tools import fetch_canonical_missing_metadata as metadata_fetch


class InventoryBatchTests(unittest.TestCase):
    def test_duplicate_reports_preserve_nonempty_metadata_and_first_membership(self):
        first = {"accession": "GCA_990000001.1"}
        full = {"accession": first["accession"], "organism": {"organism_name": "Example organism", "tax_id": 123},
                "assembly_info": {"biosample": {"accession": "SAMN00000001"}, "paired_assembly": {"accession": "GCF_990000001.1"}}}
        records = [first, full, first, {"accession": "GCF_990000001.1"}]
        before = deepcopy(records)
        rows, invalid, valid = store.coalesce_inventory_records(iter(records))
        expected = store.normalized_inventory_record(full)
        self.assertEqual((len(rows), invalid, valid), (1, 1, 3))
        for field in ("organism_name", "tax_id", "biosample_accession", "paired_refseq_accession", "raw_fingerprint", "raw_payload"):
            self.assertEqual(rows[0][field], expected[field])
        self.assertEqual(rows[0]["membership_fingerprint"], store.normalized_inventory_record(first)["raw_fingerprint"])
        self.assertEqual(records, before)

    def test_later_informative_report_updates_raw_payload_but_retains_missing_fields(self):
        first = {"accession": "GCA_990000001.1", "organism": {"organism_name": "Original", "tax_id": 123},
                 "assembly_info": {"biosample": {"accession": "SAMN00000001"}}}
        later = {"accession": first["accession"], "organism": {"organism_name": "Updated", "tax_id": 0}}
        rows, _, _ = store.coalesce_inventory_records([first, later])
        self.assertEqual(rows[0]["organism_name"], "Updated")
        self.assertEqual(rows[0]["tax_id"], 123)
        self.assertEqual(rows[0]["biosample_accession"], "SAMN00000001")
        self.assertEqual(rows[0]["raw_payload"], later)

    def test_invalid_only_batch_does_not_open_database(self):
        with patch.object(store, "connect") as connect:
            self.assertEqual(store.insert_inventory_batch("fixture", [{"accession": "GCF_990000001.1"}, {}]), (0, 2, 0))
        connect.assert_not_called()

    def test_writer_uses_two_statements_and_counts_existing_memberships(self):
        connection = MagicMock()
        connection.execute.return_value.fetchone.return_value = (1,)
        records = [{"accession": "GCA_990000001.1"}, {"accession": "GCA_990000001.1"}, {"accession": "GCA_990000002.1"}]
        with patch.object(store, "connect") as connect:
            connect.return_value.__enter__.return_value = connection
            self.assertEqual(store.insert_inventory_batch("fixture", records), (1, 0, 2))
        self.assertEqual(connection.execute.call_count, 2)
        connection.commit.assert_called_once()
        master_rows = connection.execute.call_args_list[0].args[1][-1].obj
        membership_rows = connection.execute.call_args_list[1].args[1][-1].obj
        self.assertEqual(len(master_rows), 2)
        self.assertEqual(len(membership_rows), 2)

    def test_failed_membership_write_does_not_commit(self):
        connection = MagicMock()
        connection.execute.side_effect = [MagicMock(), RuntimeError("membership failure")]
        with patch.object(store, "connect") as connect:
            connect.return_value.__enter__.return_value = connection
            with self.assertRaisesRegex(RuntimeError, "membership failure"):
                store.insert_inventory_batch("fixture", [{"accession": "GCA_990000001.1"}])
        connection.commit.assert_not_called()
        self.assertIs(connect.return_value.__exit__.call_args.args[0], RuntimeError)


class InventoryHTTPTests(unittest.TestCase):
    def test_metadata_batches_explicitly_request_every_report(self):
        accessions = [f"GCA_{990000000 + index}.1" for index in range(100)]
        reports = [{"accession": accession} for accession in accessions]
        with patch.object(metadata_fetch.urllib.request, "urlopen") as open_url:
            open_url.return_value.__enter__.return_value = StringIO(json.dumps({"reports": reports}))
            result = metadata_fetch.fetch_reports(accessions, api_key="", max_attempts=1, retry_sleep=0, timeout=30)
        self.assertEqual(result, reports)
        self.assertIn("page_size=100", open_url.call_args.args[0].full_url)
        open_url.assert_called_once()

    def test_fetch_reuses_provided_session_and_preserves_filters(self):
        client = MagicMock()
        response = client.get.return_value.__enter__.return_value
        response.json.return_value = {"reports": [{"accession": "GCA_990000001.1"}]}
        args = SimpleNamespace(page_size=1000, api_key="fixture-key", max_attempts=2, request_timeout=30, retry_sleep=0)
        with patch.object(inventory.requests, "Session") as factory:
            result = inventory.fetch_page(args, "opaque-checkpoint", client=client)
        factory.assert_not_called()
        self.assertEqual(len(result["reports"]), 1)
        params = client.get.call_args.kwargs["params"]
        self.assertEqual(params["returned_content"], "ASSM_ACC")
        self.assertEqual(params["filters.assembly_source"], "GENBANK")
        self.assertEqual(params["filters.assembly_version"], "CURRENT")
        self.assertEqual(params["page_token"], "opaque-checkpoint")
        response.raise_for_status.assert_called_once()

    def test_network_error_is_retried_on_same_session(self):
        client = MagicMock()
        response = MagicMock()
        response.__enter__.return_value = response
        response.json.return_value = {"reports": []}
        client.get.side_effect = [inventory.requests.Timeout("fixture timeout"), response]
        args = SimpleNamespace(page_size=1000, api_key="", max_attempts=2, request_timeout=30, retry_sleep=0)
        with patch.object(inventory.time, "sleep"):
            self.assertEqual(inventory.fetch_page(args, None, client=client), {"reports": []})
        self.assertEqual(client.get.call_count, 2)

    def test_resumed_scan_keeps_one_session_and_page_checkpoint_order(self):
        client = MagicMock()
        response = client.get.return_value.__enter__.return_value
        response.json.side_effect = [
            {"reports": [{"accession": "GCA_990000008.1"}], "total_count": 9, "next_page_token": "page-9"},
            {"reports": [{"accession": "GCA_990000009.1"}], "total_count": 9},
        ]
        with patch.object(inventory.requests, "Session") as session, patch.object(
            inventory, "start_inventory_snapshot"
        ), patch.object(inventory, "latest_inventory_page_checkpoint", return_value={"page_number": 7, "next_page_token": "page-8"}), patch.object(
            inventory, "start_inventory_page"
        ) as start, patch.object(inventory, "finish_inventory_page") as finish, patch.object(
            inventory, "insert_inventory_batch", return_value=(1, 0, 0)
        ), patch.object(inventory, "inventory_page_progress", return_value={"raw_records": 9, "noncanonical_records": 0, "duplicate_records": 0}), patch.object(
            inventory, "finish_inventory_snapshot", return_value={"status": "completed"}
        ), patch("sys.argv", ["inventory", "--snapshot-id", "fixture"]), redirect_stdout(StringIO()):
            session.return_value.__enter__.return_value = client
            self.assertEqual(inventory.main(), 0)
        session.assert_called_once()
        self.assertEqual([call.args[1:] for call in start.call_args_list], [(8, "page-8"), (9, "page-9")])
        self.assertEqual([call.args[1] for call in finish.call_args_list], [8, 9])
        self.assertEqual(client.get.call_count, 2)
        session.return_value.__exit__.assert_called_once()


@unittest.skipUnless(os.environ.get("FETCHM_TEST_TEMP_POSTGRES") == "1", "requires session-local PostgreSQL fixtures")
class InventoryPostgresTests(unittest.TestCase):
    def setUp(self):
        self.connection = store.psycopg.connect(store.dataset_database_url())
        self.addCleanup(self.connection.close)
        self.connection.execute("CREATE TEMP TABLE assembly_master (LIKE public.assembly_master INCLUDING ALL)")
        self.connection.execute("CREATE TEMP TABLE bacterial_inventory_membership (LIKE public.bacterial_inventory_membership INCLUDING ALL)")
        self.connection.commit()
        for name in ("assembly_master", "bacterial_inventory_membership"):
            self.assertEqual(self.connection.execute("SELECT relpersistence FROM pg_class WHERE oid=%s::regclass", (name,)).fetchone()[0], "t")

        @contextmanager
        def temporary_connection():
            yield self.connection

        self.patch = patch.object(store, "connect", temporary_connection)
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def test_real_bulk_insert_preserves_full_metadata_and_membership_on_replay(self):
        accession = "GCA_990000001.1"
        full = {"accession": accession, "organism": {"organism_name": "Example organism", "tax_id": 123}}
        self.assertEqual(store.insert_inventory_batch("fixture", [full]), (1, 0, 0))
        self.assertEqual(store.insert_inventory_batch("fixture", [{"accession": accession}] * 2), (0, 0, 2))
        row = self.connection.execute("SELECT organism_name,raw_payload,raw_fingerprint FROM assembly_master").fetchone()
        self.assertEqual(row, ("Example organism", full, store.normalized_inventory_record(full)["raw_fingerprint"]))
        self.assertEqual(self.connection.execute("SELECT raw_fingerprint FROM bacterial_inventory_membership").fetchone()[0], row[2])

    def test_real_duplicate_batch_preserves_sequential_merge_and_first_membership(self):
        first = {"accession": "GCA_990000001.1"}
        full = {"accession": first["accession"], "organism": {"organism_name": "Example organism", "tax_id": 123}}
        self.assertEqual(store.insert_inventory_batch("fixture", [first, full, first, {"accession": "GCF_990000001.1"}]), (1, 1, 2))
        self.assertEqual(self.connection.execute("SELECT raw_payload FROM assembly_master").fetchone()[0], full)
        self.assertEqual(self.connection.execute("SELECT raw_fingerprint FROM bacterial_inventory_membership").fetchone()[0], store.normalized_inventory_record(first)["raw_fingerprint"])


if __name__ == "__main__":
    unittest.main()
