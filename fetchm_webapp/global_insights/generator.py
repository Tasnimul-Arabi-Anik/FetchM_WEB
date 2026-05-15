from __future__ import annotations

import csv
import json
import math
import re
import shutil
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


MISSING_TOKENS = {
    "",
    "-",
    "--",
    "na",
    "n/a",
    "nan",
    "none",
    "null",
    "missing",
    "unknown",
    "not collected",
    "not provided",
    "not applicable",
    "not available",
    "unavailable",
    "undetermined",
    "unidentified",
    "unspecified",
    "not reported",
    "not determined",
}

COUNTRY_STD_FIELDS = ("Country", "country")
COUNTRY_RAW_FIELDS = ("Geographic Location", "geographic_location", "geo_loc_name", "Country_Raw")
HOST_STD_FIELDS = ("Host_SD", "host_standardized", "Host Standardized")
HOST_RAW_FIELDS = ("Host", "host")
SOURCE_STD_FIELDS = ("Isolation_Source_SD", "isolation_source_standardized", "Isolation Source Standardized")
SOURCE_RAW_FIELDS = ("Isolation Source", "isolation_source")
SAMPLE_STD_FIELDS = ("Sample_Type_SD", "sample_type_standardized")
SAMPLE_RAW_FIELDS = ("Sample Type", "sample_type", "sample type", "Sample_Type")
ENV_STD_FIELDS = ("Environment_Medium_SD", "Environment_Broad_Scale_SD", "Environment_Local_Scale_SD")
ENV_RAW_FIELDS = ("Environment", "environment", "env_broad_scale", "env_local_scale", "env_medium")
BIOPROJECT_FIELDS = ("Assembly BioProject Accession", "BioProject Accession", "bioproject_accession")
ASSEMBLY_FIELDS = ("Assembly Accession", "assembly_accession")
ORGANISM_FIELDS = ("Organism Name", "organism_name")
ASSEMBLY_LEVEL_FIELDS = ("Assembly Level", "assembly_level")
RELEASE_DATE_FIELDS = ("Assembly Release Date", "release_date")
COLLECTION_DATE_FIELDS = ("Collection Date", "collection_date")
COMPLETENESS_FIELDS = ("CheckM completeness", "Completeness", "CheckM2_Completeness", "completeness")
CONTAMINATION_FIELDS = ("CheckM contamination", "Contamination", "CheckM2_Contamination", "contamination")
N50_FIELDS = ("Assembly Stats Contig N50", "Assembly Stats Scaffold N50", "N50")
CONTIG_FIELDS = ("Assembly Stats Number of Contigs", "contigs", "Contigs")
GENOME_SIZE_FIELDS = ("Assembly Stats Total Sequence Length", "Assembly Stats Total Ungapped Length", "Genome size")
GC_FIELDS = ("Assembly Stats GC Percent", "GC", "GC%")
COUNTRY_CONFIDENCE_FIELDS = ("Country_Confidence", "Country_Evidence")
HOST_CONFIDENCE_FIELDS = ("Host_SD_Confidence", "Host_SD_Method")


@dataclass
class TaxonInput:
    id: int
    name: str
    rank: str
    genome_count: int | None
    metadata_clean_path: str
    last_synced_at: str | None = None


@dataclass
class TaxonStats:
    name: str
    rank: str
    rows: int = 0
    country_usable: int = 0
    host_or_source_usable: int = 0
    year_usable: int = 0
    quality_available: int = 0
    isolation_usable: int = 0
    confidence_available: int = 0
    bioprojects: Counter[str] = field(default_factory=Counter)
    countries: Counter[str] = field(default_factory=Counter)
    hosts: Counter[str] = field(default_factory=Counter)
    years: Counter[str] = field(default_factory=Counter)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_slug(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9._-]+", "-", value.strip()).strip("-").lower()
    return slug or "global-insights"


def normalize(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def normalized_key(value: Any) -> str:
    return normalize(value).casefold()


def is_usable(value: Any) -> bool:
    text = normalized_key(value)
    if text in MISSING_TOKENS:
        return False
    if not text:
        return False
    if text.startswith("unknown ") or text.endswith(" unknown"):
        return False
    return True


def row_value(row: dict[str, Any], fields: Iterable[str]) -> str:
    for field_name in fields:
        value = row.get(field_name)
        if value is not None and str(value).strip():
            return normalize(value)
    return ""


def parse_year(value: str) -> str:
    text = normalize(value)
    if not text:
        return ""
    match = re.search(r"(19|20)\d{2}", text)
    if not match:
        return ""
    year = int(match.group(0))
    if 1900 <= year <= datetime.now(timezone.utc).year + 1:
        return str(year)
    return ""


def parse_taxonomy(organism_name: str, fallback_taxon: str = "") -> tuple[str, str]:
    text = normalize(organism_name) or normalize(fallback_taxon)
    if not text:
        return "Unclassified", "Unclassified"
    parts = [part for part in re.split(r"\s+", text) if part]
    genus = parts[0] if parts else "Unclassified"
    if len(parts) >= 2:
        species = f"{parts[0]} {parts[1]}"
    else:
        species = genus
    return genus, species


def parse_float(value: str) -> float | None:
    text = normalize(value).replace(",", "")
    if not text:
        return None
    try:
        number = float(text)
    except ValueError:
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def percent(numerator: int | float, denominator: int | float) -> float:
    if denominator <= 0:
        return 0.0
    return round((float(numerator) / float(denominator)) * 100.0, 2)


def top_rows(counter: Counter[str], total: int, limit: int = 20) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for rank, (value, count) in enumerate(counter.most_common(limit), start=1):
        rows.append(
            {
                "rank": rank,
                "label": value,
                "count": int(count),
                "percent": percent(count, total),
            }
        )
    return rows


def severity_from_share(value: float) -> str:
    if value >= 75:
        return "severe"
    if value >= 50:
        return "high"
    if value >= 25:
        return "moderate"
    return "low"


def classify_host_source(host: str, source: str) -> str:
    text = f"{host} {source}".casefold()
    if not text.strip():
        return "Missing/ambiguous"
    if any(token in text for token in ("human", "homo sapiens", "patient", "clinical", "hospital", "stool", "feces", "faeces", "blood", "urine", "sputum")):
        return "Human-associated"
    if any(token in text for token in ("chicken", "cattle", "cow", "pig", "swine", "bird", "fish", "mouse", "rat", "dog", "cat", "animal", "poultry")):
        return "Animal-associated"
    if any(token in text for token in ("plant", "root", "leaf", "rhizosphere", "phyllosphere", "crop")):
        return "Plant-associated"
    if any(token in text for token in ("food", "meat", "milk", "cheese", "dairy", "vegetable", "produce", "seafood")):
        return "Food-associated"
    if "soil" in text:
        return "Soil"
    if any(token in text for token in ("water", "river", "lake", "marine", "seawater", "wastewater", "sewage", "aquatic")):
        return "Aquatic/water"
    if any(token in text for token in ("environment", "sediment", "biofilm", "surface", "air", "built environment")):
        return "Environmental"
    if any(token in text for token in ("lab", "laboratory", "culture medium")):
        return "Laboratory/culture"
    return "Other/ambiguous"


def resolve_metadata_path(path_text: str) -> Path:
    path = Path(path_text)
    if path.exists():
        return path
    marker = "/app/fetchm_webapp"
    if path_text.startswith(marker):
        local_root = Path(__file__).resolve().parents[1]
        candidate = local_root / path_text[len(marker) + 1 :]
        if candidate.exists():
            return candidate
    return path


def sorted_taxa(taxa: Iterable[dict[str, Any] | TaxonInput]) -> list[TaxonInput]:
    normalized: list[TaxonInput] = []
    for item in taxa:
        if isinstance(item, TaxonInput):
            normalized.append(item)
        else:
            normalized.append(
                TaxonInput(
                    id=int(item.get("id") or 0),
                    name=str(item.get("name") or item.get("species_name") or ""),
                    rank=str(item.get("rank") or item.get("taxon_rank") or "species"),
                    genome_count=int(item["genome_count"]) if item.get("genome_count") not in (None, "") else None,
                    metadata_clean_path=str(item.get("metadata_clean_path") or ""),
                    last_synced_at=str(item.get("last_synced_at") or "") or None,
                )
            )

    def sort_key(taxon: TaxonInput) -> tuple[int, str, str]:
        rank_order = 0 if taxon.rank == "species" else 1 if taxon.rank == "genus" else 2
        newest_first = "".join(chr(255 - ord(char)) for char in (taxon.last_synced_at or ""))
        return (rank_order, newest_first, taxon.name.casefold())

    return sorted(normalized, key=sort_key)


def quality_available(row: dict[str, Any]) -> bool:
    return any(is_usable(row_value(row, fields)) for fields in (COMPLETENESS_FIELDS, CONTAMINATION_FIELDS, N50_FIELDS, CONTIG_FIELDS, GENOME_SIZE_FIELDS, GC_FIELDS))


def confidence_available(row: dict[str, Any]) -> bool:
    return any(is_usable(row_value(row, fields)) for fields in (COUNTRY_CONFIDENCE_FIELDS, HOST_CONFIDENCE_FIELDS))


def update_field_pair_stats(stats: dict[str, int], raw_value: str, standardized_value: str) -> None:
    raw_ok = is_usable(raw_value)
    standardized_ok = is_usable(standardized_value)
    if raw_ok:
        stats["raw_usable"] += 1
    if standardized_ok:
        stats["standardized_usable"] += 1
    if raw_ok and standardized_ok:
        stats["both_usable"] += 1
        if normalized_key(raw_value) != normalized_key(standardized_value):
            stats["changed_mappings"] += 1
    elif standardized_ok:
        stats["standardized_only"] += 1
    elif raw_ok:
        stats["raw_only"] += 1


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def load_priority_pathogens(path: Path | None = None) -> list[dict[str, str]]:
    source = path or Path(__file__).with_name("priority_pathogens.csv")
    if not source.exists():
        return []
    with source.open(newline="", encoding="utf-8") as handle:
        return [
            {str(key): normalize(value) for key, value in row.items() if key is not None}
            for row in csv.DictReader(handle)
        ]


def calculate_taxon_quality(row: TaxonStats) -> dict[str, Any]:
    total = max(row.rows, 1)
    country = percent(row.country_usable, total)
    host_source = percent(row.host_or_source_usable, total)
    year = percent(row.year_usable, total)
    quality = percent(row.quality_available, total)
    isolation = percent(row.isolation_usable, total)
    confidence = percent(row.confidence_available, total)
    top_project_share = percent(row.bioprojects.most_common(1)[0][1], total) if row.bioprojects else 100.0
    diversity = max(0.0, 100.0 - top_project_share)
    score = round(
        0.20 * country
        + 0.20 * host_source
        + 0.15 * year
        + 0.15 * quality
        + 0.10 * diversity
        + 0.10 * isolation
        + 0.10 * confidence,
        2,
    )
    if score >= 85:
        grade = "Excellent"
    elif score >= 70:
        grade = "Good"
    elif score >= 50:
        grade = "Moderate"
    elif score >= 25:
        grade = "Poor"
    else:
        grade = "Very poor"
    return {
        "taxon": row.name,
        "rank": row.rank,
        "assemblies": row.rows,
        "metadata_quality_score": score,
        "metadata_quality_grade": grade,
        "country_completeness_percent": country,
        "host_or_source_completeness_percent": host_source,
        "collection_year_completeness_percent": year,
        "assembly_quality_availability_percent": quality,
        "bioproject_diversity_percent": round(diversity, 2),
        "isolation_source_completeness_percent": isolation,
        "standardization_confidence_percent": confidence,
    }


def build_narrative(summary: dict[str, Any]) -> dict[str, str]:
    overview = summary["overview"]
    top_genera = summary["taxonomic_landscape"]["top_genera"]
    top_genus_names = ", ".join(row["label"] for row in top_genera[:5]) or "the most represented genera"
    completeness = {row["field"]: row for row in summary["metadata_completeness"]}
    country = completeness.get("Country", {})
    host = completeness.get("Host", {})
    source = completeness.get("Isolation source", {})
    top10_share = summary["taxonomic_landscape"].get("top_10_genus_share_percent", 0)
    abstract = (
        f"The current FetchM Global Metadata Insights snapshot contained {overview['unique_assemblies']:,} unique bacterial "
        f"genome assemblies from {overview['metadata_files_scanned']:,} ready metadata files. The indexed assemblies represented "
        f"{overview['species_observed']:,} observed species labels, {overview['genera_observed']:,} genera, and "
        f"{overview['bioprojects_observed']:,} BioProjects. The ten most represented genera accounted for {top10_share}% "
        "of unique assemblies, indicating that public genome repositories are taxonomically concentrated."
    )
    results = (
        f"Genome availability was highly uneven across taxa. The most represented genera included {top_genus_names}. "
        f"Raw country fields were present for {country.get('raw_usable_percent', 0)}% of unique assemblies, while "
        f"FetchM standardized country assignments were available for {country.get('standardized_usable_percent', 0)}%. "
        f"Standardized host assignments were available for {host.get('standardized_usable_percent', 0)}% of assemblies, and "
        f"standardized isolation-source assignments were available for {source.get('standardized_usable_percent', 0)}%. "
        "Raw-field presence and standardized-field availability are different measures: a submitter value can be present but "
        "still too vague, inconsistent, or unmapped for reliable metadata filtering."
    )
    methods = (
        "Global Metadata Insights were generated from ready FetchM standardized metadata files. Unique assemblies were counted "
        "using Assembly Accession. When duplicate assemblies were present across species- and genus-level files, the species-level "
        "record was preferred, followed by the newest synced taxon. Metadata completeness was calculated separately for raw and "
        "standardized fields, with missing, unknown, not collected, not applicable, unidentified, and similarly non-informative "
        "values treated as unusable."
    )
    limitations = (
        "These results describe representation within public bacterial genome repositories and should not be interpreted as direct "
        "estimates of true global bacterial abundance, disease burden, or environmental prevalence. Public genome metadata are "
        "influenced by database submission practices, surveillance priorities, sequencing capacity, outbreak sampling, and uneven "
        "reporting standards."
    )
    figure_legend = (
        "Figure X. Global distribution and metadata completeness of FetchM-indexed bacterial genome assemblies. Annual genome "
        "availability, dominant bacterial taxa, geographic representation, host/source distributions, raw-versus-standardized "
        "metadata completeness, and bias warnings were calculated from the latest FetchM Global Metadata Insights snapshot."
    )
    table_caption = (
        "Table X. Metadata completeness, standardization impact, metadata quality scores, and repository representation bias "
        "among FetchM-indexed bacterial taxa."
    )
    return {
        "abstract": abstract,
        "results": results,
        "methods": methods,
        "limitations": limitations,
        "figure_legend": figure_legend,
        "table_caption": table_caption,
    }


def generate_global_insights_snapshot(
    taxa: Iterable[dict[str, Any] | TaxonInput],
    output_root: Path,
    *,
    app_version: str,
    app_commit: str,
    snapshot_id: str | None = None,
    demo: bool = False,
) -> dict[str, Any]:
    if demo:
        return generate_demo_snapshot(output_root, app_version=app_version, app_commit=app_commit, snapshot_id=snapshot_id)

    snapshot_id = snapshot_id or f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_global_insights"
    output_root.mkdir(parents=True, exist_ok=True)
    snapshot_dir = output_root / "snapshots" / safe_slug(snapshot_id)
    tmp_dir = output_root / "snapshots" / f".{safe_slug(snapshot_id)}.tmp"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    table_dir = tmp_dir / "tables"
    table_dir.mkdir(parents=True, exist_ok=True)

    status_path = tmp_dir / "status.json"
    status_path.write_text(json.dumps({"snapshot_id": snapshot_id, "status": "running", "started_at": utc_now()}, indent=2), encoding="utf-8")

    seen_accessions: set[str] = set()
    unique_total = 0
    duplicate_rows = 0
    metadata_rows_scanned = 0
    files_scanned = 0
    files_skipped = 0
    genus_counter: Counter[str] = Counter()
    species_counter: Counter[str] = Counter()
    country_counter: Counter[str] = Counter()
    continent_counter: Counter[str] = Counter()
    subcontinent_counter: Counter[str] = Counter()
    host_counter: Counter[str] = Counter()
    source_counter: Counter[str] = Counter()
    host_category_counter: Counter[str] = Counter()
    assembly_level_counter: Counter[str] = Counter()
    year_counter: Counter[str] = Counter()
    cumulative_year_counter: Counter[str] = Counter()
    bioproject_counter: Counter[str] = Counter()
    confidence_counter: Counter[str] = Counter()
    correction_counter: Counter[tuple[str, str, str]] = Counter()
    taxon_stats: dict[str, TaxonStats] = {}
    completeness_fields = {
        "Country": {"raw_usable": 0, "standardized_usable": 0, "both_usable": 0, "standardized_only": 0, "raw_only": 0, "changed_mappings": 0},
        "Host": {"raw_usable": 0, "standardized_usable": 0, "both_usable": 0, "standardized_only": 0, "raw_only": 0, "changed_mappings": 0},
        "Collection year": {"raw_usable": 0, "standardized_usable": 0, "both_usable": 0, "standardized_only": 0, "raw_only": 0, "changed_mappings": 0},
        "Isolation source": {"raw_usable": 0, "standardized_usable": 0, "both_usable": 0, "standardized_only": 0, "raw_only": 0, "changed_mappings": 0},
        "Sample type": {"raw_usable": 0, "standardized_usable": 0, "both_usable": 0, "standardized_only": 0, "raw_only": 0, "changed_mappings": 0},
        "Environment": {"raw_usable": 0, "standardized_usable": 0, "both_usable": 0, "standardized_only": 0, "raw_only": 0, "changed_mappings": 0},
    }

    simulator_path = table_dir / "simulator_records.csv"
    simulator_fields = [
        "assembly_accession",
        "genus",
        "species",
        "taxon",
        "taxon_rank",
        "raw_country",
        "standardized_country",
        "raw_host",
        "standardized_host",
        "raw_source",
        "standardized_source",
        "collection_year",
        "assembly_level",
    ]
    with simulator_path.open("w", newline="", encoding="utf-8") as simulator_handle:
        simulator_writer = csv.DictWriter(simulator_handle, fieldnames=simulator_fields)
        simulator_writer.writeheader()
        for taxon in sorted_taxa(taxa):
            path = resolve_metadata_path(taxon.metadata_clean_path)
            if not path.exists():
                files_skipped += 1
                continue
            files_scanned += 1
            taxon_key = f"{taxon.rank}:{taxon.name}"
            stat = taxon_stats.setdefault(taxon_key, TaxonStats(name=taxon.name, rank=taxon.rank))
            with path.open(newline="", encoding="utf-8", errors="replace") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    metadata_rows_scanned += 1
                    accession = row_value(row, ASSEMBLY_FIELDS)
                    if not accession:
                        accession = f"{taxon_key}:{metadata_rows_scanned}"
                    if accession in seen_accessions:
                        duplicate_rows += 1
                        continue
                    seen_accessions.add(accession)
                    unique_total += 1
                    stat.rows += 1

                    organism = row_value(row, ORGANISM_FIELDS)
                    genus, species = parse_taxonomy(organism, taxon.name)
                    genus_counter[genus] += 1
                    species_counter[species] += 1

                    raw_country = row_value(row, COUNTRY_RAW_FIELDS)
                    std_country = row_value(row, COUNTRY_STD_FIELDS)
                    raw_host = row_value(row, HOST_RAW_FIELDS)
                    std_host = row_value(row, HOST_STD_FIELDS)
                    raw_source = row_value(row, SOURCE_RAW_FIELDS)
                    std_source = row_value(row, SOURCE_STD_FIELDS)
                    raw_sample = row_value(row, SAMPLE_RAW_FIELDS)
                    std_sample = row_value(row, SAMPLE_STD_FIELDS)
                    raw_env = row_value(row, ENV_RAW_FIELDS)
                    std_env = row_value(row, ENV_STD_FIELDS)
                    release_year = parse_year(row_value(row, RELEASE_DATE_FIELDS))
                    collection_year = parse_year(row_value(row, COLLECTION_DATE_FIELDS))
                    assembly_level = row_value(row, ASSEMBLY_LEVEL_FIELDS)
                    bioproject = row_value(row, BIOPROJECT_FIELDS)
                    continent = row_value(row, ("Continent",))
                    subcontinent = row_value(row, ("Subcontinent",))

                    update_field_pair_stats(completeness_fields["Country"], raw_country, std_country)
                    if is_usable(std_country):
                        country_counter[std_country] += 1
                        stat.country_usable += 1
                        stat.countries[std_country] += 1
                    if is_usable(continent):
                        continent_counter[continent] += 1
                    if is_usable(subcontinent):
                        subcontinent_counter[subcontinent] += 1
                    update_field_pair_stats(completeness_fields["Host"], raw_host, std_host)
                    if is_usable(std_host):
                        host_counter[std_host] += 1
                        stat.hosts[std_host] += 1
                    update_field_pair_stats(completeness_fields["Isolation source"], raw_source, std_source)
                    if is_usable(std_source):
                        source_counter[std_source] += 1
                        stat.isolation_usable += 1
                    update_field_pair_stats(completeness_fields["Sample type"], raw_sample, std_sample)
                    update_field_pair_stats(completeness_fields["Environment"], raw_env, std_env)
                    if collection_year:
                        update_field_pair_stats(completeness_fields["Collection year"], collection_year, collection_year)
                        stat.year_usable += 1
                        stat.years[collection_year] += 1
                    if release_year:
                        year_counter[release_year] += 1
                    if collection_year:
                        cumulative_year_counter[collection_year] += 1
                    if is_usable(assembly_level):
                        assembly_level_counter[assembly_level] += 1
                    if is_usable(bioproject):
                        bioproject_counter[bioproject] += 1
                        stat.bioprojects[bioproject] += 1
                    if quality_available(row):
                        stat.quality_available += 1
                    if is_usable(std_host) or is_usable(std_source):
                        stat.host_or_source_usable += 1
                    if confidence_available(row):
                        stat.confidence_available += 1
                    for field_name in (*COUNTRY_CONFIDENCE_FIELDS, *HOST_CONFIDENCE_FIELDS):
                        if is_usable(row.get(field_name)):
                            confidence_counter[f"{field_name}: {normalize(row[field_name])}"] += 1

                    host_category_counter[classify_host_source(std_host or raw_host, std_source or raw_source)] += 1
                    for label, raw_value, std_value in (
                        ("Country", raw_country, std_country),
                        ("Host", raw_host, std_host),
                        ("Isolation source", raw_source, std_source),
                        ("Sample type", raw_sample, std_sample),
                        ("Environment", raw_env, std_env),
                    ):
                        if is_usable(raw_value) and is_usable(std_value) and normalized_key(raw_value) != normalized_key(std_value):
                            correction_counter[(label, raw_value[:120], std_value[:120])] += 1

                    simulator_writer.writerow(
                        {
                            "assembly_accession": accession,
                            "genus": genus,
                            "species": species,
                            "taxon": taxon.name,
                            "taxon_rank": taxon.rank,
                            "raw_country": raw_country,
                            "standardized_country": std_country,
                            "raw_host": raw_host,
                            "standardized_host": std_host,
                            "raw_source": raw_source,
                            "standardized_source": std_source,
                            "collection_year": collection_year,
                            "assembly_level": assembly_level,
                        }
                    )

    metadata_completeness = []
    for field_name, counts in completeness_fields.items():
        raw_count = int(counts["raw_usable"])
        std_count = int(counts["standardized_usable"])
        delta_points = round(percent(std_count, unique_total) - percent(raw_count, unique_total), 2)
        metadata_completeness.append(
            {
                "field": field_name,
                "raw_usable": raw_count,
                "raw_usable_percent": percent(raw_count, unique_total),
                "raw_present": raw_count,
                "raw_present_percent": percent(raw_count, unique_total),
                "standardized_usable": std_count,
                "standardized_usable_percent": percent(std_count, unique_total),
                "standardized_records": std_count,
                "standardized_percent": percent(std_count, unique_total),
                "both_usable_records": int(counts["both_usable"]),
                "standardized_only_records": int(counts["standardized_only"]),
                "raw_only_records": int(counts["raw_only"]),
                "raw_not_standardized_records": int(counts["raw_only"]),
                "changed_mappings": int(counts["changed_mappings"]),
                "raw_values_remapped": int(counts["changed_mappings"]),
                "rescued_records": int(counts["standardized_only"]),
                "gain_percentage_points": delta_points,
            }
        )

    taxon_quality = [calculate_taxon_quality(stat) for stat in taxon_stats.values()]
    taxon_quality.sort(key=lambda row: (-int(row["assemblies"]), -float(row["metadata_quality_score"]), row["taxon"]))
    qc_ready_taxa = [
        row
        for row in taxon_quality
        if int(row["assemblies"]) >= 100
        and float(row["country_completeness_percent"]) >= 70
        and float(row["host_or_source_completeness_percent"]) >= 70
        and float(row["collection_year_completeness_percent"]) >= 50
    ]

    bias_warnings: list[dict[str, Any]] = []
    bioproject_dominance: list[dict[str, Any]] = []
    for stat in taxon_stats.values():
        if stat.rows <= 0:
            continue
        top_projects = stat.bioprojects.most_common(10)
        top1 = top_projects[0][1] if top_projects else 0
        top5 = sum(count for _, count in top_projects[:5])
        top10 = sum(count for _, count in top_projects[:10])
        dominance = {
            "taxon": stat.name,
            "rank": stat.rank,
            "assemblies": stat.rows,
            "top_1_bioproject_share_percent": percent(top1, stat.rows),
            "top_5_bioproject_share_percent": percent(top5, stat.rows),
            "top_10_bioproject_share_percent": percent(top10, stat.rows),
            "top_bioproject": top_projects[0][0] if top_projects else "",
        }
        bioproject_dominance.append(dominance)
        if stat.rows >= 100 and dominance["top_5_bioproject_share_percent"] >= 30:
            bias_warnings.append(
                {
                    "scope": stat.name,
                    "scope_type": stat.rank,
                    "assemblies": stat.rows,
                    "bias_type": "BioProject dominance",
                    "severity": severity_from_share(dominance["top_5_bioproject_share_percent"]),
                    "metric_percent": dominance["top_5_bioproject_share_percent"],
                    "warning": (
                        f"{stat.name} has {dominance['top_5_bioproject_share_percent']}% of assemblies in the top five BioProjects. "
                        "Downstream analyses should consider BioProject-aware sampling."
                    ),
                }
            )
        for label, counter in (("country", stat.countries), ("host", stat.hosts), ("collection year", stat.years)):
            if not counter:
                continue
            top_label, top_count = counter.most_common(1)[0]
            share = percent(top_count, stat.rows)
            if share >= 50 and stat.rows >= 100:
                bias_warnings.append(
                    {
                        "scope": stat.name,
                        "scope_type": stat.rank,
                        "assemblies": stat.rows,
                        "bias_type": f"{label} dominance",
                        "severity": severity_from_share(share),
                        "metric_percent": share,
                        "warning": f"{stat.name} is dominated by {label} '{top_label}' ({share}% of assemblies).",
                    }
                )

    bioproject_dominance.sort(key=lambda row: (-float(row["top_5_bioproject_share_percent"]), -int(row["assemblies"])))
    bias_warnings.sort(key=lambda row: ({"severe": 0, "high": 1, "moderate": 2, "low": 3}.get(str(row["severity"]), 9), -int(row.get("assemblies") or 0), -float(row["metric_percent"])))

    correction_rows = [
        {"field": field, "raw_value": raw, "standardized_value": std, "records_rescued": count}
        for (field, raw, std), count in correction_counter.most_common(100)
    ]

    yearly_growth_rows = []
    cumulative = 0
    for year in sorted(year_counter):
        count = int(year_counter[year])
        cumulative += count
        yearly_growth_rows.append({"year": year, "assemblies": count, "cumulative_assemblies": cumulative})

    priority_pathogens = load_priority_pathogens()
    taxon_quality_by_name = {normalized_key(row["taxon"]): row for row in taxon_quality}
    pathogen_rows = []
    for pathogen in priority_pathogens:
        name = pathogen.get("taxon_name", "")
        quality = taxon_quality_by_name.get(normalized_key(name))
        if quality:
            pathogen_rows.append({**pathogen, **quality})
        else:
            pathogen_rows.append({**pathogen, "assemblies": 0, "metadata_quality_score": "", "metadata_quality_grade": "Not available"})

    summary = {
        "snapshot_id": snapshot_id,
        "generated_at": utc_now(),
        "is_demo": False,
        "overview": {
            "unique_assemblies": unique_total,
            "metadata_rows_scanned": metadata_rows_scanned,
            "duplicate_rows_skipped": duplicate_rows,
            "metadata_files_scanned": files_scanned,
            "metadata_files_skipped": files_skipped,
            "genera_observed": len(genus_counter),
            "species_observed": len(species_counter),
            "countries_observed": len(country_counter),
            "hosts_observed": len(host_counter),
            "bioprojects_observed": len(bioproject_counter),
            "qc_ready_taxa": len(qc_ready_taxa),
        },
        "methods": {
            "app_version": app_version,
            "app_commit": app_commit,
            "generation_scope": "Ready FetchM taxa with metadata_status='ready' and a metadata_clean_path were scanned after metadata update and standardization refresh completion.",
            "duplicate_rule": "Unique assemblies are counted by Assembly Accession. Species-level metadata rows are preferred over genus-level rows; newer synced taxa are scanned first within each rank.",
            "field_mappings": "Country: Geographic Location/Country_Raw -> Country; Host: Host -> Host_SD; Isolation source: Isolation Source -> Isolation_Source_SD; sample type and environment raw comparisons use raw sample/environment fields when present, while standardized coverage is counted from Sample_Type_SD and Environment_*_SD; collection year: Collection Date; growth year: Assembly Release Date.",
            "missing_value_rule": "Empty, unknown, not collected, not applicable, unidentified, and similarly non-informative values are treated as unusable metadata.",
            "qc_ready_rule": "At least 100 assemblies, >=70% standardized country completeness, >=70% host/source completeness, and >=50% collection-year completeness.",
            "bias_score_formulas": "Dominance scores are calculated as the top category share among assemblies in scope: top 1/5/10 BioProject share, top country share, top host share, and top collection-year share. Warning severity uses low <25%, moderate 25-49.99%, high 50-74.99%, and severe >=75%.",
            "metadata_quality_score": "0.20 country + 0.20 host/source + 0.15 collection year + 0.15 assembly quality + 0.10 BioProject diversity + 0.10 isolation source + 0.10 standardization confidence.",
            "caution": "Global Insights describe representation within public genome repositories, not true global bacterial abundance, disease burden, or environmental prevalence.",
        },
        "taxonomic_landscape": {
            "top_genera": top_rows(genus_counter, unique_total, 25),
            "top_species": top_rows(species_counter, unique_total, 25),
            "top_10_genus_share_percent": percent(sum(count for _, count in genus_counter.most_common(10)), unique_total),
        },
        "geographic_bias": {
            "countries": top_rows(country_counter, unique_total, 50),
            "continents": top_rows(continent_counter, unique_total, 20),
            "subcontinents": top_rows(subcontinent_counter, unique_total, 30),
        },
        "host_source_bias": {
            "hosts": top_rows(host_counter, unique_total, 50),
            "sources": top_rows(source_counter, unique_total, 50),
            "host_categories": top_rows(host_category_counter, unique_total, 20),
        },
        "assembly_quality": {
            "assembly_levels": top_rows(assembly_level_counter, unique_total, 20),
        },
        "metadata_completeness": metadata_completeness,
        "standardization_impact": {
            "top_corrections": correction_rows[:30],
            "confidence_methods": top_rows(confidence_counter, sum(confidence_counter.values()), 30),
            "mapped_records": int(sum(row["records_rescued"] for row in correction_rows)),
        },
        "yearly_growth": yearly_growth_rows,
        "metadata_quality": taxon_quality[:100],
        "qc_ready_taxa": qc_ready_taxa[:100],
        "bioproject_dominance": bioproject_dominance[:100],
        "bias_warnings": bias_warnings[:100],
        "pathogen_insights": pathogen_rows,
        "downloads": {
            "summary_json": "summary.json",
            "simulator_records": "tables/simulator_records.csv",
            "top_genera": "tables/top_genera.csv",
            "top_species": "tables/top_species.csv",
            "countries": "tables/countries.csv",
            "metadata_completeness": "tables/metadata_completeness.csv",
            "metadata_quality": "tables/metadata_quality.csv",
            "bias_warnings": "tables/bias_warnings.csv",
            "top_corrections": "tables/top_corrections.csv",
            "yearly_growth": "tables/yearly_growth.csv",
            "qc_ready_taxa": "tables/qc_ready_taxa.csv",
            "bioproject_dominance": "tables/bioproject_dominance.csv",
            "pathogen_insights": "tables/pathogen_insights.csv",
        },
    }
    summary["manuscript"] = build_narrative(summary)

    write_csv(table_dir / "top_genera.csv", summary["taxonomic_landscape"]["top_genera"], ["rank", "label", "count", "percent"])
    write_csv(table_dir / "top_species.csv", summary["taxonomic_landscape"]["top_species"], ["rank", "label", "count", "percent"])
    write_csv(table_dir / "countries.csv", summary["geographic_bias"]["countries"], ["rank", "label", "count", "percent"])
    write_csv(
        table_dir / "metadata_completeness.csv",
        metadata_completeness,
        [
            "field",
            "raw_present",
            "raw_present_percent",
            "standardized_records",
            "standardized_percent",
            "both_usable_records",
            "raw_not_standardized_records",
            "raw_values_remapped",
        ],
    )
    write_csv(table_dir / "metadata_quality.csv", taxon_quality, list(taxon_quality[0].keys()) if taxon_quality else ["taxon", "rank", "assemblies"])
    write_csv(table_dir / "bias_warnings.csv", bias_warnings, ["scope", "scope_type", "assemblies", "bias_type", "severity", "metric_percent", "warning"])
    write_csv(table_dir / "top_corrections.csv", correction_rows, ["field", "raw_value", "standardized_value", "records_rescued"])
    write_csv(table_dir / "yearly_growth.csv", yearly_growth_rows, ["year", "assemblies", "cumulative_assemblies"])
    write_csv(table_dir / "qc_ready_taxa.csv", qc_ready_taxa, list(qc_ready_taxa[0].keys()) if qc_ready_taxa else ["taxon", "rank", "assemblies"])
    write_csv(table_dir / "bioproject_dominance.csv", bioproject_dominance, ["taxon", "rank", "assemblies", "top_1_bioproject_share_percent", "top_5_bioproject_share_percent", "top_10_bioproject_share_percent", "top_bioproject"])
    write_csv(table_dir / "pathogen_insights.csv", pathogen_rows, sorted({key for row in pathogen_rows for key in row.keys()}) if pathogen_rows else ["taxon_name", "rank", "group", "notes"])

    (tmp_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    status_path.write_text(json.dumps({"snapshot_id": snapshot_id, "status": "completed", "completed_at": utc_now()}, indent=2), encoding="utf-8")
    if snapshot_dir.exists():
        shutil.rmtree(snapshot_dir)
    tmp_dir.rename(snapshot_dir)
    latest = {"snapshot_id": snapshot_id, "summary_path": str(snapshot_dir / "summary.json"), "generated_at": summary["generated_at"], "is_demo": False}
    (output_root / "latest.json").write_text(json.dumps(latest, indent=2), encoding="utf-8")
    return summary


def generate_demo_snapshot(output_root: Path, *, app_version: str, app_commit: str, snapshot_id: str | None = None) -> dict[str, Any]:
    snapshot_id = snapshot_id or "demo_global_insights"
    output_root.mkdir(parents=True, exist_ok=True)
    snapshot_dir = output_root / "snapshots" / safe_slug(snapshot_id)
    if snapshot_dir.exists():
        shutil.rmtree(snapshot_dir)
    table_dir = snapshot_dir / "tables"
    table_dir.mkdir(parents=True, exist_ok=True)
    summary = {
        "snapshot_id": snapshot_id,
        "generated_at": utc_now(),
        "is_demo": True,
        "overview": {
            "unique_assemblies": 2846213,
            "metadata_rows_scanned": 3120448,
            "duplicate_rows_skipped": 274235,
            "metadata_files_scanned": 5066,
            "metadata_files_skipped": 0,
            "genera_observed": 5814,
            "species_observed": 48392,
            "countries_observed": 184,
            "hosts_observed": 12140,
            "bioprojects_observed": 152706,
            "qc_ready_taxa": 1286,
        },
        "methods": {
            "app_version": app_version,
            "app_commit": app_commit,
            "generation_scope": "DEMO: ready standardized metadata files after refresh completion.",
            "duplicate_rule": "DEMO: unique assemblies are counted by Assembly Accession.",
            "field_mappings": "DEMO: raw country/host/source fields are compared with FetchM standardized country/host/source fields.",
            "missing_value_rule": "DEMO: non-informative metadata values are treated as unusable.",
            "qc_ready_rule": "DEMO: >=100 assemblies and adequate standardized metadata completeness.",
            "bias_score_formulas": "DEMO: dominance shares use top BioProject, country, host, and year representation.",
            "metadata_quality_score": "DEMO: weighted country, host/source, year, quality, BioProject diversity, source, and confidence components.",
            "caution": "DEMO DATA - NOT REAL RESULTS. Repository representation is not global biological prevalence.",
        },
        "taxonomic_landscape": {
            "top_genera": [
                {"rank": 1, "label": "Escherichia", "count": 318420, "percent": 11.19},
                {"rank": 2, "label": "Salmonella", "count": 284611, "percent": 10.0},
                {"rank": 3, "label": "Staphylococcus", "count": 201984, "percent": 7.1},
                {"rank": 4, "label": "Klebsiella", "count": 176392, "percent": 6.2},
                {"rank": 5, "label": "Mycobacterium", "count": 144210, "percent": 5.07},
            ],
            "top_species": [
                {"rank": 1, "label": "Escherichia coli", "count": 287441, "percent": 10.1},
                {"rank": 2, "label": "Salmonella enterica", "count": 241882, "percent": 8.5},
                {"rank": 3, "label": "Staphylococcus aureus", "count": 184901, "percent": 6.5},
            ],
            "top_10_genus_share_percent": 49.6,
        },
        "geographic_bias": {
            "countries": [{"rank": 1, "label": "United States", "count": 702112, "percent": 24.67}],
            "continents": [
                {"rank": 1, "label": "North America", "count": 948210, "percent": 33.31},
                {"rank": 2, "label": "Europe", "count": 821403, "percent": 28.86},
                {"rank": 3, "label": "Asia", "count": 612992, "percent": 21.54},
            ],
            "subcontinents": [],
        },
        "host_source_bias": {
            "hosts": [{"rank": 1, "label": "Human", "count": 1042770, "percent": 36.64}],
            "sources": [{"rank": 1, "label": "feces/stool", "count": 381119, "percent": 13.39}],
            "host_categories": [
                {"rank": 1, "label": "Human-associated", "count": 1042770, "percent": 36.64},
                {"rank": 2, "label": "Animal-associated", "count": 428119, "percent": 15.04},
            ],
        },
        "assembly_quality": {"assembly_levels": [{"rank": 1, "label": "Contig", "count": 1800000, "percent": 63.24}]},
        "metadata_completeness": [
            {"field": "Country", "raw_usable": 1702114, "raw_usable_percent": 59.8, "standardized_usable": 2436890, "standardized_usable_percent": 85.6, "rescued_records": 734776, "gain_percentage_points": 25.8},
            {"field": "Host", "raw_usable": 1371947, "raw_usable_percent": 48.2, "standardized_usable": 2189338, "standardized_usable_percent": 76.9, "rescued_records": 817391, "gain_percentage_points": 28.7},
            {"field": "Isolation source", "raw_usable": 1238090, "raw_usable_percent": 43.5, "standardized_usable": 2026530, "standardized_usable_percent": 71.2, "rescued_records": 788440, "gain_percentage_points": 27.7},
        ],
        "standardization_impact": {
            "top_corrections": [
                {"field": "Country", "raw_value": "USA", "standardized_value": "United States", "records_rescued": 82441},
                {"field": "Host", "raw_value": "Homo sapiens", "standardized_value": "Human", "records_rescued": 64220},
            ],
            "confidence_methods": [],
        },
        "yearly_growth": [
            {"year": "2022", "assemblies": 310000, "cumulative_assemblies": 2010000},
            {"year": "2023", "assemblies": 386000, "cumulative_assemblies": 2396000},
            {"year": "2024", "assemblies": 420000, "cumulative_assemblies": 2816000},
        ],
        "metadata_quality": [
            {"taxon": "Salmonella enterica", "rank": "species", "assemblies": 241882, "metadata_quality_score": 91.2, "metadata_quality_grade": "Excellent"},
            {"taxon": "Escherichia coli", "rank": "species", "assemblies": 287441, "metadata_quality_score": 83.4, "metadata_quality_grade": "Good"},
        ],
        "qc_ready_taxa": [],
        "bioproject_dominance": [],
        "bias_warnings": [
            {"scope": "Salmonella enterica", "scope_type": "species", "bias_type": "BioProject dominance", "severity": "moderate", "metric_percent": 31.4, "warning": "DEMO: top five BioProjects account for 31.4% of assemblies."}
        ],
        "pathogen_insights": [],
        "downloads": {},
    }
    summary["manuscript"] = build_narrative(summary)
    (snapshot_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    (snapshot_dir / "status.json").write_text(json.dumps({"snapshot_id": snapshot_id, "status": "completed", "completed_at": utc_now()}, indent=2), encoding="utf-8")
    (output_root / "latest.json").write_text(json.dumps({"snapshot_id": snapshot_id, "summary_path": str(snapshot_dir / "summary.json"), "generated_at": summary["generated_at"], "is_demo": True}, indent=2), encoding="utf-8")
    return summary


def matches_filter(value: str, expected: str) -> bool:
    if not expected:
        return True
    return expected.casefold() in normalize(value).casefold()


def run_standardization_simulator(records_path: Path, filters: dict[str, Any], *, limit_examples: int = 25) -> dict[str, Any]:
    if not records_path.exists():
        return {"available": False, "error": "Simulator records are not available for this snapshot."}
    raw_count = 0
    standardized_count = 0
    rescued_count = 0
    examples: list[dict[str, str]] = []
    country = normalize(filters.get("country"))
    host = normalize(filters.get("host"))
    taxon = normalize(filters.get("taxon"))
    assembly_level = normalize(filters.get("assembly_level"))
    year_from = parse_year(str(filters.get("year_from") or ""))
    year_to = parse_year(str(filters.get("year_to") or ""))
    with records_path.open(newline="", encoding="utf-8", errors="replace") as handle:
        for row in csv.DictReader(handle):
            year = row.get("collection_year", "")
            if year_from and (not year or year < year_from):
                continue
            if year_to and (not year or year > year_to):
                continue
            if taxon and not (matches_filter(row.get("taxon", ""), taxon) or matches_filter(row.get("species", ""), taxon) or matches_filter(row.get("genus", ""), taxon)):
                continue
            if assembly_level and not matches_filter(row.get("assembly_level", ""), assembly_level):
                continue
            raw_match = matches_filter(row.get("raw_country", ""), country) and matches_filter(row.get("raw_host", ""), host)
            std_match = matches_filter(row.get("standardized_country", ""), country) and (
                matches_filter(row.get("standardized_host", ""), host) or matches_filter(row.get("standardized_source", ""), host)
            )
            if raw_match:
                raw_count += 1
            if std_match:
                standardized_count += 1
            if std_match and not raw_match:
                rescued_count += 1
                if len(examples) < limit_examples:
                    examples.append(
                        {
                            "assembly_accession": row.get("assembly_accession", ""),
                            "taxon": row.get("taxon", ""),
                            "raw_country": row.get("raw_country", ""),
                            "standardized_country": row.get("standardized_country", ""),
                            "raw_host": row.get("raw_host", ""),
                            "standardized_host": row.get("standardized_host", ""),
                        }
                    )
    return {
        "available": True,
        "raw_count": raw_count,
        "standardized_count": standardized_count,
        "rescued_count": rescued_count,
        "gain_percent": percent(rescued_count, raw_count) if raw_count else 0.0,
        "examples": examples,
    }
