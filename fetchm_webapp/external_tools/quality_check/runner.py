from __future__ import annotations

import json
import os
import shlex
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .schemas import QUALITY_MODULES


EXTERNAL_TOOL_DIR = Path(__file__).resolve().parent
DEFAULT_NEXTFLOW_REPO = "Tasnimul-Arabi-Anik/PanResistome"


def quality_tool_status() -> dict[str, Any]:
    tools = {
        "nextflow": shutil.which("nextflow"),
        "checkm2": shutil.which("checkm2"),
        "quast.py": shutil.which("quast.py"),
        "skani": shutil.which("skani"),
        "mash": shutil.which("mash"),
        "gtdbtk": shutil.which("gtdbtk"),
    }
    nextflow_enabled = os.environ.get("FETCHM_WEBAPP_QUALITY_NEXTFLOW_ENABLED", "").strip().lower() in {"1", "true", "yes", "on"}
    workflow = os.environ.get("FETCHM_WEBAPP_QUALITY_NEXTFLOW_WORKFLOW", DEFAULT_NEXTFLOW_REPO).strip() or DEFAULT_NEXTFLOW_REPO
    nextflow_profile = os.environ.get("FETCHM_WEBAPP_QUALITY_NEXTFLOW_PROFILE", "conda,lowmem").strip() or "conda,lowmem"
    checkm2_db = os.environ.get("FETCHM_WEBAPP_QUALITY_CHECKM2_DB", "").strip()
    checkm2_db_dir = os.environ.get("FETCHM_WEBAPP_QUALITY_CHECKM2_DB_DIR", "").strip()
    gtdbtk_data_path = os.environ.get("FETCHM_WEBAPP_QUALITY_GTDBTK_DATA_PATH", "").strip()
    nextflow_syntax_parser = os.environ.get("NXF_SYNTAX_PARSER", "v1").strip() or "v1"
    return {
        "nextflow_enabled": nextflow_enabled,
        "nextflow_available": bool(tools["nextflow"]),
        "conda_available": bool(shutil.which("conda")),
        "nextflow_workflow": workflow,
        "nextflow_workflow_exists": Path(workflow).exists() if workflow.startswith("/") else None,
        "nextflow_profile": nextflow_profile,
        "nextflow_syntax_parser": nextflow_syntax_parser,
        "checkm2_db": checkm2_db,
        "checkm2_db_dir": checkm2_db_dir,
        "gtdbtk_data_path": gtdbtk_data_path,
        "tools": tools,
        "available_tools": {key: bool(value) for key, value in tools.items()},
        "external_tool_dir": str(EXTERNAL_TOOL_DIR),
    }


def build_quality_display_command(input_path: Path, output_dir: Path, config: dict[str, Any]) -> list[str]:
    thresholds = config.get("thresholds") or {}
    command = [
        "fetchm-web-quality-check",
        "--input",
        str(input_path),
        "--outdir",
        str(output_dir),
        "--run-mode",
        str(config.get("run_mode") or "quick"),
    ]
    for module in config.get("selected_modules") or []:
        command.extend(["--module", str(module)])
    for key, value in thresholds.items():
        if value is None:
            continue
        command.extend([f"--{key.replace('_', '-')}", str(value)])
    return command


def prepare_panresistome_local_samples(input_path: Path, output_dir: Path) -> Path:
    sample_dir = output_dir / "external_tools" / "quality_check" / "local_samples" / "fetchm_web_qc"
    sequence_dir = sample_dir / "sequence"
    metadata_dir = sample_dir / "metadata_output"
    sequence_dir.mkdir(parents=True, exist_ok=True)
    metadata_dir.mkdir(parents=True, exist_ok=True)

    if input_path.exists():
        shutil.copy2(input_path, metadata_dir / "ncbi_clean.csv")
        shutil.copy2(input_path, metadata_dir / "fetchm2_clean.csv")

    for fasta_path in sorted(output_dir.glob("*.fna")):
        target = sequence_dir / fasta_path.name
        if target.exists():
            continue
        try:
            os.link(fasta_path, target)
        except OSError:
            shutil.copy2(fasta_path, target)

    (sample_dir / "metadata_engine.txt").write_text("fetchm_web\n", encoding="utf-8")
    return sample_dir.parent


def build_nextflow_command(input_path: Path, output_dir: Path, config: dict[str, Any]) -> list[str]:
    status = quality_tool_status()
    workflow = status["nextflow_workflow"]
    thresholds = config.get("thresholds") or {}
    selected_modules = set(str(module) for module in (config.get("selected_modules") or []))
    local_samples = prepare_panresistome_local_samples(input_path, output_dir)
    work_dir = output_dir / "external_tools" / "quality_check" / "nextflow_work"
    command = [
        "nextflow",
        "run",
        workflow,
        "-profile",
        status["nextflow_profile"],
        "-work-dir",
        str(work_dir),
        "--input",
        str(input_path),
        "--local_samples",
        str(local_samples),
        "--outdir",
        str(output_dir / "nextflow_qc"),
        "--stop_after_qc",
        "true",
        "--qc_filter",
        "true",
        "--sequence_qc_engine",
        "python",
        "--threads",
        os.environ.get("FETCHM_WEBAPP_QUALITY_THREADS", "4"),
        "--checkm2_threads",
        os.environ.get("FETCHM_WEBAPP_QUALITY_CHECKM2_THREADS", "1"),
    ]
    module_flags = {
        "checkm2": "--run_checkm2",
        "quast": "--run_quast",
        "ani": "--run_ani",
        "mash": "--run_mash",
        "gtdbtk": "--run_gtdbtk",
    }
    for module, flag in module_flags.items():
        command.extend([flag, "true" if module in selected_modules else "false"])
    if "ani" in selected_modules:
        command.extend(["--ani_tool", os.environ.get("FETCHM_WEBAPP_QUALITY_ANI_TOOL", "skani")])
    if status.get("checkm2_db"):
        command.extend(["--checkm2_db", str(status["checkm2_db"])])
    elif status.get("checkm2_db_dir"):
        command.extend(["--checkm2_db_dir", str(status["checkm2_db_dir"])])
        command.extend(["--checkm2_auto_download_db", "true"])
    if status.get("gtdbtk_data_path"):
        command.extend(["--gtdbtk_data_path", str(status["gtdbtk_data_path"])])
    threshold_flags = {
        "min_completeness": "--min_completeness",
        "max_contamination": "--max_contamination",
        "max_contigs": "--max_contigs",
        "min_n50": "--min_n50",
        "min_ani_percent": "--ani_species_threshold",
    }
    for key, flag in threshold_flags.items():
        value = thresholds.get(key)
        if value is not None:
            command.extend([flag, str(value)])
    return command


def module_manifest(config: dict[str, Any]) -> list[dict[str, Any]]:
    module_lookup = {module.key: module for module in QUALITY_MODULES}
    selected = set(config.get("selected_modules") or [])
    status = quality_tool_status()
    items: list[dict[str, Any]] = []
    for key in config.get("selected_modules") or []:
        module = module_lookup.get(str(key))
        if module is None:
            continue
        tool_available = True
        if module.requires_external_tool:
            tool_available = bool(status["available_tools"].get(module.tool_name)) or bool(
                status["nextflow_available"] and status["conda_available"]
            )
        items.append(
            {
                **module.to_dict(),
                "selected": module.key in selected,
                "tool_available": tool_available,
                "execution_status": "built_in" if not module.requires_external_tool else "requires_nextflow",
            }
        )
    return items


def build_quality_handoff(job_id: str, input_path: Path, output_dir: Path, config: dict[str, Any]) -> dict[str, Any]:
    handoff_dir = output_dir / "external_tools" / "quality_check"
    handoff_dir.mkdir(parents=True, exist_ok=True)
    display_command = build_quality_display_command(input_path, output_dir, config)
    nextflow_command = build_nextflow_command(input_path, output_dir, config)
    status = quality_tool_status()
    manifest = {
        "job_id": job_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "input_path": str(input_path),
        "output_dir": str(output_dir),
        "handoff_dir": str(handoff_dir),
        "quality_config": config,
        "module_manifest": module_manifest(config),
        "tool_status": status,
        "display_command": display_command,
        "nextflow_command": nextflow_command,
        "nextflow_execution_enabled": bool(status["nextflow_enabled"]),
    }
    (handoff_dir / "quality_check_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (handoff_dir / "nextflow_command.sh").write_text(
        "#!/usr/bin/env bash\nset -euo pipefail\nexport NXF_SYNTAX_PARSER=\"${NXF_SYNTAX_PARSER:-v1}\"\n"
        + shlex.join(nextflow_command)
        + "\n",
        encoding="utf-8",
    )
    (handoff_dir / "README.md").write_text(
        "\n".join(
            [
                "# FetchM Web External Quality Check Handoff",
                "",
                "This folder records the selected comprehensive QC modules and the Nextflow command",
                "needed to execute PanResistome-style quality checks when the external tool stack",
                "is configured on the server.",
                "",
                "FetchM Web always keeps raw metadata and built-in quick QC outputs separate from",
                "external-tool handoff files so runs remain auditable.",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return manifest
