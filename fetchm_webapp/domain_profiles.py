"""Domain profile definitions for canonical FetchM metadata workflows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class DomainProfile:
    key: str
    label: str
    ncbi_taxon_id: int
    snapshot_suffix: str
    user_agent_token: str
    rule_scopes: tuple[str, ...]
    public_enabled: bool = False

    def snapshot_id(self, timestamp: datetime) -> str:
        return f"{timestamp.strftime('%Y%m%dT%H%M%SZ')}_{self.snapshot_suffix}"

    def applies_rule_scope(self, scope: str) -> bool:
        return scope.strip().lower() in self.rule_scopes


BACTERIA_PROFILE = DomainProfile(
    key="bacteria",
    label="Bacteria",
    ncbi_taxon_id=2,
    snapshot_suffix="genbank_bacteria_root",
    user_agent_token="canonical-bacterial-inventory",
    rule_scopes=("common", "prokaryote", "bacteria"),
    public_enabled=True,
)

ARCHAEA_PROFILE = DomainProfile(
    key="archaea",
    label="Archaea",
    ncbi_taxon_id=2157,
    snapshot_suffix="genbank_archaea_root",
    user_agent_token="canonical-archaea-inventory",
    rule_scopes=("common", "prokaryote", "archaea"),
    public_enabled=False,
)

DOMAIN_PROFILES = {
    BACTERIA_PROFILE.key: BACTERIA_PROFILE,
    ARCHAEA_PROFILE.key: ARCHAEA_PROFILE,
}


def domain_profile(key: str | None = None) -> DomainProfile:
    normalized = (key or BACTERIA_PROFILE.key).strip().lower()
    try:
        return DOMAIN_PROFILES[normalized]
    except KeyError as exc:
        allowed = ", ".join(sorted(DOMAIN_PROFILES))
        raise ValueError(f"Unsupported FetchM domain profile {key!r}; expected one of: {allowed}") from exc


def domain_profile_from_snapshot_id(snapshot_id: str | None) -> DomainProfile:
    text = snapshot_id or ""
    for profile in DOMAIN_PROFILES.values():
        if text.endswith(profile.snapshot_suffix):
            return profile
    return BACTERIA_PROFILE


def domain_profile_from_taxon_id(taxon_id: int | str | None) -> DomainProfile:
    try:
        parsed = int(taxon_id)
    except (TypeError, ValueError):
        return BACTERIA_PROFILE
    for profile in DOMAIN_PROFILES.values():
        if profile.ncbi_taxon_id == parsed:
            return profile
    return BACTERIA_PROFILE


def validate_snapshot_id_for_profile(snapshot_id: str, profile: DomainProfile) -> str:
    normalized = (snapshot_id or "").strip()
    if not normalized:
        raise ValueError("Snapshot ID is required.")
    if not normalized.endswith(profile.snapshot_suffix):
        raise ValueError(
            f"Snapshot ID {normalized!r} does not match domain profile {profile.key!r}; "
            f"expected suffix {profile.snapshot_suffix!r}."
        )
    return normalized

