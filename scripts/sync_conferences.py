#!/usr/bin/env python3
"""Rebuild _data/conferences.yml from upstream + gist filter."""

from __future__ import annotations

import copy
import csv
import io
import os
import re
import sys
import unicodedata
import urllib.request
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_PATH = ROOT / "_data" / "conferences.yml"
MANUAL_CONFERENCES_PATH = ROOT / "_data" / "manual_conferences.yml"

UPSTREAM_CONFERENCES_URL = os.getenv(
    "UPSTREAM_CONFERENCES_URL",
    "https://raw.githubusercontent.com/sec-deadlines/sec-deadlines.github.io/master/_data/conferences.yml",
)
GIST_CSV_URL = os.getenv(
    "GIST_CSV_URL",
    "https://gist.githubusercontent.com/Pusnow/6eb933355b5cb8d31ef1abcb3c3e1206/raw/CS%20%EB%B6%84%EC%95%BC%20%EC%9A%B0%EC%88%98%20%ED%95%99%EC%88%A0%EB%8C%80%ED%9A%8C%20%EB%AA%A9%EB%A1%9D.csv",
)
BK21_COLUMN = os.getenv("BK21_COLUMN", "BK21플러스 IF (2018)")
BK21_MIN = float(os.getenv("BK21_MIN", "1"))

# Fallback aliases for entries whose local name differs from the gist shorthand.
NAME_ALIASES = {
    "S&P (Oakland)": {"S&P", "SP", "OAKLAND"},
    "AsiaCCS": {"ASIACCS"},
    "IFIP SEC": {"SEC (IFIP-SEC)", "IFIP SEC", "IFIP-SEC"},
}


def is_distributed_systems_conference(conf: dict) -> bool:
    haystack = " ".join(
        str(conf.get(field, "")) for field in ("name", "description", "comment")
    ).lower()
    return "distributed" in haystack


def score_tag(score: float) -> str:
    return f"BK21-{int(score)}"


def augment_conference(conf: dict, score: float | None = None) -> dict:
    updated = copy.deepcopy(conf)
    tags = list(updated.get("tags", []))

    if score is not None:
        updated["bk21_plus_if_2018"] = score
        bk_tag = score_tag(score)
        if bk_tag not in tags:
          tags.append(bk_tag)

    if is_distributed_systems_conference(updated) and "DIST" not in tags:
        tags.append("DIST")

    updated["tags"] = tags
    return updated


def fetch_text(url: str) -> str:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "sec-deadlines-sync/1.0"},
    )
    with urllib.request.urlopen(request) as response:
        return response.read().decode("utf-8-sig")


def normalize(value: str | None) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFKC", value).upper()
    normalized = normalized.replace("&", " AND ")
    normalized = re.sub(r"[^A-Z0-9]+", "", normalized)
    return normalized


def parse_numeric(value: str | None) -> float | None:
    if not value:
        return None
    match = re.search(r"-?\d+(?:\.\d+)?", value)
    if not match:
        return None
    return float(match.group(0))


def extract_dblp_key(dblp_url: str | None) -> str | None:
    if not dblp_url:
        return None
    match = re.search(r"dblp\.org/db/([^?#]+?)/index\.html", dblp_url)
    if match:
        return match.group(1)
    return None


def load_gist_filter(
    csv_text: str,
) -> tuple[set[str], set[str], set[str], list[dict[str, str]]]:
    reader = csv.DictReader(io.StringIO(csv_text))
    eligible_rows: list[dict[str, str]] = []
    allowed_dblp: set[str] = set()
    allowed_short_names: set[str] = set()
    allowed_full_names: set[str] = set()

    for row in reader:
        score = parse_numeric(row.get(BK21_COLUMN))
        if score is None or score < BK21_MIN:
            continue

        eligible_rows.append(row)

        dblp_key = row.get("DBLP Key", "").strip()
        if dblp_key:
            allowed_dblp.add(dblp_key)

        short_name = normalize(row.get("약자", ""))
        if short_name:
            allowed_short_names.add(short_name)

        full_name = normalize(row.get("학회명", ""))
        if full_name:
            allowed_full_names.add(full_name)

    return allowed_dblp, allowed_short_names, allowed_full_names, eligible_rows


def conference_match_keys(
    conf: dict,
    allowed_dblp: set[str],
    allowed_short_names: set[str],
    allowed_full_names: set[str],
    short_name_counts: dict[str, int],
) -> set[str]:
    match_keys: set[str] = set()
    dblp_key = extract_dblp_key(conf.get("dblp"))
    if dblp_key:
        if dblp_key in allowed_dblp:
            match_keys.add(f"dblp:{dblp_key}")
        return match_keys

    candidates = {normalize(conf.get("description"))}
    for alias in NAME_ALIASES.get(conf.get("name"), set()):
        candidates.add(normalize(alias))

    name_candidate = normalize(conf.get("name"))
    if name_candidate and short_name_counts.get(name_candidate, 0) == 1:
        candidates.add(name_candidate)
    elif len(name_candidate) >= 5:
        candidates.add(name_candidate)

    for candidate in candidates:
        if not candidate:
            continue
        if candidate in allowed_short_names or candidate in allowed_full_names:
            match_keys.add(f"name:{candidate}")
        if len(candidate) >= 8 and any(
            candidate in full_name or full_name in candidate for full_name in allowed_full_names
        ):
            match_keys.add(f"name:{candidate}")

    return match_keys


def build_score_lookup(eligible_rows: list[dict[str, str]]) -> dict[str, float]:
    score_lookup: dict[str, float] = {}
    for row in eligible_rows:
        score = parse_numeric(row.get(BK21_COLUMN))
        if score is None:
            continue

        dblp_key = row.get("DBLP Key", "").strip()
        if dblp_key:
            score_lookup[f"dblp:{dblp_key}"] = score

        short_name = normalize(row.get("약자", ""))
        if short_name:
            score_lookup[f"name:{short_name}"] = score

        full_name = normalize(row.get("학회명", ""))
        if full_name:
            score_lookup[f"name:{full_name}"] = score

    return score_lookup


def dump_yaml(conferences: list[dict]) -> str:
    header = (
        "# AUTO-GENERATED FILE. DO NOT EDIT MANUALLY.\n"
        "# Source: sec-deadlines upstream _data/conferences.yml\n"
        "# Filter: Pusnow gist rows with BK21플러스 IF (2018) >= 1\n\n"
    )
    body = yaml.safe_dump(
        conferences,
        sort_keys=False,
        allow_unicode=True,
        default_flow_style=False,
        width=1000,
    )
    return header + body


def load_manual_conferences() -> list[dict]:
    if not MANUAL_CONFERENCES_PATH.exists():
        return []
    manual = yaml.safe_load(MANUAL_CONFERENCES_PATH.read_text(encoding="utf-8")) or []
    if not isinstance(manual, list):
        raise ValueError(f"{MANUAL_CONFERENCES_PATH} must contain a YAML list")
    return manual


def main() -> int:
    upstream_yaml = fetch_text(UPSTREAM_CONFERENCES_URL)
    gist_csv = fetch_text(GIST_CSV_URL)

    upstream_conferences = yaml.safe_load(upstream_yaml)
    allowed_dblp, allowed_short_names, allowed_full_names, eligible_rows = load_gist_filter(gist_csv)
    score_lookup = build_score_lookup(eligible_rows)
    short_name_counts: dict[str, int] = {}
    for conf in upstream_conferences:
        name_key = normalize(conf.get("name"))
        if name_key:
            short_name_counts[name_key] = short_name_counts.get(name_key, 0) + 1

    filtered = []
    for conf in upstream_conferences:
        match_keys = conference_match_keys(
            conf,
            allowed_dblp,
            allowed_short_names,
            allowed_full_names,
            short_name_counts,
        )
        if not match_keys:
            continue

        matched_scores = [score_lookup[key] for key in match_keys if key in score_lookup]
        if not matched_scores:
            continue

        filtered.append(augment_conference(conf, max(matched_scores)))

    manual_conferences = load_manual_conferences()
    existing_keys = {(conf.get("name"), conf.get("year")) for conf in filtered}
    for conf in manual_conferences:
        conf_key = (conf.get("name"), conf.get("year"))
        if conf_key not in existing_keys:
            filtered.append(augment_conference(conf, conf.get("bk21_plus_if_2018")))
            existing_keys.add(conf_key)

    OUTPUT_PATH.write_text(dump_yaml(filtered), encoding="utf-8")

    matched_labels = sorted(conf["name"] for conf in filtered)

    print(f"Fetched {len(upstream_conferences)} upstream conferences")
    print(f"Eligible gist rows with {BK21_COLUMN} >= {BK21_MIN:g}: {len(eligible_rows)}")
    print(f"Merged {len(manual_conferences)} manual conferences from {MANUAL_CONFERENCES_PATH.relative_to(ROOT)}")
    print(f"Wrote {len(filtered)} conferences to {OUTPUT_PATH.relative_to(ROOT)}")
    print("Matched conferences:")
    for label in matched_labels:
        print(f"  - {label}")

    if not filtered:
        print("No conferences matched; refusing to leave an empty file.", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
