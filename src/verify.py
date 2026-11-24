"""Stage 7 - pipeline verification: compare record counts across all stages."""
from __future__ import annotations

import sys

from src import config
from src.utils import get_logger, load_metrics, update_metrics

log = get_logger("verify")


def build_report(m: dict) -> tuple[list[tuple[str, int]], list[tuple[str, bool]], list[str]]:
    api, prod, cons = m["api"], m["producer"], m["consumer"]
    val, clean, db = m["validation"], m["cleaning"], m["sqlite"]

    counts = [
        ("Records collected from the API", api["records_collected"]),
        ("Deliberate invalid test records added by producer", prod["test_records_sent"]),
        ("Messages published to Kafka", prod["published"]),
        ("Messages consumed from Kafka", cons["consumed"]),
        ("Valid records (Pandera)", val["valid"]),
        ("Invalid records (Pandera)", val["invalid"]),
        ("Records after cleaning", clean["records_after_cleaning"]),
        ("Records stored in SQLite3", db["records_stored"]),
    ]
    checks = [
        (f"API records >= {config.MIN_RECORDS}", api["records_collected"] >= config.MIN_RECORDS),
        (f"Published messages >= {config.MIN_RECORDS}", prod["published"] >= config.MIN_RECORDS),
        (f"Consumed messages >= {config.MIN_RECORDS}", cons["consumed"] >= config.MIN_RECORDS),
        ("published == API records + test records",
         prod["published"] == api["records_collected"] + prod["test_records_sent"]),
        ("consumed == published", cons["consumed"] == prod["published"]),
        ("valid + invalid == consumed", val["valid"] + val["invalid"] == cons["consumed"]),
        ("all deliberate test records detected by Pandera",
         val["test_records_missed"] == 0 and val["test_records_detected"] == val["test_records_total"]),
        ("at least 3 invalid test records demonstrated",
         val["test_records_detected"] >= 3 or val["real_records_invalid"] >= 3),
        ("cleaned == valid - test - duplicates",
         clean["records_after_cleaning"] == val["valid"] - clean["test_records_removed"] - clean["duplicates_removed"]),
        ("stored in SQLite == cleaned", db["records_stored"] == clean["records_after_cleaning"]),
        ("idempotent loading (2nd run adds 0 rows, same checksum)", bool(db.get("idempotency_passed"))),
        ("SQLite integrity_check == ok", db["verification"]["pragma_integrity_check"] == "ok"),
        ("no duplicate record_id in SQLite", db["verification"]["duplicate_check"]["row_count"] == 0),
    ]
    notes = [
        f"API records ({api['records_collected']}) + test records ({prod['test_records_sent']}) = "
        f"published messages ({prod['published']}).",
        f"Consumed ({cons['consumed']}) equals published ({prod['published']}): no message was lost or duplicated "
        f"(skipped messages of older runs: {cons['skipped_other_runs']}).",
        f"Invalid records ({val['invalid']}) = test records detected ({val['test_records_detected']}) "
        f"+ real invalid API records ({val['real_records_invalid']}); they are quarantined, not loaded.",
        f"Cleaning removed {clean['test_records_removed']} leftover test records and "
        f"{clean['duplicates_removed']} duplicates; {clean['wind_values_imputed']} wind values were imputed (rows kept).",
        f"Stored rows ({db['records_stored']}) equal cleaned rows ({clean['records_after_cleaning']}); "
        f"a second load inserted {db.get('load_2', {}).get('rows_inserted', 'n/a')} rows.",
    ]
    return counts, checks, notes


def main() -> int:
    metrics = load_metrics()
    needed = ["api", "producer", "consumer", "validation", "cleaning", "sqlite"]
    missing = [s for s in needed if s not in metrics]
    if missing:
        log.error("Missing metrics for stages: %s - run the full pipeline first", missing)
        return 1
    counts, checks, notes = build_report(metrics)

    lines = ["# Pipeline verification", "", "| Stage | Records |", "|---|---:|"]
    lines += [f"| {name} | {value} |" for name, value in counts]
    lines += ["", "## Checks", ""] + [f"- [{'x' if ok else ' '}] {name}" for name, ok in checks]
    lines += ["", "## Explanation of count differences", ""] + [f"- {n}" for n in notes]
    config.SUMMARY_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")

    for name, value in counts:
        log.info("%-52s %6d", name, value)
    for name, ok in checks:
        log.info("[%s] %s", "PASS" if ok else "FAIL", name)
    all_ok = all(ok for _, ok in checks)
    update_metrics("verification", {"counts": dict(counts), "checks": dict(checks),
                                    "notes": notes, "all_checks_passed": all_ok})
    log.info("PIPELINE %s", "VERIFIED - all checks passed" if all_ok else "FAILED - see FAIL lines above")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
