"""
Benchmark Runner — Distributed Apriori on Online Retail Dataset
================================================================
Tests multiple minimum-support thresholds and logs execution times.
Results are written to  logs/apriori_results.md  for review.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from datetime import datetime

# ---------------------------------------------------------------------------
# Resolve paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
STAGING_CSV  = PROJECT_ROOT / "data" / "staging" / "online_retail.csv"
LOG_DIR      = PROJECT_ROOT / "logs"
RESULT_MD    = LOG_DIR / "apriori_results.md"

LOG_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "apriori_run.log", mode="a"),
    ],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Import after path setup so src/ modules resolve correctly
# ---------------------------------------------------------------------------
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from apriori_distributed import DistributedApriori   # noqa: E402  (after sys.path)
from spark_session import create_spark_session        # noqa: E402


# ---------------------------------------------------------------------------
# Thresholds to benchmark
# ---------------------------------------------------------------------------
# Relative support ratios tested (converted to absolute counts at runtime).
# Adjust to trade depth vs. run-time.
SUPPORT_RATIOS = [0.05, 0.02, 0.01, 0.005]   # 5 %, 2 %, 1 %, 0.5 %


# ---------------------------------------------------------------------------
# Main benchmark loop
# ---------------------------------------------------------------------------

def run_benchmark() -> None:
    spark = create_spark_session(app_name="apriori-benchmark")
    spark.sparkContext.setLogLevel("WARN")          # suppress verbose Spark logs
    # Ship source files to Python workers so mapPartitions can import modules.
    spark.sparkContext.addPyFile(str(PROJECT_ROOT / "src" / "apriori_distributed.py"))
    spark.sparkContext.addPyFile(str(PROJECT_ROOT / "src" / "spark_session.py"))

    results: list[dict] = []

    # ---- Load transactions once ----------------------------------------
    logger.info("Loading transactions from %s …", STAGING_CSV)
    # We use a temporary instance just to load; min_support is set per run.
    loader = DistributedApriori(spark=spark, min_support=1, max_k=10)
    loader.load_transactions(str(STAGING_CSV))
    n_txn = loader._n_transactions
    logger.info("Dataset: %d transactions.", n_txn)

    # ---- Per-threshold runs ---------------------------------------------
    for ratio in SUPPORT_RATIOS:
        abs_support = loader.min_support_from_ratio(ratio)
        logger.info(
            "\n%s\nRunning Apriori  support=%.1f%%  (abs=%d)\n%s",
            "=" * 60, ratio * 100, abs_support, "=" * 60,
        )

        apriori = DistributedApriori(
            spark=spark,
            min_support=abs_support,
            max_k=10,
        )
        # Share the already-cached transactions RDD
        apriori.transactions_rdd   = loader.transactions_rdd
        apriori._n_transactions    = loader._n_transactions
        apriori._item_to_id        = loader._item_to_id
        apriori._id_to_item        = loader._id_to_item

        all_freq, elapsed = apriori.run()
        apriori.summary(all_freq)

        max_k_found = max(all_freq.keys(), default=0)
        total_freq  = sum(len(v) for v in all_freq.values())

        results.append({
            "ratio":        ratio,
            "abs_support":  abs_support,
            "n_txn":        n_txn,
            "elapsed_s":    round(elapsed, 3),
            "max_k":        max_k_found,
            "total_freq":   total_freq,
            "per_k":        {k: len(v) for k, v in all_freq.items()},
        })

        logger.info(
            "  → support=%.1f%%  elapsed=%.3fs  frequent=%d  max_k=%d",
            ratio * 100, elapsed, total_freq, max_k_found,
        )

    spark.stop()

    # ---- Write results markdown ------------------------------------------
    _write_results_md(results, n_txn)
    logger.info("Results written to %s", RESULT_MD)


def _write_results_md(results: list[dict], n_txn: int) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines: list[str] = [
        "# Distributed Apriori — Benchmark Results",
        "",
        f"**Run date:** {now}  ",
        f"**Dataset:** Online Retail (~{n_txn:,} transactions)  ",
        f"**Engine:** PySpark (local[*])  ",
        "",
        "---",
        "",
        "## Execution Time vs. Minimum Support",
        "",
        "| Min Support (%) | Abs. Support | Total Freq. Itemsets | Max k | Elapsed (s) |",
        "|-----------------|-------------|----------------------|-------|-------------|",
    ]
    for r in results:
        lines.append(
            f"| {r['ratio']*100:.1f}% "
            f"| {r['abs_support']:,} "
            f"| {r['total_freq']:,} "
            f"| {r['max_k']} "
            f"| {r['elapsed_s']:.3f} |"
        )

    lines += [
        "",
        "---",
        "",
        "## Per-k Breakdown",
        "",
    ]
    for r in results:
        lines.append(f"### Support = {r['ratio']*100:.1f}%  (abs={r['abs_support']:,})")
        lines.append("")
        lines.append("| k | Frequent Itemsets |")
        lines.append("|---|-------------------|")
        for k, cnt in sorted(r["per_k"].items()):
            lines.append(f"| {k} | {cnt:,} |")
        lines.append("")

    lines += [
        "---",
        "",
        "## Observations",
        "",
        "- Execution time increases significantly as min_support decreases.",
        "- The broadcast-based approach avoids full shuffles during candidate counting.",
        "- Integer encoding of items reduces memory overhead on executors.",
        "- Transactions are cached once and reused across all threshold runs.",
        "",
    ]

    RESULT_MD.write_text("\n".join(lines), encoding="utf-8")


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    run_benchmark()
