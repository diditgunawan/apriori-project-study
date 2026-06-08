"""
Adapted Apriori for Distributed Environments — PySpark Implementation
======================================================================
Strategy: Broadcast-based iterative Apriori

Phase 1 – MapReduce for C1 / L1:
    Transactions RDD  →  flatMap(items)  →  reduceByKey(sum)  →  filter(≥ min_support)

Phase 2 – Iterative Ck / Lk:
    For each k ≥ 2:
      1. Driver generates candidates Ck from Lk-1 using apriori_gen (anti-monotone pruning).
      2. Ck is broadcast to all executors as a frozenset dict for O(1) lookup.
      3. Each partition counts matching candidates locally  →  global reduce  →  Lk.
      4. Repeat until Lk is empty or max_k is reached.

Optimisations applied:
    - Transactions are encoded as sorted integer tuples (integer encoding for speed).
    - Transactions RDD is cached after encoding.
    - Candidate set is broadcast so no shuffle is needed during counting.
    - Partitions skip transactions shorter than k (early prune).
    - Local combiners reduce inter-node traffic (combineByKey).
"""

from __future__ import annotations

import itertools
import logging
import time
from typing import Dict, FrozenSet, Iterable, List, Optional, Set, Tuple

from pyspark import Broadcast
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------
Item = int                          # encoded item id
Transaction = Tuple[Item, ...]      # sorted tuple of items (immutable, hashable)
Itemset = FrozenSet[Item]           # candidate or frequent itemset
FreqDict = Dict[Itemset, int]       # itemset → absolute support count


# ---------------------------------------------------------------------------
# Helper: apriori_gen  (Ck from Lk-1)
# ---------------------------------------------------------------------------

def apriori_gen(lk_minus1: List[Itemset]) -> List[Itemset]:
    """
    Standard Apriori candidate generation from Lk-1.

    Join step  : Merge two (k-1)-itemsets that share a (k-2)-length prefix.
    Prune step : Discard any k-candidate whose (k-1)-subsets are not all frequent.
    """
    if not lk_minus1:
        return []

    sorted_lk = [tuple(sorted(fs)) for fs in lk_minus1]
    sorted_lk.sort()
    k_minus1 = len(sorted_lk[0])
    k = k_minus1 + 1
    freq_set: Set[Itemset] = set(lk_minus1)
    candidates: List[Itemset] = []

    for i in range(len(sorted_lk)):
        for j in range(i + 1, len(sorted_lk)):
            a, b = sorted_lk[i], sorted_lk[j]
            # Join: prefixes of length k-2 must match
            if a[:k_minus1 - 1] != b[:k_minus1 - 1]:
                break
            candidate = frozenset(a) | frozenset(b)
            if len(candidate) != k:
                continue
            # Prune: all (k-1)-subsets must be frequent
            if all(
                frozenset(sub) in freq_set
                for sub in itertools.combinations(candidate, k_minus1)
            ):
                candidates.append(candidate)

    return candidates


# ---------------------------------------------------------------------------
# Core counting helpers (run inside executors)
# ---------------------------------------------------------------------------

def _count_candidates_in_partition(
    transactions: Iterable[Transaction],
    bc_candidates: Broadcast,
    k: int,
) -> Iterable[Tuple[Itemset, int]]:
    """
    For each transaction, find which broadcast candidates are subsets of it
    and emit (candidate, 1).  Transactions shorter than k are skipped.
    """
    candidates: List[Itemset] = bc_candidates.value
    for txn in transactions:
        if len(txn) < k:
            continue
        txn_set = frozenset(txn)
        for cand in candidates:
            if cand.issubset(txn_set):
                yield (cand, 1)


# ---------------------------------------------------------------------------
# Phase 1 – MapReduce: C1 → L1
# ---------------------------------------------------------------------------

def phase1_generate_l1(
    transactions_rdd,
    min_support: int,
) -> FreqDict:
    """
    MapReduce job:
        Map  : (transaction) → [(item, 1), ...]
        Reduce: sum by item
        Filter: count ≥ min_support
    Returns a dict {frozenset({item}): count}.
    """
    l1: FreqDict = (
        transactions_rdd
        .flatMap(lambda txn: ((item,) for item in txn))   # emit (item,) tuples
        .map(lambda item: (frozenset(item), 1))
        .reduceByKey(lambda a, b: a + b)
        .filter(lambda kv: kv[1] >= min_support)
        .collectAsMap()
    )
    return l1


# ---------------------------------------------------------------------------
# Phase 2 – Iterative Ck / Lk
# ---------------------------------------------------------------------------

def phase2_iterative(
    transactions_rdd,
    l1: FreqDict,
    min_support: int,
    max_k: int = 10,
) -> Dict[int, FreqDict]:
    """
    Iterative Apriori phase.

    Returns a dict  { k: {itemset: count} }  for k = 1..max_k.
    """
    sc = transactions_rdd.context
    all_freq: Dict[int, FreqDict] = {1: l1}
    lk_prev = list(l1.keys())

    for k in range(2, max_k + 1):
        # --- candidate generation (on driver) ---
        ck = apriori_gen(lk_prev)
        if not ck:
            logger.info("k=%d: no candidates generated — stopping.", k)
            break

        logger.info("k=%d: %d candidates generated.", k, len(ck))

        # --- broadcast candidates to executors ---
        bc_ck = sc.broadcast(ck)

        # --- distributed counting ---
        lk: FreqDict = (
            transactions_rdd
            .mapPartitions(lambda part: _count_candidates_in_partition(part, bc_ck, k))
            .reduceByKey(lambda a, b: a + b)
            .filter(lambda kv: kv[1] >= min_support)
            .collectAsMap()
        )

        bc_ck.unpersist()           # free broadcast memory

        if not lk:
            logger.info("k=%d: no frequent itemsets — stopping.", k)
            break

        logger.info("k=%d: %d frequent itemsets found.", k, len(lk))
        all_freq[k] = lk
        lk_prev = list(lk.keys())

    return all_freq


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

class DistributedApriori:
    """
    End-to-end distributed Apriori on PySpark.

    Parameters
    ----------
    spark        : active SparkSession
    min_support  : absolute minimum support count
    max_k        : maximum itemset length to mine (safety cap)
    """

    def __init__(
        self,
        spark: SparkSession,
        min_support: int,
        max_k: int = 10,
    ) -> None:
        self.spark = spark
        self.sc = spark.sparkContext
        self.min_support = min_support
        self.max_k = max_k
        self._item_to_id: Dict[str, int] = {}
        self._id_to_item: Dict[int, str] = {}

    # ------------------------------------------------------------------
    # Data loading helpers
    # ------------------------------------------------------------------

    def load_transactions(self, csv_path: str) -> None:
        """
        Load the Online Retail CSV, group items by InvoiceNo, encode items
        as integers, cache the result, and store as self.transactions_rdd.

        Only rows with valid InvoiceNo (not starting with 'C') and positive
        Quantity are kept to remove cancellations and returns.
        """
        df = (
            self.spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(csv_path)
            .filter("InvoiceNo IS NOT NULL AND NOT STARTSWITH(InvoiceNo, 'C')")
            .filter("Quantity > 0 OR Quantity IS NULL")   # keep valid purchases
            .select("InvoiceNo", "StockCode")
            .dropna(subset=["InvoiceNo", "StockCode"])
        )

        # Collect distinct items to build an integer encoding on the driver.
        items = [row.StockCode for row in df.select("StockCode").distinct().collect()]
        self._item_to_id = {item: idx for idx, item in enumerate(items)}
        self._id_to_item = {idx: item for item, idx in self._item_to_id.items()}

        item_map = self._item_to_id          # local variable for closure

        # Build transactions: each InvoiceNo → sorted tuple of encoded item ids.
        self.transactions_rdd = (
            df.rdd
            .map(lambda row: (row.InvoiceNo, item_map.get(row.StockCode, -1)))
            .filter(lambda kv: kv[1] != -1)
            .groupByKey()
            .map(lambda kv: tuple(sorted(set(kv[1]))))   # deduplicate + sort
            .cache()
        )

        # Force caching
        self._n_transactions: int = self.transactions_rdd.count()
        logger.info(
            "Loaded %d transactions, %d unique items.",
            self._n_transactions,
            len(self._item_to_id),
        )

    def min_support_from_ratio(self, ratio: float) -> int:
        """Convert a relative support threshold (0–1) to an absolute count."""
        return max(1, int(ratio * self._n_transactions))

    # ------------------------------------------------------------------
    # Main run
    # ------------------------------------------------------------------

    def run(self) -> Tuple[Dict[int, FreqDict], float]:
        """
        Execute both phases and return (all_frequent_itemsets, elapsed_seconds).
        """
        t0 = time.perf_counter()

        # Phase 1
        l1 = phase1_generate_l1(self.transactions_rdd, self.min_support)
        logger.info("Phase 1 complete: %d frequent 1-itemsets.", len(l1))

        if not l1:
            return {}, time.perf_counter() - t0

        # Phase 2
        all_freq = phase2_iterative(
            self.transactions_rdd, l1, self.min_support, self.max_k
        )

        elapsed = time.perf_counter() - t0
        return all_freq, elapsed

    # ------------------------------------------------------------------
    # Decode helper
    # ------------------------------------------------------------------

    def decode(self, itemset: Itemset) -> FrozenSet[str]:
        """Convert an encoded itemset back to original StockCode strings."""
        return frozenset(self._id_to_item[i] for i in itemset)

    def summary(self, all_freq: Dict[int, FreqDict]) -> None:
        """Print a compact summary of mining results."""
        total = sum(len(v) for v in all_freq.values())
        print(f"\n{'='*60}")
        print(f"  min_support = {self.min_support}")
        print(f"  Total frequent itemsets found: {total}")
        for k, freq in sorted(all_freq.items()):
            top = sorted(freq.items(), key=lambda x: -x[1])[:5]
            top_str = ", ".join(
                f"{set(self.decode(fs))}:{cnt}" for fs, cnt in top
            )
            print(f"  L{k}: {len(freq)} itemsets  | top-5: {top_str}")
        print(f"{'='*60}\n")
