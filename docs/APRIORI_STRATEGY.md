# Adapted Apriori for Distributed Environments — Strategy & Design

## 1. Problem Context

The classic Apriori algorithm was designed for a single-machine, memory-resident dataset. Scaling it naively to distributed systems introduces two key bottlenecks:

| Bottleneck | Root Cause |
|---|---|
| **Global candidate generation** | Ck depends on the full Lk-1; cannot be done locally per node |
| **Counting phase shuffle** | Every node must count every candidate against every transaction |

This implementation resolves both problems using PySpark's **broadcast variables** and **MapReduce-style aggregation**.

---

## 2. Algorithm Overview

```
Dataset (InvoiceNo → [StockCodes])
          │
          ▼
┌─────────────────────────────────────────────────────┐
│  PHASE 1 — MapReduce: C1 → L1                       │
│                                                     │
│  Map:    transaction  →  (item, 1)  for each item   │
│  Reduce: sum counts by item                         │
│  Filter: count ≥ min_support                        │
└───────────────────────┬─────────────────────────────┘
                        │  L1 (frequent 1-itemsets)
                        ▼
┌─────────────────────────────────────────────────────┐
│  PHASE 2 — Iterative Ck / Lk  (k = 2, 3, …)        │
│                                                     │
│  ① Driver: apriori_gen(Lk-1) → Ck                  │
│  ② Driver: broadcast(Ck) to all executors           │
│  ③ Executors: count subsets  →  (candidate, count)  │
│  ④ Driver: reduce + filter  →  Lk                   │
│  ⑤ Repeat until Lk = ∅ or k > max_k                │
└─────────────────────────────────────────────────────┘
```

---

## 3. Best Strategy for Distributed Apriori

### 3.1 Broadcast-Based Counting (chosen approach)

**Why broadcast?**

In a naive distributed Apriori every executor would need to shuffle data to a single aggregator. Broadcasting the candidate set instead means:

- Each partition independently counts candidates it sees locally.
- Only the (small) partial counts are shuffled across the network — not the transaction data.
- No full data movement. Network cost = `O(|Ck|)` instead of `O(|Transactions|)`.

**When does it break down?**

At very low support thresholds `|Ck|` can become enormous (exponential in k). In practice, set min_support ≥ 0.5 % for this dataset to keep Ck manageable.

### 3.2 Integer Encoding

Items (StockCodes) are encoded to integers once on the driver before distribution:

- Reduces memory per transaction (int vs. string).
- Enables fast set operations (`frozenset.issubset`).
- Encoding/decoding is O(1) using pre-built dicts.

### 3.3 Transaction Caching

The transactions RDD is **cached** after initial load. All k-iterations reuse it from memory, avoiding repeated CSV parsing and encoding.

### 3.4 Cancellation & Invalid Row Filtering

Rows where `InvoiceNo` starts with `'C'` are returns/cancellations. They are excluded before mining to avoid artificially inflating itemset counts.

### 3.5 apriori_gen (Anti-Monotone Pruning)

Candidate generation uses the standard Apriori property:

> If any (k-1)-subset of a candidate k-itemset is not frequent, the candidate cannot be frequent.

This prunes the candidate space before broadcasting, reducing counting work on executors.

---

## 4. Alternative Strategies Considered

| Strategy | Description | Trade-off vs. chosen approach |
|---|---|---|
| **SON Algorithm** | Run local Apriori on each partition, union local frequent sets → global candidates | Lower network traffic but requires multiple passes; complex to tune partition size |
| **FP-Growth (PySpark MLlib)** | Conditional FP-tree, no candidate generation | Better overall; considered where Apriori semantics are not required |
| **Hash-based Pruning (PCY)** | Use hash buckets in Phase 1 to pre-prune C2 | Reduces C2 size; applicable as a Phase 1 extension in future work |
| **Sampling** | Mine a random sample, validate on full data | Approximate; acceptable for exploration but not exact results |

The **broadcast-based iterative approach** was chosen because:
1. It is the most faithful distributed translation of the original Apriori algorithm.
2. It leverages PySpark primitives naturally (RDD broadcast, reduceByKey).
3. It produces exact results (unlike sampling).
4. It is straightforward to debug and reason about.

---

## 5. Data Flow Diagram

```
CSV (staging/online_retail.csv)
   │
   │  spark.read.csv()
   ▼
DataFrame [InvoiceNo, StockCode]
   │
   │  filter cancellations, dropna
   │  groupByKey + encode items as int
   ▼
transactions_rdd: RDD[Tuple[int, ...]]   ← cached in memory
   │
   ├─────────────────────────────────────────────┐
   │  Phase 1                                    │
   │  flatMap → (item, 1)                        │
   │  reduceByKey → (item, count)                │
   │  filter ≥ min_support                       │
   └───────────────────────────► L1 (driver)     │
                                     │           │
                              Phase 2 loop       │
                              apriori_gen → Ck   │
                              broadcast(Ck) ─────┤
                                     │           │
                              mapPartitions ←────┘
                              (count subsets)
                              reduceByKey
                              filter ≥ min_support
                                     │
                                    Lk
```

---

## 6. File Structure

```
online-retail-pyspark/
├── src/
│   ├── apriori_distributed.py   ← Core algorithm (Phase 1 + Phase 2)
│   ├── apriori_runner.py        ← Benchmark runner (multi-threshold)
│   ├── spark_session.py         ← SparkSession factory
│   ├── extract_excel.py         ← Excel → CSV converter
│   └── main.py                  ← Original EDA pipeline
├── data/
│   ├── raw/                     ← Source .xlsx file
│   ├── staging/                 ← online_retail.csv
│   └── curated/                 ← Parquet outputs
├── docs/
│   └── APRIORI_STRATEGY.md      ← This document
└── logs/
    ├── apriori_checklist.md     ← Step-by-step progress log
    ├── apriori_results.md       ← Auto-generated benchmark results
    └── apriori_run.log          ← Runtime log file
```

---

## 7. Running the Benchmark

```bash
# From project root
.venv/bin/python src/apriori_runner.py
```

Results are automatically written to `logs/apriori_results.md`.

### Threshold Guidance

| Min Support | Use Case |
|---|---|
| 5 % | Fast sanity check; very few itemsets |
| 2 % | Balanced depth/speed |
| 1 % | Practical association rules |
| 0.5 % | Deep mining; longest runtime |

---

## 8. Complexity Analysis

| Phase | Time Complexity | Notes |
|---|---|---|
| Phase 1 | O(N × avg_txn_len) | Linear in data; fully parallelised |
| Phase 2 (per k) | O(N × \|Ck\|) | Counting; broadcast eliminates shuffle |
| apriori_gen | O(\|Lk-1\|²) | Done on driver; small for high support |
| Total | O(N × Σ\|Ck\|) | Dominated by low-support iterations |

---

## 9. Known Limitations

- At very low support (< 0.1 %) `|Ck|` can exceed driver memory.
  **Mitigation:** Increase `min_support` or switch to FP-Growth.
- `local[*]` mode uses all local cores but does not scale across machines.
  **Mitigation:** Replace `.master("local[*]")` with a YARN/K8s cluster URL.
- Integer encoding is rebuilt each run; persist the encoding map for repeated runs.
