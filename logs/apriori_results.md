# Distributed Apriori — Benchmark Results

**Run date:** 2026-05-06 15:18:14  
**Dataset:** Online Retail (~20,728 transactions)  
**Engine:** PySpark (local[*])  

---

## Execution Time vs. Minimum Support

| Min Support (%) | Abs. Support | Total Freq. Itemsets | Max k | Elapsed (s) |
|-----------------|-------------|----------------------|-------|-------------|
| 5.0% | 1,036 | 33 | 1 | 1.461 |
| 2.0% | 414 | 356 | 3 | 21.303 |
| 1.0% | 207 | 1,857 | 4 | 168.272 |
| 0.5% | 103 | 16,923 | 6 | 626.312 |

---

## Per-k Breakdown

### Support = 5.0%  (abs=1,036)

| k | Frequent Itemsets |
|---|-------------------|
| 1 | 33 |

### Support = 2.0%  (abs=414)

| k | Frequent Itemsets |
|---|-------------------|
| 1 | 276 |
| 2 | 79 |
| 3 | 1 |

### Support = 1.0%  (abs=207)

| k | Frequent Itemsets |
|---|-------------------|
| 1 | 792 |
| 2 | 827 |
| 3 | 220 |
| 4 | 18 |

### Support = 0.5%  (abs=103)

| k | Frequent Itemsets |
|---|-------------------|
| 1 | 1,503 |
| 2 | 7,445 |
| 3 | 5,457 |
| 4 | 2,121 |
| 5 | 368 |
| 6 | 29 |

---

## Observations

- Execution time increases significantly as min_support decreases.
- The broadcast-based approach avoids full shuffles during candidate counting.
- Integer encoding of items reduces memory overhead on executors.
- Transactions are cached once and reused across all threshold runs.
