"""Simple report generation helpers for Apriori workflows.

This module intentionally keeps reporting lightweight so it can be reused by
both local notebook logic and distributed PySpark scripts.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Callable, Mapping, Optional

import pandas as pd


def _itemset_to_text(itemset: object) -> str:
    if isinstance(itemset, (set, frozenset, tuple, list)):
        return ", ".join(sorted(map(str, itemset)))
    return str(itemset)


def _markdown_table(headers: list[str], rows: list[list[object]]) -> str:
    if not rows:
        return "No data available."

    header_line = "| " + " | ".join(headers) + " |"
    separator = "|" + "|".join(["---" for _ in headers]) + "|"
    data_lines = ["| " + " | ".join(map(str, row)) + " |" for row in rows]
    return "\n".join([header_line, separator, *data_lines])


def build_execution_summary_section(execution_df: pd.DataFrame) -> str:
    """Render a markdown section for threshold benchmark output."""
    if execution_df is None or execution_df.empty:
        return "## Execution Summary\n\nNo execution summary data available."

    view = execution_df.copy()
    rows: list[list[object]] = []
    for _, row in view.iterrows():
        rows.append(
            [
                f"{float(row.get('min_support', 0.0)):.4f}",
                int(row.get("abs_support_count", 0)),
                f"{float(row.get('execution_time_sec', 0.0)):.6f}",
                int(row.get("l1_count", 0)),
                int(row.get("l2_count", 0)),
                int(row.get("l3_count", 0)),
            ]
        )

    table = _markdown_table(
        [
            "Min Support",
            "Abs Support",
            "Execution Time (s)",
            "L1 Count",
            "L2 Count",
            "L3 Count",
        ],
        rows,
    )
    return "## Execution Summary\n\n" + table


def build_frequent_itemsets_section(
    all_freq: Mapping[int, Mapping[frozenset, int]],
    *,
    top_n: int = 5,
    decode_itemset: Optional[Callable[[frozenset], object]] = None,
) -> str:
    """Render top frequent itemsets per level from distributed output."""
    if not all_freq:
        return "## Frequent Itemsets\n\nNo frequent itemsets available."

    lines: list[str] = ["## Frequent Itemsets", ""]
    for k in sorted(all_freq):
        freq_k = all_freq[k]
        lines.append(f"### L{k}")
        lines.append("")
        top = sorted(freq_k.items(), key=lambda x: x[1], reverse=True)[:top_n]
        rows: list[list[object]] = []
        for itemset, count in top:
            decoded = decode_itemset(itemset) if decode_itemset else itemset
            rows.append([_itemset_to_text(decoded), int(count)])
        lines.append(_markdown_table(["Itemset", "Count"], rows))
        lines.append("")
    return "\n".join(lines).strip()


def build_rules_section(rules_df: pd.DataFrame, *, top_n: int = 10) -> str:
    """Render top rules with support, confidence, and lift."""
    if rules_df is None or rules_df.empty:
        return "## Association Rules\n\nNo association rules available."

    required = {"rule", "support", "confidence", "lift"}
    if not required.issubset(rules_df.columns):
        return "## Association Rules\n\nRules data is missing required columns."

    view = rules_df.sort_values(
        by=["confidence", "lift", "support"], ascending=False
    ).head(top_n)
    rows: list[list[object]] = []
    for _, row in view.iterrows():
        rows.append(
            [
                row["rule"],
                f"{float(row['support']):.4f}",
                f"{float(row['confidence']):.4f}",
                f"{float(row['lift']):.2f}",
            ]
        )

    table = _markdown_table(["Rule", "Support", "Confidence", "Lift"], rows)
    return "## Association Rules\n\n" + table


def build_apriori_markdown_report(
    *,
    title: str,
    dataset_name: str,
    execution_df: Optional[pd.DataFrame] = None,
    all_freq: Optional[Mapping[int, Mapping[frozenset, int]]] = None,
    rules_df: Optional[pd.DataFrame] = None,
    notes: Optional[list[str]] = None,
    decode_itemset: Optional[Callable[[frozenset], object]] = None,
) -> str:
    """Build a simple and informative markdown report string."""
    lines: list[str] = [
        f"# {title}",
        "",
        f"- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"- Dataset: {dataset_name}",
        "",
    ]

    if execution_df is not None:
        lines.append(build_execution_summary_section(execution_df))
        lines.append("")

    if all_freq is not None:
        lines.append(
            build_frequent_itemsets_section(
                all_freq,
                top_n=5,
                decode_itemset=decode_itemset,
            )
        )
        lines.append("")

    if rules_df is not None:
        lines.append(build_rules_section(rules_df, top_n=10))
        lines.append("")

    if notes:
        lines.append("## Notes")
        lines.append("")
        lines.extend([f"- {note}" for note in notes])
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def save_markdown_report(content: str, output_path: str | Path) -> Path:
    """Persist markdown report content to disk and return the saved path."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path
