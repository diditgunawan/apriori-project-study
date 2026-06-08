"""Reusable helpers for simple Excel-like interactive tables in notebooks."""

from __future__ import annotations

from typing import Optional

from IPython.display import HTML, display
import pandas as pd


def _to_rule_text(value: object) -> str:
  """Normalize rule side values to readable text."""
  if isinstance(value, frozenset):
    return ", ".join(sorted(map(str, value)))
  return str(value)


def _rule_interpretation(confidence: float, lift: float) -> str:
  """Build combined interpretation label using confidence and lift thresholds."""
  strength_label = "strong" if confidence >= 0.70 else "weak"
  usefulness_label = "useful" if lift > 1.00 else "unuseful"
  return f"{strength_label} + {usefulness_label}"


def _resolve_series(df: pd.DataFrame, options: list[str], label: str) -> pd.Series:
  """Resolve a column using common naming variants."""
  for col in options:
    if col in df.columns:
      return df[col]
  raise ValueError(f"Missing {label} column: expected one of {', '.join(options)}")


def _to_itemset_text(value: object) -> str:
  """Normalize itemset values for display."""
  if isinstance(value, (set, frozenset, tuple, list)):
    return ", ".join(sorted(map(str, value)))
  return str(value)


def _safe_float(value: object, default: float = 0.0) -> float:
  """Best-effort numeric conversion for formatting output values."""
  try:
    if value is None:
      return default
    return float(value)  # type: ignore[arg-type]
  except (TypeError, ValueError):
    return default


def _format_count(value: object) -> int:
  """Normalize count values to integer quantity."""
  return int(_safe_float(value))


def _format_pct(value: object) -> str:
  """Format a ratio into percentage text."""
  return f"{_safe_float(value) * 100:.2f}%"


def _format_decimal(value: object, digits: int) -> str:
  """Format numeric value with fixed decimal digits."""
  return f"{_safe_float(value):.{digits}f}"


def _format_decimal_with_pct(value: object, digits: int = 4) -> str:
  """Format decimal and percentage in one cell."""
  numeric = _safe_float(value)
  return f"{numeric:.{digits}f} ({numeric * 100:.2f}%)"


def build_c1_table(c1_df: pd.DataFrame) -> pd.DataFrame:
  """Step 1 (C1): Itemset, Count, Support(%) table."""
  if c1_df.empty:
    return pd.DataFrame(columns=["Itemset", "Count", "Support"])

  itemsets = _resolve_series(c1_df, ["itemsets", "itemset", "Itemset"], "itemset")
  counts = _resolve_series(c1_df, ["count", "Count", "frequency", "qty", "quantity"], "count")
  support = _resolve_series(c1_df, ["support", "Support"], "support")

  return pd.DataFrame(
    {
      "Itemset": itemsets.map(_to_itemset_text),
      "Count": counts.map(_format_count),
      "Support": support.map(_format_pct),
    }
  )


def build_l1_dropped_table(dropped_df: pd.DataFrame) -> pd.DataFrame:
  """Step 2 (Prune -> L1): dropped itemsets with quantity."""
  if dropped_df.empty:
    return pd.DataFrame(columns=["Dropped Itemset", "Quantity"])

  itemsets = _resolve_series(dropped_df, ["itemsets", "itemset", "Itemset"], "itemset")
  counts = _resolve_series(
    dropped_df,
    ["count", "Count", "frequency", "qty", "quantity", "Quantity"],
    "quantity",
  )

  return pd.DataFrame(
    {
      "Dropped Itemset": itemsets.map(_to_itemset_text),
      "Quantity": counts.map(_format_count),
    }
  )


def build_c2_table(c2_df: pd.DataFrame) -> pd.DataFrame:
  """Step 3 (C2): 2-itemsets with count quantity."""
  if c2_df.empty:
    return pd.DataFrame(columns=["itemsets", "Count"])

  itemsets = _resolve_series(c2_df, ["itemsets", "itemset", "Itemset"], "itemset")
  counts = _resolve_series(c2_df, ["count", "Count", "frequency", "qty", "quantity"], "count")

  return pd.DataFrame(
    {
      "itemsets": itemsets.map(_to_itemset_text),
      "Count": counts.map(_format_count),
    }
  )


def build_l2_table(l2_df: pd.DataFrame) -> pd.DataFrame:
  """Step 4 (L2): frequent 2-itemsets table."""
  if l2_df.empty:
    return pd.DataFrame(columns=["itemsets"])

  itemsets = _resolve_series(l2_df, ["itemsets", "itemset", "Itemset"], "itemset")
  result = pd.DataFrame({"itemsets": itemsets.map(_to_itemset_text)})

  if any(col in l2_df.columns for col in ["count", "Count", "frequency", "qty", "quantity"]):
    counts = _resolve_series(l2_df, ["count", "Count", "frequency", "qty", "quantity"], "count")
    result["Count"] = counts.map(_format_count)

  if any(col in l2_df.columns for col in ["support", "Support"]):
    support = _resolve_series(l2_df, ["support", "Support"], "support")
    result["Support"] = support.map(_format_pct)

  return result


def build_c3_table(c3_df: pd.DataFrame) -> pd.DataFrame:
  """Step 5 (C3): 3-itemsets with count and support(%)."""
  if c3_df.empty:
    return pd.DataFrame(columns=["itemsets", "Count", "Support"])

  itemsets = _resolve_series(c3_df, ["itemsets", "itemset", "Itemset"], "itemset")
  counts = _resolve_series(c3_df, ["count", "Count", "frequency", "qty", "quantity"], "count")
  support = _resolve_series(c3_df, ["support", "Support"], "support")

  return pd.DataFrame(
    {
      "itemsets": itemsets.map(_to_itemset_text),
      "Count": counts.map(_format_count),
      "Support": support.map(_format_pct),
    }
  )


def build_l3_final_frequent_itemsets_table(l3_df: pd.DataFrame) -> pd.DataFrame:
  """Step 6 (L3): Final Frequent Itemsets representation."""
  if l3_df.empty:
    return pd.DataFrame(columns=["itemsets", "Count", "Support"])

  itemsets = _resolve_series(l3_df, ["itemsets", "itemset", "Itemset"], "itemset")
  result = pd.DataFrame({"itemsets": itemsets.map(_to_itemset_text)})

  if any(col in l3_df.columns for col in ["count", "Count", "frequency", "qty", "quantity"]):
    counts = _resolve_series(l3_df, ["count", "Count", "frequency", "qty", "quantity"], "count")
    result["Count"] = counts.map(_format_count)

  if any(col in l3_df.columns for col in ["support", "Support"]):
    support = _resolve_series(l3_df, ["support", "Support"], "support")
    result["Support"] = support.map(_format_pct)

  return result


def build_rules_from_l3_table(rules_df: pd.DataFrame) -> pd.DataFrame:
  """Additional Output Table 1: Rule and confidence value + percentage."""
  if rules_df.empty:
    return pd.DataFrame(columns=["Rule", "Confidence"])

  antecedent = _resolve_series(rules_df, ["antecedent", "antecedents", "X"], "antecedent")
  consequent = _resolve_series(rules_df, ["consequent", "consequents", "Y"], "consequent")
  confidence = _resolve_series(rules_df, ["confidence", "Confidence"], "confidence")

  return pd.DataFrame(
    {
      "Rule": antecedent.map(_to_rule_text) + " -> " + consequent.map(_to_rule_text),
      "Confidence": confidence.map(_format_decimal_with_pct),
    }
  )


def build_complete_rule_summary_table(rules_df: pd.DataFrame) -> pd.DataFrame:
  """Build complete rule summary aligned with project preferences.

  Output columns:
  - Rule
  - support_decimal (4 digits)
  - support_pct (2 digits + %)
  - confidence_decimal (4 digits)
  - confidence_pct (2 digits + %)
  - lift_decimal (2 digits)
  - Interpretation
  """
  if rules_df.empty:
    return pd.DataFrame(
      columns=[
        "Rule",
        "support_decimal",
        "support_pct",
        "confidence_decimal",
        "confidence_pct",
        "lift_decimal",
        "Interpretation",
      ]
    )

  candidate = rules_df.copy()

  # Resolve rule-side columns from common naming variants used across notebooks.
  if "antecedent" in candidate.columns:
    antecedent_series = candidate["antecedent"]
  elif "X" in candidate.columns:
    antecedent_series = candidate["X"]
  elif "antecedents" in candidate.columns:
    antecedent_series = candidate["antecedents"]
  else:
    raise ValueError("Missing antecedent column: expected one of antecedent/X/antecedents")

  if "consequent" in candidate.columns:
    consequent_series = candidate["consequent"]
  elif "Y" in candidate.columns:
    consequent_series = candidate["Y"]
  elif "consequents" in candidate.columns:
    consequent_series = candidate["consequents"]
  else:
    raise ValueError("Missing consequent column: expected one of consequent/Y/consequents")

  for metric_col in ["support", "confidence", "lift"]:
    if metric_col not in candidate.columns:
      raise ValueError(f"Missing metric column: {metric_col}")

  summary = pd.DataFrame(
    {
      "Rule": antecedent_series.map(_to_rule_text)
      + " -> "
      + consequent_series.map(_to_rule_text),
      "support_decimal": candidate["support"].map(lambda x: _format_decimal(x, 4)),
      "support_pct": candidate["support"].map(_format_pct),
      "confidence_decimal": candidate["confidence"].map(lambda x: _format_decimal(x, 4)),
      "confidence_pct": candidate["confidence"].map(_format_pct),
      "lift_decimal": candidate["lift"].map(lambda x: _format_decimal(x, 2)),
      "Interpretation": candidate.apply(
        lambda r: _rule_interpretation(_safe_float(r["confidence"]), _safe_float(r["lift"])),
        axis=1,
      ),
    }
  )

  return summary


def show_apriori_table(df: pd.DataFrame, table_id: str, table_title: str) -> pd.DataFrame:
  """Convenience wrapper to keep Apriori step table titles consistent."""
  return show_simple_interactive_table(df, table_id=table_id, table_title=table_title)


def show_simple_interactive_table(
    df: pd.DataFrame,
    table_id: str,
    *,
    max_rows: Optional[int] = None,
    page_length: int = 20,
    table_title: Optional[str] = None,
) -> pd.DataFrame:
    """Render a simple, Excel-like interactive DataTable in Jupyter.

    Args:
        df: Source DataFrame to render.
        table_id: Unique HTML id for the table element.
        max_rows: Optional row cap for faster notebook rendering.
        page_length: Default rows per page.
        table_title: Optional title text displayed above the table.

    Returns:
        The DataFrame view that was rendered.
    """
    # Global presentation rule: keep all rendered tables compact.
    effective_max_rows = 5 if max_rows is None else min(max_rows, 5)
    effective_page_length = min(page_length, 5)

    view = df.head(effective_max_rows).copy()

    if table_title:
      display(HTML(f"<div class='apriori-table-title'>{table_title}</div>"))

    display(
        HTML(
            """
<link rel='stylesheet' href='https://cdn.datatables.net/1.13.8/css/jquery.dataTables.min.css'>
<style>
div.apriori-table-title {
    font-family: Arial, sans-serif;
    font-size: 1rem;
    font-weight: 600;
    margin: 0.6rem 0 0.35rem;
    color: #111827;
}
table.dataTable tbody tr, table.dataTable thead th, table.dataTable tbody td {
    background: #ffffff !important;
    color: #111827 !important;
}
table.dataTable {
    border-collapse: collapse !important;
    width: 100% !important;
    border: 1px solid #d1d5db !important;
  table-layout: fixed !important;
}
table.dataTable th,
table.dataTable td {
  white-space: normal !important;
  word-break: break-word !important;
  vertical-align: top !important;
  line-height: 1.35 !important;
}
.dataTables_wrapper .dataTables_length,
.dataTables_wrapper .dataTables_filter,
.dataTables_wrapper .dataTables_info,
.dataTables_wrapper .dataTables_paginate {
    color: #374151 !important;
}
.dataTables_wrapper .dataTables_filter input,
.dataTables_wrapper .dataTables_length select {
    background: #ffffff !important;
    border: 1px solid #d1d5db !important;
    border-radius: 4px !important;
    padding: 0.2rem 0.35rem !important;
}
</style>
<script src='https://code.jquery.com/jquery-3.7.1.min.js'></script>
<script src='https://cdn.datatables.net/1.13.8/js/jquery.dataTables.min.js'></script>
"""
        )
    )

    html_table = view.to_html(
        index=False,
        table_id=table_id,
        classes='display compact',
        border=0,
    )
    display(HTML(html_table))

    display(
        HTML(
            f"""
<script>
(function() {{
  const init = () => {{
    if (window.jQuery && jQuery.fn && jQuery.fn.DataTable) {{
      if (!jQuery.fn.DataTable.isDataTable('#{table_id}')) {{
        jQuery('#{table_id}').DataTable({{
          pageLength: {effective_page_length},
          lengthMenu: [[20, 50, 100, -1], [20, 50, 100, 'All']],
          ordering: true,
          searching: true,
          info: true,
          autoWidth: false
        }});
      }}
    }} else {{
      setTimeout(init, 120);
    }}
  }};
  init();
}})();
</script>
"""
        )
    )

    return view
