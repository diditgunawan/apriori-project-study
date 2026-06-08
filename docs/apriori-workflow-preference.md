# Apriori Workflow Preference
- User expects Apriori steps to be followed in this exact order:
  1) Generate C1: count frequency of each individual item.
  2) Prune to L1: keep items meeting minimum support.
  3) Generate C2: combine L1 into item pairs.
  4) Prune to L2: keep pairs meeting minimum support.
  5) Repeat candidate generation + pruning until no more frequent itemsets.
  6) Generate association rules from frequent itemsets and compute confidence + lift.
- Apply this sequence as default when implementing/explaining Apriori for this user.
- Output preference: use simple Excel-like interactive table style in notebook outputs instead of plain text prints when showing result tables.
- Implementation preference: keep table rendering style in a reusable module file (use src/table_output.py) and call it whenever output or summary is presented as a table.
- Table interaction preference: every output/summary table should support manual Excel-like filtering/search/sort behavior.

- Output labeling preference: every table output should display a clear table name/title directly above the table.
- Cell 2 display preference: table 3 (frequent itemsets detail) should show only top 20 rows.


## Metric Reference Preference (Support, Confidence, Lift)
- When presenting Apriori results, include metric explanation in this order: definition, formula, process, interpretation, Apriori-principle relationship.
- Support:
  - Definition: fraction of transactions containing an itemset/rule items.
  - Formula: support(X) = count(X)/N, support(X∪Y) = count(X∪Y)/N.
  - Process: count transaction occurrences, divide by total transactions N.
  - Apriori relation: anti-monotonic property uses support for pruning; if an itemset is infrequent, all supersets are infrequent.
- Confidence:
  - Definition: conditional probability that Y appears when X appears for rule X→Y.
  - Formula: confidence(X→Y) = support(X∪Y)/support(X).
  - Process: compute joint support of X and Y, divide by support of antecedent X.
  - Apriori relation: calculated after frequent itemsets are found; used for rule strength filtering.
- Lift:
  - Definition: strength of X→Y relative to independence between X and Y.
  - Formula: lift(X→Y) = confidence(X→Y)/support(Y) = support(X∪Y)/(support(X)·support(Y)).
  - Process: compute confidence and support(Y), then divide.
  - Interpretation and impact: lift>1 positive association (useful co-occurrence), lift=1 independent, lift<1 negative association/substitution.
  - Apriori relation: post-mining interestingness metric to prioritize actionable rules after support-based frequent pattern mining.


## Complete Summary Table Preference
- Add a complete summary table for association rules with fields:
  - Rule: extracted directly from rule results, formatted as `antecedent -> consequent`.
  - Support: show decimal with 4 digits + percentage column.
    - `support_decimal` format: %.4f
    - `support_pct` format: %.2f%%
  - Confidence: show decimal with 4 digits + percentage column.
    - `confidence_decimal` format: %.4f
    - `confidence_pct` format: %.2f%%
  - Lift: show decimal with 2 digits.
    - `lift_decimal` format: %.2f
  - Interpretation: classify each rule as individual or combination label from strong/useful/weak/unuseful.
- Interpretation rule preference:
  - Strength by confidence:
    - strong if confidence >= 0.70
    - weak if confidence < 0.70
  - Usefulness by lift:
    - useful if lift > 1.00
    - unuseful if lift <= 1.00
  - Combined label allowed (preferred):
    - strong + useful
    - strong + unuseful
    - weak + useful
    - weak + unuseful
