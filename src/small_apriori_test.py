import pandas as pd
from typing import Any
from mlxtend.frequent_patterns import apriori, association_rules
from mlxtend.preprocessing import TransactionEncoder


df = pd.read_csv(
	"/home/komputer7/SparkProjects/online-retail-pyspark/data/staging/online_retail.csv"
)

# Keep only valid product descriptions and non-return quantities.
df = df.dropna(subset=["Description"])
df = df[df["Quantity"] > 0]

# Build a list of items per invoice.
transactions = (
	df.groupby("InvoiceNo")["Description"]
	.apply(lambda x: list(set(x.astype(str))))
	.tolist()
)

te = TransactionEncoder()
te_array: Any = te.fit(transactions).transform(transactions, sparse=False)
if hasattr(te_array, "toarray"):
	te_array = te_array.toarray()
basket_df = pd.DataFrame(te_array, columns=te.columns_)

frequent_itemsets = apriori(basket_df, min_support=0.02, use_colnames=True)
rules = association_rules(frequent_itemsets, metric="confidence", min_threshold=0.3)

print("Frequent itemsets (top 10):")
print(frequent_itemsets.sort_values("support", ascending=False).head(10))

print("\nAssociation rules (top 10 by confidence):")
print(rules.sort_values("confidence", ascending=False).head(10))
