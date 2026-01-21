import pandas as pd
from pprint import pprint

df = pd.read_parquet("data/processed/recipes.parquet")

print("\nColumns:", df.columns.tolist())

print("\nFirst recipe:")
print(df.iloc[0][['name', 'source_name', 'source_id', 'category', 'area']])
print("\nIngredients type:", type(df.iloc[0]['ingredients']))
print("\nIngredients (converted to list):")
pprint(df.iloc[0]['ingredients'].tolist())

# find recipes with missing measures
print("\nIngredients with missing measures:")
missing = df.explode("ingredients")
missing = missing[missing["ingredients"].apply(lambda x: x.get("measure") in [None, "", "null", "NULL"])]
print(missing[["name", "source_name", "source_id", "ingredients"]])
print(f"\n{len(missing)} recipes with missing measures")
