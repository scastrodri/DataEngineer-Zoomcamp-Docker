import sys
import pandas as pd


print("arguments", sys.argv)

month = int(sys.argv[1])

df = pd.DataFrame({"day": [1, 2], "num_passangers": [3, 4]})
df['month'] = month
df.to_parquet(f"output_{month}.parquet", index=False)

print(f"Running pipeline for month={month}")
print(df.head())