import pandas as pd
df = pd.read_csv("../../Downloads/historical_stock_prices.csv/merged_data.csv")
print(df.shape)
print(df.head())
df = df.sample(n=100, random_state=1)
df = df.dropna(axis=0)
df.to_csv("../../Downloads/historical_stock_prices.csv/merged_data_ridotti.csv", index=False)
