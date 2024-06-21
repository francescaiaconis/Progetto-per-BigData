import pandas as pd
df = pd.read_csv("../../Desktop/merged_data.csv")
print(df.shape)
print(df.head())
df = df[df['sector'] == 'BASIC INDUSTRIES']
df = df[df['industry'] == 'ENGINEERING & CONSTRUCTION']

print(df.head())
df.to_csv("../../Downloads/historical_stock_prices.csv/merged_data_truck.csv", index=False)
