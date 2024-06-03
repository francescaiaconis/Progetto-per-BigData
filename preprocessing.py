import pandas as pd

# Caricare i dati
historical_stock_prices = pd.read_csv("../../Downloads/historical_stock_prices.csv/historical_stock_prices.csv")
historical_stocks = pd.read_csv("../../Downloads/historical_stock_prices.csv/historical_stocks.csv")

# Visualizzare le prime righe per comprendere la struttura dei dati
print(historical_stock_prices.head())
print(historical_stocks.head())

# Pulizia dei dati: rimuovere le righe con valori nulli
cleaned_prices = historical_stock_prices.dropna()
cleaned_stocks = historical_stocks.dropna()

# Unire i dataset sulla colonna 'ticker'
merged_data = pd.merge(cleaned_prices, cleaned_stocks, on="ticker")

# Salvataggio del dataset pulito
merged_data.to_csv("../../Downloads/historical_stock_prices.csv/merged_data.csv", index=False)

print("Preprocessing completato e dati salvati in 'path/to/cleaned_stock_data.csv'")
