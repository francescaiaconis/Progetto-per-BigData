import pandas as pd

# Caricare i dati
historical_stock_prices = pd.read_csv("../../Downloads/historical_stock_prices.csv/historical_stock_prices.csv")
historical_stocks = pd.read_csv("../../Downloads/historical_stock_prices.csv/historical_stocks.csv")

# Visualizzare le prime righe per comprendere la struttura dei dati
print(historical_stock_prices.head())
print(historical_stocks.head())

historical_stock_prices=historical_stock_prices.drop(columns='adj_close')
historical_stocks=historical_stocks[historical_stocks["ticker"].isin(historical_stock_prices["ticker"])]

# Pattern e sostituzione
pattern = r'"[^"]*?,\s*[^"]*?"'  # Regex per catturare le virgole all'interno di frasi racchiuse tra virgolette
# Sostituzione di tutte le virgole con spazi
historical_stocks['name'] = historical_stocks['name'].str.replace(',', ' ')


# Pulizia dei dati: rimuovere le righe con valori nulli
cleaned_prices = historical_stock_prices.fillna("N/A")
cleaned_stocks = historical_stocks.fillna("N/A")

# Unire i dataset sulla colonna 'ticker'
merged_data = pd.merge(cleaned_prices, cleaned_stocks, on="ticker", how="inner",validate="many_to_one")
merged_data=merged_data.fillna("N/A")

# Salvataggio del dataset pulito
merged_data.to_csv("../../Downloads/historical_stock_prices.csv/merged_data.csv", index=False)
print(merged_data.head())
      
print("Preprocessing completato e dati salvati")
