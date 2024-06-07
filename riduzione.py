import pandas as pd

# Carica il CSV in un DataFrame
df = pd.read_csv('../../Downloads/historical_stock_prices.csv/merged_data.csv')

# Estrai 100 record casuali
df_wtr = df[df['sector'] == 'TECHNOLOGY']

# Salva il campione in un nuovo file CSV
df_wtr.to_csv('campione_record-2.csv', index=False)
