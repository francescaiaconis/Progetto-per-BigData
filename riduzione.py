import pandas as pd

# Carica il CSV in un DataFrame
df = pd.read_csv('merged_data.csv')

# Estrai 100 record casuali
df_sample = df.sample(n=100, random_state=42)

# Salva il campione in un nuovo file CSV
df_sample.to_csv('campione_100_record.csv', index=False)
