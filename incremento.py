import pandas as pd

df = pd.read_csv("../../Desktop/dati_bigdata/merged_data.csv")
print(df.shape)

sampled_df1 = df.sample(n=200000, random_state=1)

remaining_df = df.drop(sampled_df1.index)

sampled_df2 = remaining_df.sample(n=400000, random_state=2)
sampled_df1['close'] = sampled_df1['close'] * 1.1
sampled_df1['open'] = sampled_df1['open'] * 1.1
sampled_df1['low'] = sampled_df1['low'] * 1.1
sampled_df1['high'] = sampled_df1['high'] * 1.1
sampled_df1['volume'] = sampled_df1['volume'] * 1.1
concatenated_df1 = pd.concat([df, sampled_df1], axis=0)
concatenated_df1.to_csv("../../Desktop/dati_bigdata/data_200.csv", index=False)
sampled_df2['close'] = sampled_df2['close'] * 1.1
sampled_df2['open'] = sampled_df2['open'] * 1.1
sampled_df2['low'] = sampled_df2['low'] * 1.1
sampled_df2['high'] = sampled_df2['high'] * 1.1
sampled_df2['volume'] = sampled_df2['volume'] * 1.1
concatenated_df2 = pd.concat([df, sampled_df2], axis=0)
concatenated_df2.to_csv("../../Desktop/dati_bigdata/data_400.csv", index=False)