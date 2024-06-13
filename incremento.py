import pandas as pd
import numpy as np
# Carica il CSV in un DataFrame
df = pd.read_csv('../../Downloads/historical_stock_prices.csv/merged_data.csv')

num_samples = 50000
def generate_numerical_data(df, columns, num_samples):
    new_data={}
    for column in columns:
        mean = df[column].mean()
        std = df[column].std()
        new_data[column] = np.random.normal(mean, std, num_samples)
    return pd.DataFrame(new_data)
numerical_col=['open','close','low','high','volume']
new_numerical_data = generate_numerical_data(df, numerical_col, num_samples)
categorical_col=['ticker','date','name','exchange','sector','industry']
new_categorical_data = (df[categorical_col].sample(n=num_samples,replace=True).reset_index(drop=True))
new_data = pd.concat([new_categorical_data, new_numerical_data], axis=1)
new_data=new_data[df.columns]
new_data=new_data.fillna("N/A")
new_data.to_csv('data50.csv', index=False)
result=pd.concat([df,new_data],axis=1,ignore_index=True)
result.to_csv('data_mer_50.csv', index=False)