#%%
import pandas as pd
path = r"E:\RA_Aghajanzadeh\Data\PriceTradeData\\"
df = pd.read_parquet(path+ "mergerdallData_cleaned.parquet" )
# %%
df
# %%
len(df.name.unique())