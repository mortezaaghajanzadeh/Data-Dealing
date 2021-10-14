#%%
import pandas as pd

path = r"E:\RA_Aghajanzadeh\Data\PriceTradeData\\"
df = pd.read_parquet(path + "mergerdPriceAllData_cleaned.parquet")
# %%
df
# %%
list(df)
# %%
# Max and min are price limits
