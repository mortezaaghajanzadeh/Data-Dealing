#%%
import pandas as pd

path = r"E:\RA_Aghajanzadeh\Data\\"
df = pd.read_csv(path + "Cleaned_Stocks_Holders_1400_06_28.csv")
df = df[
    [
        "name",
        "jalaliDate",
        "date",
        "shrout",
        "close_price",
    ]
]
df["MarketCap"] = df.close_price * df.shrout
df.to_csv(path + "SymbolShrout_1400_06_28.csv", index=False)
#%%
df[df.name == 'گکیش']
# %%
