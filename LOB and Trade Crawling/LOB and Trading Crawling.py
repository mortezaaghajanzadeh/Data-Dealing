#%%
from LOBandTradeCrawlingFunction import *

path = r"E:\RA_Aghajanzadeh\Data\\"

import threading

df = pd.read_parquet(path + "Cleaned_Stock_Prices_14001006.parquet")
df = df[~df.title.str.startswith("ح .")]
df = df.drop(df[(df["name"] == "وقوام") & (df["close_price"] == 1000)].index)

df["volume"] = df.volume.astype(float)
gg = df.groupby("name")
print(len(df.name.unique()))


def check(g):
    if len(g) != len(g[g.volume < 1]):
        return g
    return


df["volume"] = df.volume.astype(str)
df = df.dropna()
print(len(df.name.unique()))


error = []


def excepthook(args):
    3 == 1 + 2


threading.excepthook = excepthook
gg = df[["date", "stock_id"]].groupby("stock_id")

dates = gg.apply(function).to_dict()

ids = list(df.stock_id.unique())

path2 = r"D:\{}\{}.p"
counter = 0
del df
del gg 
#%%
# stock_id == 35366681030756042
# gen_LOB_Trade(stock_id, dates[str(stock_id)], 2400, path2,False,250)

#%%
for stock_id in ids:
    counter += 1
    print(
        "Parsed stock count",
        counter,
        stock_id,
        len(dates[str(stock_id)]),
        end="\n",
        flush=True,
    )
    try:
        gen_LOB_Trade(stock_id, dates[str(stock_id)], 2400, path2, False,500)
    except:
        error.append(stock_id)

