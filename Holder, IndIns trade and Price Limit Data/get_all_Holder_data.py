#%%
from HolderCrawlingFunction import *
import pickle
import threading

path = r"E:\RA_Aghajanzadeh\Data\\"
# path = r"G:\Economics\Finance(Prof.Heidari-Aghajanzadeh)\Data\\"

df = pd.read_parquet(path + "Cleaned_Stock_Prices_1400_06_29.parquet")
df = df[~df.title.str.startswith('ح .')]
df = df.drop(df[(df["name"] == "وقوام") & (df["close_price"] == 1000)].index)

df['volume'] = df.volume.astype(float)
gg = df.groupby('name')
print(len(df.name.unique()))

df['volume'] = df.volume.astype(str)
df = df.dropna()
print(len(df.name.unique()))

mlist = [str(i) for i in range(1380, 1400)]
mlist.sort(reverse=True)
print(mlist)
counter = 0
all_stock_data = []
Excepted_stock = []
error = []

def excepthook(args):
    3 == 1+2

threading.excepthook = excepthook
# %%
stock_id = "46348559193224090"
dates = date_of_stocks(df, "1")
del df, gg

#%%

t = Main2(
    counter, stock_id, dates, Excepted_stock, {}, 5000, True, 200
    )
# %%
all_stock_data.append(t)
clean(all_stock_data)
#%%