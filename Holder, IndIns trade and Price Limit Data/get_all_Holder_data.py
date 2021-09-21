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

all_stock_data = []
Excepted_stock = []
error = []

def excepthook(args):
    3 == 1+2

threading.excepthook = excepthook

stock_id = "22255783119783047"
dates = date_of_stocks(df, "1")
del df, gg
#%%
path2 = r"D:\Holders\\"
t = Main2(
    0, stock_id, dates, Excepted_stock, {}, 5000, True, 1000
    )
# %%

for counter,stock_id in enumerate(dates.keys()):
    print("#################{}##################".format(len(dates.keys())+1))
    t = Main2(
    counter, stock_id, dates, Excepted_stock, {}, 5000, True, 500
    )
    pickle.dump(t, open(path2 + "Holders_{}.p".format(stock_id), "wb"))
    print("###################################")
#%%

import os
arr = os.listdir(path2)
print(arr)
# %%
