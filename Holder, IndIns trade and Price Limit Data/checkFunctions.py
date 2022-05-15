#%%
from tkinter import NW
from HolderCrawlingFunction import *
import pickle
import threading

path = r"E:\RA_Aghajanzadeh\Data\\"
# path = r"G:\Economics\Finance(Prof.Heidari-Aghajanzadeh)\Data\\"
# df = pd.read_parquet(path + "Cleaned_Stock_Prices_1400_06_29.parquet")
# print(len(df.name.unique()))
df = pd.read_parquet(path + "Cleaned_Stock_Prices_14010122.parquet")
print(len(df.name.unique()))
df = df[df.jalaliDate > 13880000]
#%%
# df = df[~df.title.str.startswith("ح .")]
df = df.drop(df[(df["name"] == "وقوام") & (df["close_price"] == 1000)].index)

df["volume"] = df.volume.astype(float)
gg = df.groupby("name")
print(len(df.name.unique()))

df["volume"] = df.volume.astype(str)
# df = df.dropna()
print(len(df.name.unique()))

all_stock_data = []
Excepted_stock = []
error = []


def excepthook(args):
    3 == 1 + 2


threading.excepthook = excepthook
#%%
stock_id = "17617474823279712"
dates = date_of_stocks(df, "1")
path2 = r"D:\Holders\\"
del df, gg
error = []
counter = 0
# %%
import time

now = time.time()
number, stat, number_days = 1000, False, 10
t = Main2(counter, stock_id, dates, Excepted_stock, {}, number, stat, number_days)
print(time.time() - now)
#%%
now = time.time()
number, stat, number_days = 1000, False, 20
t = Main2(counter, stock_id, dates, Excepted_stock, {}, number, stat, number_days)
print(time.time() - now)
#%%
now = time.time()
number, stat, number_days = 1000, False, 40
t = Main2(counter, stock_id, dates, Excepted_stock, {}, number, stat, number_days)
print(time.time() - now)
#%%
now = time.time()
number, stat, number_days = 1000, False, 50
t = Main2(counter, stock_id, dates, Excepted_stock, {}, number, stat, number_days)
print(time.time() - now)
#%%
now = time.time()
number, stat, number_days = 1000, False, 75
t = Main2(counter, stock_id, dates, Excepted_stock, {}, number, stat, number_days)
print(time.time() - now)
#%%
df = cleaning([t])
# %%
df
