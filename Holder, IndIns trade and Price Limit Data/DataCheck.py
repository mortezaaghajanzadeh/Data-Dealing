#%%
from HolderCrawlingFunction import *
import pickle
import threading

path = r"E:\RA_Aghajanzadeh\Data\\"
# path = r"G:\Economics\Finance(Prof.Heidari-Aghajanzadeh)\Data\\"

df = pd.read_parquet(path + "Cleaned_Stock_Prices_1400_06_29.parquet")
df = df[df.jalaliDate > 13880000]
#%%
df = df[~df.title.str.startswith("ح .")]
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

dates = date_of_stocks(df, "1")
path2 = r"D:\Holders\\"
del df, gg
error = []
counter = 0
#%%
number, stat, number_days = 10000, False, 100 
stock_id = "46348559193224090"
t = Main2(counter, stock_id, dates, Excepted_stock, {},number, stat, number_days)
# %%
df = cleaning([t])
#%%
df[df.date > 20190410].groupby('date').size()

