#%%
from HolderCrawlingFunction import *
import pickle
import threading

path = r"E:\RA_Aghajanzadeh\Data\\"
# path = r"G:\Economics\Finance(Prof.Heidari-Aghajanzadeh)\Data\\"
df = pd.read_parquet(path + "Cleaned_Stock_Prices_1400_06_29.parquet")
print(len(df.name.unique()))
df = pd.read_parquet(path + "Cleaned_Stock_Prices_14001006.parquet")
print(len(df.name.unique()))
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
excepted = pd.read_pickle(path2 + "Excepted_stock.p")
newdates = {}
for i in excepted:
    newdates[i[0]] = i[1]
error = pd.read_pickle(path2 + "Error.p")
for i in error:
    newdates[i] = dates[i]
# del dates

error = []
counter = 0
#%%
for counter, stock_id in enumerate(newdates.keys()):
    print("#################{}##################".format(len(newdates.keys())))
    try:
        t = Main2(counter, stock_id, newdates, Excepted_stock, {}, 10000, False, 100)
        pickle.dump(t, open(path2 + "Excepted_Holders_{}.p".format(stock_id), "wb"))
    except:
        print("Error in stock_id {}".format(stock_id))
        error.append(stock_id)
pickle.dump(Excepted_stock, open(path2 + "Second_Excepted_stock.p", "wb"))
pickle.dump(error, open(path2 + "Error.p", "wb"))
#%%
excepted = pd.read_pickle(path2 + "Second_Excepted_stock.p")
newdates = {}
for i in excepted:
    newdates[i[0]] = i[1]
error = pd.read_pickle(path2 + "Error.p")
for i in error:
    newdates[i] = dates[i]
del dates

error = []
counter = 0
#%%
for counter, stock_id in enumerate(newdates.keys()):
    print("#################{}##################".format(len(newdates.keys())))
    try:
        t = Main2(counter, stock_id, newdates, Excepted_stock, {}, 10000, False, 100)
        pickle.dump(t, open(path2 + "Excepted2_Holders_{}.p".format(stock_id), "wb"))
    except:
        print("Error in stock_id {}".format(stock_id))
        error.append(stock_id)
pickle.dump(Excepted_stock, open(path2 + "Second2_Excepted_stock.p", "wb"))
pickle.dump(error, open(path2 + "Error.p", "wb"))
# %%
