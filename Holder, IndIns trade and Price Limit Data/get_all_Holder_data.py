#%%
from HolderCrawlingFunction import *
import pickle
import threading

path = r"E:\RA_Aghajanzadeh\Data\\"
# path = r"G:\Economics\Finance(Prof.Heidari-Aghajanzadeh)\Data\\"

df = pd.read_parquet(path + "Cleaned_Stock_Prices_1400_06_29.parquet")
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

stock_id = "65883838195688438"
dates = date_of_stocks(df, "1")
path2 = r"D:\Holders\\"
del df, gg
#%%

# t = Main2(
#     0, stock_id, dates, Excepted_stock, {}, 5000, True, 5000
#     )
# %%

error = []
counter = 0
j = 0
ids = list(dates.keys())
nums = 10
tot = (int(len(ids) / nums)) + 2
# import os
# arr = os.listdir(path2)
# arr.remove("Error.p")
# arr.remove("Excepted_stock.p")
# arr.remove("PriceTradeData")
# arr.remove("HolderData")
# done_id = []
# for i in arr:
#     done_id.append(i[8:-2])
# again_id = list(set(ids)-set(done_id))
# ids = again_id


for i in range(1, tot ):
    print("It is set of {} from total {}".format(i,tot))
    k = min(j + nums, len(ids))
    print(j, k)
    NoId = ids[j:k]
    threads = {}
    result = {}
    for stock_id in NoId:
        counter = counter + 1
        # dates[stock_id] = dates[stock_id][::]
        threads[stock_id] = Thread(
            target=Main,
            args=(counter, stock_id, dates, Excepted_stock, result, 1000, True, 500),
        )
        threads[stock_id].start()

    for i in threads:
        threads[i].join()
        pickle.dump(result[i], open(path2 + "Holders_{}.p".format(stock_id), "wb"))
    j = k
pickle.dump(Excepted_stock, open(path2 + "Excepted_stock.p", "wb"))
pickle.dump(error, open(path2 + "Error.p", "wb"))
#%%
# for counter,stock_id in enumerate(dates.keys()):
#     print("#################{}##################".format(len(dates.keys())+1))
#     try:
#         t = Main2(
#         counter, stock_id, dates, Excepted_stock, {}, 10000, True, 2000
#         )
#         pickle.dump(t, open(path2 + "Holders_{}.p".format(stock_id), "wb"))
#     except:
#         print("Error in stock_id {}".format(stock_id))
#         error.append(stock_id)
# pickle.dump(Excepted_stock, open(path2 + "Excepted_stock.p", "wb"))
# pickle.dump(error, open(path2 + "Error.p", "wb"))
#%%

# import os
# arr = os.listdir(path2)
# done_id = []
# for i in arr:
#     done_id.append(i[8:-2])
