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
stock_id = "17617474823279712"
dates = date_of_stocks(df, "1")
path2 = r"D:\Holders\\"
del df, gg
error = []
counter = 0
#%%
number, stat, number_days = 10000, False, 100

#%%
for counter, stock_id in enumerate(list(dates.keys())[::]):
    print("#################{}##################".format(len(dates.keys()) + 1))
    try:
        t = Main2(counter, stock_id, dates, Excepted_stock, {}, 10000, False, 1000)
        pickle.dump(t, open(path2 + "Holders_{}.p".format(stock_id), "wb"))
    except:
        print("Error in stock_id {}".format(stock_id))
        error.append(stock_id)
pickle.dump(Excepted_stock, open(path2 + "Excepted_stock.p", "wb"))
pickle.dump(error, open(path2 + "Error.p", "wb"))
#%%

import os

arr = os.listdir(path2)
done_id = []
for i in arr:
    done_id.append(i[8:-2])

# %%
set(dates.keys()) - set(done_id)

# ids = list(dates.keys())

# j = 0
# nums = 25
# tot = int(len(ids)/nums) + 1
# for i in range(1, tot):
#     print("It is set of {} from total {}".format(i,tot-1))
#     k = min(j + nums, len(ids))
#     print(j, k)
#     NoId = ids[j:k]
#     threads = {}
#     result = {}
#     for stock_id in NoId:
#         counter = counter + 1
#         # dates[stock_id] = dates[stock_id][::]
#         threads[stock_id] = Thread(
#             target=Main,
#             args=(counter, stock_id, dates, Excepted_stock, result, 1000, False, 5000),
#         )
#         threads[stock_id].start()

#     for i in threads:
#         threads[i].join()
#         pickle.dump(result[i], open(path2 + "Holders_{}.p".format(stock_id), "wb"))
#     j = k
# pickle.dump(Excepted_stock, open(path2 + "Excepted_stock.p", "wb"))
