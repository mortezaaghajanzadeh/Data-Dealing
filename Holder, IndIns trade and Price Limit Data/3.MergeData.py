#%%
import re
import os
import pickle
import numpy as np
import pandas as pd
from persiantools.jdatetime import JalaliDate
from HolderCrawlingFunction import cleaning
from threading import Thread

#%%
path = r"D:\Holders\\"
arr = os.listdir(path)
allstock = []
arr.remove("Error.p")
arr.remove("Excepted_stock.p")
arr.remove("Second_Excepted_stock.p")
arr.remove("PriceTradeData")
arr.remove("HolderData")

HolderDataColumns = [
    "jalaliDate",
    "date",
    "Holder",
    "Holder_id",
    "Number",
    "Percent",
    "Change",
    "ChangeAmount",
    "Firm",
    "name",
    "shrout",
    "stock_id",
    "close_price",
]

PriceTradeDataColumns = [
    "jalaliDate",
    "date",
    "Firm",
    "name",
    "shrout",
    "basevalue",
    "market",
    "pricechange",
    "priceMin",
    "priceMax",
    "priceYesterday",
    "priceFirst",
    "stock_id",
    "close_price",
    "last_price",
    "count",
    "volume",
    "value",
    "max",
    "min",
    "ind_buy_volume",
    "ins_buy_volume",
    "ind_buy_value",
    "ins_buy_value",
    "ins_buy_count",
    "ind_buy_count",
    "ind_sell_volume",
    "ins_sell_volume",
    "ind_sell_value",
    "ins_sell_value",
    "ins_sell_count",
    "ind_sell_count",
]
#%%
def genFile(i,path,HolderDataColumns,PriceTradeDataColumns):
    t = pd.read_pickle(path + i)
    df = cleaning([t])
    pickle.dump(
        df[HolderDataColumns],
        open(path + "HolderData\HolderData_{}.p".format(i[8:-2]), "wb"),
    )
    pickle.dump(
        df[PriceTradeDataColumns].drop_duplicates(),
        open(path + "PriceTradeData\PriceTradeData_{}.p".format(i[8:-2]), "wb"),
    )

threads = {}
for counter, i in enumerate(arr):
    genFile(i,path,HolderDataColumns,PriceTradeDataColumns)
    print(counter)
#     threads[i] = Thread(
#             target=genFile,
#             args=(i,path,HolderDataColumns,PriceTradeDataColumns),
#         )
#     threads[i].start()
# for i in threads:
#     threads[i].join()
#     print(i)
#%%
def finish(threads,result):
    data = pd.DataFrame()
    tempt = pd.DataFrame()
    for i in threads:
        threads[i].join()
        print(i)
        tempt = tempt.append(result[i])
        result[i] = []
        if len(tempt) > 2e6:
            data = data.append(tempt).drop_duplicates()
            tempt = pd.DataFrame()
    data = data.append(tempt).drop_duplicates()
    return data
# %%
arr = os.listdir(path + "HolderData")

print(len(arr))
def genDate(result,i,counter):
    result[counter] = pd.read_pickle(path + "HolderData\\{}".format(i))
result = {}
threads = {}
for counter, i in enumerate(arr):
    print(counter)
    genDate(result,i,counter)
    threads[counter] = Thread(
            target=genDate,
            args=(result,i,counter),
        )
    threads[counter].start()
data = finish(threads,result)
path2 = r"E:\RA_Aghajanzadeh\Data\Stock_holder_new\\"
data[data.Holder != "-"].to_parquet(
    path2 + "mergerdHolderAllData_cleaned.parquet"
)
#%%
arr = os.listdir(path + "PriceTradeData")
data = pd.DataFrame()
print(len(arr))
def genDate(result,i,counter):
    result[counter] = pd.read_pickle(path + "PriceTradeData\\{}".format(i))
result = {}
threads = {}
for counter, i in enumerate(arr):
    print(counter)
    genDate(result,i,counter)
    threads[counter] = Thread(
            target=genDate,
            args=(result,i,counter),
        )
    threads[counter].start()


data = finish(threads,result)
data = data.replace("-", np.nan)
data = data.replace("", np.nan)
path2 = r"E:\RA_Aghajanzadeh\Data\PriceTradeData\\"
data.to_parquet(path2 + "mergerdPriceAllData_cleaned.parquet")
# %%
