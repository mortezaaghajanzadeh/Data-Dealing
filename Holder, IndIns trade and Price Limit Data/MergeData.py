#%%
import re
import os
import pickle
import numpy as np
import pandas as pd
from persiantools.jdatetime import JalaliDate
from HolderCrawlingFunction import cleaning

#%%
path = r"D:\Holders\\"
arr = os.listdir(path)
allstock = []
arr.remove("Error.p")
arr.remove("Excepted_stock.p")
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
# t = pd.read_pickle(path + "Holders_{}.p".format(2400322364771558))
# df = cleaning([t])
# df[PriceTradeDataColumns].head()
for counter, i in enumerate(arr):
    print(counter)
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


# %%
arr = os.listdir(path + "HolderData")
data = pd.DataFrame()
for counter, i in enumerate(arr):
    print(counter)
    data = data.append(pd.read_pickle(path + "HolderData\\{}".format(i)))
path2 = r"E:\RA_Aghajanzadeh\Data\Stock_holder_new\\"
data[data.Holder != "-"].to_parquet(path2 + "mergerdallData_cleaned.parquet")
#%%
arr = os.listdir(path + "PriceTradeData")
data = pd.DataFrame()
for counter, i in enumerate(arr):
    print(counter)
    data = data.append(pd.read_pickle(path + "PriceTradeData\\{}".format(i)))

# %%
data = data.replace("-", np.nan)
data = data.replace("", np.nan)
path2 = r"E:\RA_Aghajanzadeh\Data\PriceTradeData\\"
data.to_parquet(path2 + "mergerdallData_cleaned.parquet")
# %%
