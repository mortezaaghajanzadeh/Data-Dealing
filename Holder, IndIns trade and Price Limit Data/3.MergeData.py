#%%
import re
import os
import pickle
import numpy as np
import pandas as pd
from persiantools.jdatetime import JalaliDate
from tqdm import tqdm
from HolderCrawlingFunction import cleaning
from threading import Thread

#%%
path = r"D:\Holders\\"
arr = os.listdir(path)
allstock = []
arr.remove("Error.p")
arr.remove("Excepted_stock.p")
arr.remove("Second_Excepted_stock.p")
arr.remove("Second_econd_Excepted_stock.p")
arr.remove("Second2_Excepted_stock.p")
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


#%%
def genFile(i, path, HolderDataColumns, PriceTradeDataColumns):
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

# for i in tqdm(arr):
#     genFile(i, path, HolderDataColumns, PriceTradeDataColumns)
#%%
def append_dict(dictionary,data_dict):
    for i in dictionary.keys():
        data_dict[i] += list(dictionary[i].values())
    return data_dict
# %%
arr = os.listdir(path + "HolderData")

arr.remove("Old")
data_dict = {}

for i in pd.read_pickle(path + "HolderData\\{}".format(arr[0])).to_dict().keys():
    data_dict[i] = []
for i in tqdm(arr[::]):
    tempt = pd.read_pickle(path + "HolderData\\{}".format(i)).to_dict()
    data_dict = append_dict(tempt,data_dict)

#%%
data = pd.DataFrame.from_dict(data_dict)
# del data_dict
data[data.name == 'هاي وب']
#%%
print(len(data))
data = data.drop_duplicates()#keep='first')
print(len(data))
data[data.name == 'هاي وب']
#%%
#% Check
# id = "43362635835198978"
# i = 'HolderData_{}.p'.format(id)
# tempt = pd.read_pickle(path + "HolderData\\{}".format(i))
# data_dict =  append_dict(tempt,data_dict)
tempt
#%%

#%%
path2 = r"E:\RA_Aghajanzadeh\Data\Stock_holder_new\\"
data["stock_id"] = data.stock_id.astype(float)
data["date"] = data.date.astype(int)
data["jalaliDate"] = data.jalaliDate.apply(
    lambda x: int(x.split("-")[0] + x.split("-")[1] + x.split("-")[2])
)
# data = data[data.Holder != "-"]
#%%
old_data_df = pd.read_pickle(path2 + "mergerdHolderAllData_cleaned.p")
data = data.append(old_data_df).reset_index(drop = True)
print(len(data))
data = data.drop_duplicates(keep='first')
print(len(data))

#%%
arr = os.listdir(path + "PriceTradeData")

arr.remove("Old")
data_dict = {}

for i in pd.read_pickle(path + "PriceTradeData\\{}".format(arr[0])).to_dict().keys():
    data_dict[i] = []
    
for i in tqdm(arr):
    tempt = pd.read_pickle(path + "PriceTradeData\\{}".format(i)).to_dict()
    data_dict = append_dict(tempt,data_dict)



#%%
data = pd.DataFrame.from_dict(data_dict)
del data_dict
print(len(data))
data = data.drop_duplicates(keep='first')
print(len(data))
data = data.replace("-", np.nan)
data = data.replace("", np.nan)
data["jalaliDate"] = data.jalaliDate.apply(
    lambda x: int(x.split("-")[0] + x.split("-")[1] + x.split("-")[2])
)
path2 = r"E:\RA_Aghajanzadeh\Data\PriceTradeData\\"
data["stock_id"] = data.stock_id.astype(float)
#%%
old_data_df = pd.read_parquet(path2 + "mergerdPriceAllData_cleaned.parquet")
data = data.append(old_data_df).reset_index(drop = True)
print(len(data))
data = data.drop_duplicates(keep='first')
print(len(data))
data.to_parquet(path2 + "mergerdPriceAllData_cleaned.parquet")
# %%