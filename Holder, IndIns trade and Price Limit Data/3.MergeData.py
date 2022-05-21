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
def convert_ar_characters(input_str):

    mapping = {
        "ك": "ک",
        "گ": "گ",
        "دِ": "د",
        "بِ": "ب",
        "زِ": "ز",
        "ذِ": "ذ",
        "شِ": "ش",
        "سِ": "س",
        "ى": "ی",
        "ي": "ی",
    }
    return _multiple_replace(mapping, input_str)


def _multiple_replace(mapping, text):
    pattern = "|".join(map(re.escape, mapping.keys()))
    return re.sub(pattern, lambda m: mapping[m.group()], str(text))


#%%
path = r"D:\Holders\\"
arr = os.listdir(path)
allstock = []
arr.remove("Error.p")
arr.remove("Excepted_stock.p")
arr.remove("Second_Excepted_stock.p")
# arr.remove("Second_econd_Excepted_stock.p")
# arr.remove("Second2_Excepted_stock.p")
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

#%%
for i in tqdm(arr):
    genFile(i, path, HolderDataColumns, PriceTradeDataColumns)
#%%
def append_dict(dictionary, data_dict):
    for i in dictionary.keys():
        data_dict[i] += list(dictionary[i].values())
    return data_dict


# %%
arr = os.listdir(path + "HolderData")

# arr.remove("Old")
data_dict = {}

for i in pd.read_pickle(path + "HolderData\\{}".format(arr[0])).to_dict().keys():
    data_dict[i] = []
for i in tqdm(arr[::]):
    tempt = pd.read_pickle(path + "HolderData\\{}".format(i)).to_dict()
    data_dict = append_dict(tempt, data_dict)

#%%
data = pd.DataFrame.from_dict(data_dict)
# del data_dict
data[data.name == "هاي وب"]
#%%
print(len(data))
data = data.drop_duplicates()  # keep='first')
print(len(data))
data[data.name == "هاي وب"]

#%%
path2 = r"E:\RA_Aghajanzadeh\Data\Stock_holder_new\\"
data["stock_id"] = data.stock_id.astype(float)
data["date"] = data.date.astype(int)
data["name"] = data["name"].apply(lambda x: convert_ar_characters(x))
data["name"] = data["name"].apply(lambda x: x.strip())
data["Holder"] = data["Holder"].apply(lambda x: convert_ar_characters(x))
data["Holder"] = data["Holder"].apply(lambda x: x.strip())
data["jalaliDate"] = data.jalaliDate.apply(
    lambda x: int(x.split("-")[0] + x.split("-")[1] + x.split("-")[2])
)
# data = data[data.Holder != "-"]
#%%
old_data_df = pd.read_pickle(path2 + "mergerdHolderAllData_cleaned.p")
data = data.append(old_data_df).reset_index(drop=True)
print(len(data))
data = data.drop_duplicates(keep="first")
print(len(data))
#%%
data.to_pickle(path2 + "mergerdHolderAllData_cleaned.p")
#%%
arr = os.listdir(path + "PriceTradeData")

# arr.remove("Old")
data_dict = {}

for i in pd.read_pickle(path + "PriceTradeData\\{}".format(arr[0])).to_dict().keys():
    data_dict[i] = []

for i in tqdm(arr):
    tempt = pd.read_pickle(path + "PriceTradeData\\{}".format(i)).to_dict()
    data_dict = append_dict(tempt, data_dict)


#%%
data = pd.DataFrame.from_dict(data_dict)
del data_dict
print(len(data))
data = data.drop_duplicates(keep="first")
print(len(data))
data = data.replace("-", np.nan)
data = data.replace("", np.nan)
data["name"] = data["name"].apply(lambda x: convert_ar_characters(x))
data["name"] = data["name"].apply(lambda x: x.strip())
data["date"] = data["date"].astype(int)
data["jalaliDate"] = data.jalaliDate.apply(
    lambda x: int(x.split("-")[0] + x.split("-")[1] + x.split("-")[2])
)
path2 = r"E:\RA_Aghajanzadeh\Data\PriceTradeData\\"
data["stock_id"] = data.stock_id.astype(float)
#%%
old_data_df = pd.read_parquet(path2 + "mergerdPriceAllData_cleaned.parquet")
data = data.append(old_data_df).reset_index(drop=True)
print(len(data))
data = data.drop_duplicates(keep="first")
print(len(data))
data.to_parquet(path2 + "mergerdPriceAllData_cleaned.parquet")
#%%
shrout_df = (
    data[["date", "jalaliDate", "name", "shrout"]]
    .drop_duplicates()
    .reset_index(drop=True)
)
shrout_df["date"] = shrout_df.date.astype(int)
shrout_df = shrout_df.sort_values(by=["name", "date"])
shrout_df.to_csv(
    r"E:\RA_Aghajanzadeh\Data\\" + "SymbolShrout_1400_11_27.csv", index=False
)
shrout_df

# %%
