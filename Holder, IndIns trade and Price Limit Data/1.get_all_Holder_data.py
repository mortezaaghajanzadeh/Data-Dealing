#%%
from HolderCrawlingFunction import *
import pickle
import threading

path = r"E:\RA_Aghajanzadeh\Data\\"
# path = r"G:\Economics\Finance(Prof.Heidari-Aghajanzadeh)\Data\\"
# df = pd.read_parquet(path + "Cleaned_Stock_Prices_1400_06_29.parquet")
# print(len(df.name.unique()))
df = pd.read_parquet(path + "Cleaned_Stock_Prices_14010225.parquet")
print(len(df.name.unique()))
df = df[df.jalaliDate > 13880000]
#%%
[
    "بازار خرده فروشی بورس",
    "بازار جبرانی بورس",
    "بازار معاملات عمده بورس",
]

list(df.market.unique())

#%%
list(df.group_name.unique())
#%%
invalid_names = list(
    df[
        (df.market.isin(
            [
                "بازار خرده فروشی بورس",
                "بازار جبرانی بورس",
                "بازار معاملات عمده بورس",
            ]
        )
        )
    ].name.unique()
)


invalid_names.append("سنگ آهن")
len(invalid_names)
#%%
opend_df = df.loc[df.volume >0]
opend_df = opend_df.groupby(['name']).filter(lambda x: x.shape[0]<60)
for i in opend_df.name.unique():
    invalid_names.append(i)
#%%
df = df[~df.name.isin(invalid_names)]
print(df.name.nunique())
list(df.name.unique())
#%%
df = df.groupby(['name']).filter(lambda x: x.shape[0] >60)
print(df.name.nunique())
list(df.name.unique())
#%%
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
stock_id = "65883838195688438"
dates = date_of_stocks(df, "1")
path2 = r"D:\Holders\\"
del df, gg
error = []
counter = 0
#%%
number, stat, number_days = 1000, False, 25
t = Main2(counter, stock_id, dates, Excepted_stock, {}, number, stat, number_days)
df = cleaning([t])
#%%
df
#%%


for counter, stock_id in enumerate(list(dates.keys())[::]):
    print("#################{}##################".format(len(dates.keys()) + 1))
    try:
        t = Main2(
            counter, stock_id, dates, Excepted_stock, {}, number, stat, number_days
        )
        pickle.dump(t, open(path2 + "Holders_{}.p".format(stock_id), "wb"))
    except:
        print("Error in stock_id {}".format(stock_id))
        error.append(stock_id)
pickle.dump(Excepted_stock, open(path2 + "Excepted_stock.p", "wb"))
pickle.dump(error, open(path2 + "Error.p", "wb"))
#%%

# import os

# arr = os.listdir(path2)
# done_id = []
# for i in arr:
#     done_id.append(i[8:-2])

# # %%
# set(dates.keys()) - set(done_id)
#%%
# ids = list(dates.keys())

# threads = {}
# result = {}
# for stock_id in ids:
#     counter = counter + 1
#     # dates[stock_id] = dates[stock_id][::]
#     threads[stock_id] = Thread(
#         target=Main,
#         args=(counter, stock_id, dates, Excepted_stock, result, 1000, False, 5000),
#     )
#     threads[stock_id].start()
# for i in threads:
#     threads[i].join()
#     pickle.dump(result[i], open(path2 + "Holders_{}.p".format(stock_id), "wb"))
# pickle.dump(Excepted_stock, open(path2 + "Excepted_stock.p", "wb"))

# %%
