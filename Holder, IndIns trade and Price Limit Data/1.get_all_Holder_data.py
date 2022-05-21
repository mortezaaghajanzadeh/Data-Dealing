#%%
from HolderCrawlingFunction import *

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
