#%%
from HolderCrawlingFunction import *
import pickle
import threading

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
excepted = pd.read_pickle(path2 + "Excepted_stock.p")
newdates = {}
for i in excepted:
    newdates[i[0]] = i[1]
error = pd.read_pickle(path2 + "Error.p")
for i in error:
    newdates[i] = dates[i]
del dates

error = []
counter = 0
for counter, stock_id in enumerate(newdates.keys()):
    print("#################{}##################".format(len(newdates.keys())))
    try:
        t = Main2(counter, stock_id, newdates, Excepted_stock, {}, 10000, False, 100)
        pickle.dump(t, open(path2 + "Excepted2_Holders_{}.p".format(stock_id), "wb"))
    except:
        print("Error in stock_id {}".format(stock_id))
        error.append(stock_id)
pickle.dump(Excepted_stock, open(path2 + "Second_Excepted_stock.p", "wb"))
pickle.dump(error, open(path2 + "Error.p", "wb"))

# %%
