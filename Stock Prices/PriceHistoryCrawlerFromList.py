#%%
# %% [markdown]
# ## Tsetmc.com CRAWLER (Price)

# %%
from gevent import monkey as curious_george

curious_george.patch_all(thread=False, select=False)
import re
import requests
from persiantools.jdatetime import JalaliDate
import json
import pandas as pd
import datetime
from persiantools.jdatetime import JalaliDate, JalaliDateTime
import logging
# from PriceHistoryCrawler import get_stock_price_history

pathd = r"C:\Users\RA\Desktop\RA_Aghajanzadeh\Data\\"
pathd = r"E:\RA_Aghajanzadeh\Data\\"



object = pd.read_pickle(r'ids.p')
#%%
Id = []
for i in object:
    for j in i:
        Id.append(j)



# %%
def get_stock_detail(stock_id):
    try:
        # If you replace {} with a stock ID in this URL, you can access that stocks page in tsetmc.com.
        logging.captureWarnings(True)
        url = "http://www.tsetmc.com/Loader.aspx?ParTree=151311&i={}".format(stock_id)
        r = requests.get(url, timeout=1,verify=False)
        #     print(url)

        # We get stock details in stock page (text of page) hear:
        stock = {"stock_id": stock_id}
        stock["group_name"] = re.findall(r"LSecVal='([\D]*)',", r.text)[0]
        stock["instId"] = re.findall(r"InstrumentID='([\w\d]*)',", r.text)[0]
        stock["insCode"] = (
            stock_id if re.findall(r"InsCode='(\d*)',", r.text)[0] == stock_id else 0
        )
        stock["baseVol"] = float(re.findall(r"BaseVol=([\.\d]*),", r.text)[0])
        try:
            stock["name"] = re.findall(r"LVal18AFC='([\D]*)',", r.text)[0]
        except:
            return
        try:
            stock["title"] = re.findall(r"Title='([\D]*)',", r.text)[0]
        except:
            return
        try:
            stock["sectorPe"] = float(re.findall(r"SectorPE='([\.\d]*)',", r.text)[0])
        except:
            stock["sectorPe"] = None
        try:
            stock["shareCount"] = float(re.findall(r"ZTitad=([\.\d]*),", r.text)[0])
        except:
            stock["shareCount"] = None

        try:
            stock["estimatedEps"] = float(
                re.findall(r"EstimatedEPS='([\.\d]*)',", r.text)[0]
            )
        except:
            stock["estimatedEps"] = None
        if stock["name"] == "',DEven='',LSecVal='',CgrValCot='',Flow='',InstrumentID='":
            return False
    except:
        print("Again stock detail ", stock_id)
        stock = get_stock_detail(stock_id)
    return stock

def get_stock_price_history(stock_id):
    # try:
    url = "https://members.tsetmc.com/tsev2/data/InstTradeHistory.aspx?i={}&Top=999999&A=1".format(
        stock_id
    )
    #     print(url)
    logging.captureWarnings(True)
    r = requests.get(url, timeout=1, verify=False)

    price_history_text = r.text
    price_history_list = price_history_text.split(";")
    history = list()

    for item in price_history_list:
        item_list = item.split("@")
        if len(item_list) < 3:
            break
        history_item = {}
        history_item["date"] = item_list[0]
        history_item["jalaliDate"] = JalaliDate.to_jalali(
            int(item_list[0][0:4]), int(item_list[0][4:6]), int(item_list[0][6:8])
        ).isoformat()
        history_item["max_price"] = item_list[1]
        history_item["min_price"] = item_list[2]
        history_item["close_price"] = item_list[3]
        history_item["last_price"] = item_list[4]
        history_item["open_price"] = item_list[5]
        history_item["yesterday_price"] = item_list[6]
        history_item["value"] = item_list[7]
        history_item["volume"] = item_list[8]
        history_item["quantity"] = item_list[9]
        history.append(history_item)
    # except:
    #     print("Again ", stock_id)
        # history = get_stock_price_history(stock_id)
    return history


#%%
stock_id = object[0]
stock_id = '45768538238520265'
get_stock_detail(stock_id)
get_stock_price_history(stock_id)

# %%

print("Getting New ids done", len(Id))
all_stock_data = []
error = []
for i, id in enumerate(Id):
    print(
        "Parsed stock count ", i,id
    )  
    try:
        stock = get_stock_detail(id)  
        if stock == False or stock == None or type(stock) is not dict:
            continue
        stock["history"] = get_stock_price_history(id)

        all_stock_data.append(stock)
    except:
        error.append(id)
#%%
DATES = []
JALALI = []
NAMES = []
TITLES = []
STOCK_IDS = []
GROUPS_NAMES = []
GROUP_IDS = []
BASEVOLS = []
MAX = []
MIN = []
CLOSE = []
LAST = []
OPEN = []
VALUE = []
VOLUME = []
QUANTITY = []
stocks = pd.DataFrame()
for item in all_stock_data:
    for history in item["history"]:

        DATES.append(history["date"])
        JALALI.append(history["jalaliDate"])
        NAMES.append(item["name"])
        TITLES.append(item["title"])
        STOCK_IDS.append(item["stock_id"])
        GROUPS_NAMES.append(item["group_name"])
        #         GROUP_IDS.append(item['group_id'])
        BASEVOLS.append(item["baseVol"])
        MAX.append(history["max_price"])
        MIN.append(history["min_price"])
        CLOSE.append(history["close_price"])
        LAST.append(history["last_price"])
        OPEN.append(history["open_price"])
        VALUE.append(history["value"])
        VOLUME.append(history["volume"])
        QUANTITY.append(history["quantity"])


stocks["jalaliDate"] = JALALI
stocks["date"] = DATES
stocks["name"] = NAMES
stocks["title"] = TITLES
stocks["stock_id"] = STOCK_IDS
stocks["group_name"] = GROUPS_NAMES
# stocks['group_id'] = GROUP_IDS
stocks["baseVol"] = BASEVOLS
stocks["max_price"] = MAX
stocks["min_price"] = MIN
stocks["close_price"] = CLOSE
stocks["last_price"] = LAST
stocks["open_price"] = OPEN
stocks["value"] = VALUE
stocks["volume"] = VOLUME
stocks["quantity"] = QUANTITY


# %%
stocks = stocks.drop_duplicates()


# %%
path = r"E:\RA_Aghajanzadeh\Data\\"
# path = r"H:\Economics\Finance(Prof.Heidari-Aghajanzadeh)\Data\\"

# %%
now = datetime.date.today()
jalali = JalaliDate.to_jalali(now.year, now.month, now.day)
stocks.to_csv(path + "Stocks_Prices_%s.csv" % jalali)


# %%


# %%
now = datetime.datetime.now()
jalali = JalaliDate.to_jalali(now.year, now.month, now.day)
df1 = pd.read_csv("Stocks_Prices_%s.csv" % jalali)
df1.tail(1)


# %%
