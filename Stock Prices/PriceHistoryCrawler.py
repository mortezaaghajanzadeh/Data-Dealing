#%%
import pandas as pd
import requests
from persiantools.jdatetime import JalaliDate
import re
import logging
from threading import Thread

# %%
def get_stock_detail(stock_id):
    try:
        # If you replace {} with a stock ID in this URL, you can access that stocks page in tsetmc.com.
        logging.captureWarnings(True)
        url = "http://www.tsetmc.com/Loader.aspx?ParTree=151311&i={}".format(stock_id)
        r = requests.get(url, timeout=1, verify=False)
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


def clean_adjusted_price(r):
    if r.text == "":
        return []
    history = []
    for item in r.text.split(";"):
        item_list = item.split(",")
        history_item = {}
        history_item["date"] = item_list[0]
        history_item["jalaliDate"] = JalaliDate.to_jalali(
            int(item_list[0][0:4]), int(item_list[0][4:6]), int(item_list[0][6:8])
        ).isoformat()
        history_item["max_price"] = item_list[1]
        history_item["min_price"] = item_list[2]
        history_item["open_price"] = item_list[3]
        history_item["last_price"] = item_list[4]
        history_item["volume"] = item_list[5]
        history_item["close_price"] = item_list[6]
        history.append(history_item)
    return history


def clean_unadjusted_price(r):
    price_history_list = r.text.split(";")
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
    return history


def crawl_prices(id):
    url = r"https://members.tsetmc.com/tsev2/chart/data/Financial.aspx?i={}&t=ph&a={}"
    url1 = url.format(id, 1)  # Adjusted prices
    r1 = requests.get(url1, timeout=10, verify=False)
    t1 = clean_adjusted_price(r1)
    url = r"https://members.tsetmc.com/tsev2/chart/data/Financial.aspx?i={}&t=ph&a={}"
    url1 = url.format(id, 0)  # UnAdjusted prices
    r = requests.get(url1.format(id), timeout=10, verify=False)
    t0 = clean_adjusted_price(r)

    if t1 == []:
        df = pd.DataFrame.from_dict(t0)
    else:
        df = pd.DataFrame.from_dict(t0).merge(
            pd.DataFrame.from_dict(t1),
            on=["date", "jalaliDate", "volume"],
            how="left",
            suffixes=("", "_Adjusted"),
        )
    stock_detail = get_stock_detail(id)
    if type(stock_detail) != type({}):
        return pd.DataFrame()
    l = ["sectorPe", "shareCount", "estimatedEps", "insCode"]
    for i in stock_detail:
        if i in l:
            continue
        df[i] = stock_detail[i]

    return df


object = pd.read_pickle(r"Get all ids\ids.p")
Id = []
for i in object:
    for j in i:
        Id.append(j)
Id = list(set(Id))

id = Id[1]
id = 2400322364771558
df = crawl_prices(id)
df


#%%
def excepthook(args):
    3 == 1 + 2

import threading
threading.excepthook = excepthook
Data = pd.DataFrame()
counter = 0
error = []
threads = {}
result = [None]* len(Id)


def gen_price(result, stock_id, error,i):
    try:
        result[i] = crawl_prices(stock_id)
        return result,error
    except:
        error.append(stock_id)
        return result,error

import time
for  i,stock_id in enumerate(Id[:]):
    counter += 1
    print(counter, len(Id), stock_id,i)
    threads[i] = Thread(
        target=gen_price, args=(result, stock_id, error,i)
        )
    threads[i].start()
    time.sleep(0.5)
for i in threads:
    threads[i].join()
#%%

for i in result:
    if i  is not  None:
        Data = Data.append(i)
        print(len(Data))


#%%
# path = r"E:\RA_Aghajanzadeh\Data\\"
# name = "Stock_Prices_1400_09_10"
# Data.to_parquet(path + name + ".parquet")


# #%%
# Data.to_parquet(r"E:\RA_Aghajanzadeh\Data\Stock_Prices_1400_06_16.parquet")
