#%%
import pandas as pd
import requests
from persiantools.jdatetime import JalaliDate
import re

# %%
def get_stock_detail(stock_id):
    try:
        # If you replace {} with a stock ID in this URL, you can access that stocks page in tsetmc.com.
        
        url = "http://www.tsetmc.com/Loader.aspx?ParTree=151311&i={}".format(stock_id)
        r = requests.get(url)
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
def clean_crawled_price(r):
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
        history_item["close"] = item_list[6]
        history.append(history_item)
    return history
def crawl_prices(id):
    url = r"https://members.tsetmc.com/tsev2/chart/data/Financial.aspx?i={}&t=ph&a={}"
    url1 = url.format(id, 1)  # Adjusted prices
    url0 = url.format(id, 0)  # Unadjusted prices
    r0 = requests.get(url0)
    r1 = requests.get(url1)
    t1 = clean_crawled_price(r1)
    t0 = clean_crawled_price(r0)
    if t1 == []:
        df = pd.DataFrame.from_dict(t0)
    else:
        df = pd.DataFrame.from_dict(t0).merge(
        pd.DataFrame.from_dict(t1),
        on=["date", "jalaliDate", "volume"],
        suffixes=("", "_Adjusted"),
    )
    stock_detail = get_stock_detail(id)
    if type(stock_detail) != type({}):
        return pd.DataFrame()
    l = ['sectorPe','shareCount','estimatedEps','insCode']
    for i in stock_detail:
        if i in l:
            continue
        df[i] = stock_detail[i]
        
    return df

id = 60033291068807352
df = crawl_prices(id)
df
#%%

#%%
object = pd.read_pickle(r'ids.p')
Data = pd.DataFrame()
counter = 0
for stock_id in object:
    counter += 1 
    print(counter,len(object))
    Data = Data.append(crawl_prices(stock_id))
#%%
Data