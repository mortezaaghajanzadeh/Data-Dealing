#%%
from pandas.tseries.offsets import Micro
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import pandas as pd
import json
import pickle
from bs4 import BeautifulSoup

path = r"C:\Program Files (x86)\chromedriver.exe"
from selenium import webdriver
import requests
import re

#%%
driver = webdriver.Chrome(path)
driver.get("http://www.tsetmc.com/Loader.aspx?ParTree=15131Fe")
print(driver.title)
driver.find_element_by_id("mwp").click()
driver.find_element_by_id("id1").click()
#%%
# Set the setting for crawling firms Id
# %%
elements = driver.find_elements_by_class_name("\{c\}")
ids = []
for element in elements:
    ids.append(element.get_attribute("id"))

pickle.dump(ids, open("ids_Current.p", "wb"))
# %%
def get_stock_groups():
    r = requests.get(
        "http://www.tsetmc.com/Loader.aspx?ParTree=111C1213"
    )  # This URL contains all sector groups.
    groups = re.findall(r"\d{2}", r.text)  # Finding sector IDs in text of this page.
    return groups


def get_stock_ids(group):
    # If you replace {} with sector ID, you get stock all IDs of that sector.
    url = "http://www.tsetmc.com/tsev2/data/InstValue.aspx?g={}&t=g&s=0"
    r = requests.get(url.format(group))
    ids = set(re.findall(r"\d{15,20}", r.text))
    return list(ids)


ids2 = []
for group in get_stock_groups():
    print(group)
    ids2.append(get_stock_ids(group))
for i in ids2:
    for j in i:
        ids.append(j)
#%%
url = "http://www.tsetmc.com/Loader.aspx?ParTree=111C1417"
r = requests.get(url)
nids = set(re.findall(r"\d{15,20}", r.text))


#%%
print(len(ids))
ids = set(ids)
ids.update(nids)
ids = list(ids)
print(len(ids))


pickle.dump(ids, open("ids_Current_added_group.p", "wb"))
#%%
driver = webdriver.Chrome(path)
driver.get("https://tse.ir/listing.html?cat=cash&section=alphabet")
print(driver.title)
chars = [
    "ا",
    "ب",
    "پ",
    "ت",
    "ث",
    "ج",
    "چ",
    "ح",
    "خ",
    "د",
    "ذ",
    "ر",
    "ز",
    "ژ",
    "س",
    "ش",
    "ص",
    "ض",
    "ط",
    "ظ",
    "ع",
    "غ",
    "ف",
    "ق",
    "ك",
    "گ",
    "ل",
    "م",
    "ن",
    "و",
    "ه",
    "ی",
    "ك",
    "گ",
    "دِ",
    "بِ",
    "زِ",
    "ذِ",
    "شِ",
    "سِ",
    "ى",
    "ي",
]
names = []
for i in chars:
    try:
        lis = driver.find_element_by_id(
            "c_table_{}".format(i)
        ).find_elements_by_class_name("status_A")
        for report in lis:
            url = report.text.split("1")
            print(url[0])
            names.append(url[0])
    except:
        continue
len(names)
# %%
link = "https://www.ifb.ir/Issuers.aspx?id=1"
driver.get(link)
r = requests.get(link)
print(driver.title)


def ge_names(url):
    r = url.find_element_by_class_name("tblGrp").get_attribute("innerHTML")
    return get_df(BeautifulSoup(r, "html.parser"))


def get_df(soup):
    rows = []
    table_rows = soup.find_all("tr")
    th = table_rows[0].find_all("th")
    columns = [i.text.replace("\n", "").strip() for i in th]
    for tr in table_rows[1:]:
        td = tr.find_all("td")
        row = [i.text.replace("\n", "").strip() for i in td]
        rows.append(row)
    return pd.DataFrame(rows, columns=columns)


df = pd.DataFrame()
for url in driver.find_elements_by_class_name("tab-content"):
    df = df.append(ge_names(url))
for i in df["نماد"].to_list():
    names.append(i)
len(names)
#%%
names = list(set(names))
#%%
#%%
import finpy_tse as fpy
f_stock_list = fpy.Build_Market_StockList(bourse = True, farabourse = True, payeh = True, detailed_list = True, show_progress = True, 
                                           save_excel = True, save_csv = False, save_path = 'E:\RA_Aghajanzadeh\Data')
for i in list(f_stock_list.index.unique() ):
    names.append(i)
#%%

names = list(set(names))



# %%
char = "پذ"
mixchar = names


def get_id(char):
    ids = []
    try:
        results = requests.get(
            "http://tsetmc.ir/tsev2/data/search.aspx?skey={}".format(char)
        ).text.split(";")
        if len(results) == 0:
            return get_id(char)
        for i in results:
            try:
                ids.append(i.split(",")[2])
            except:
                print("Nothing")
        return ids
    except:
        get_id(char)


get_id(mixchar[1])


#%%
ids2 = []
for i in mixchar[::]:
    print(i)
    ids2.append(get_id(i))
ids[0]

#%%
id = []
for i in ids2:
    for j in i:
        id.append(j)
for j in ids:
    id.append(j)
len(set(id))
# %%
import pickle

pickle.dump(ids, open("ids.p", "wb"))

#%%
driver.get(r"https://www.ifb.ir/DataReporter/MarketMap.aspx")
reports = []
while len(reports) < 500:
    reports = (
        driver.find_element_by_id("datagrid")
        .find_element_by_class_name("tblGrp")
        .find_elements_by_xpath("//a[@target ='_blank']")
    )
for i in reports:
    try:
        new = i.text[:-1]
        print(new)
        names.append(new)
    except:
        continue


#%%
chars = [
    "ا",
    "ب",
    "پ",
    "ت",
    "ث",
    "ج",
    "چ",
    "ح",
    "خ",
    "د",
    "ذ",
    "ر",
    "ز",
    "ژ",
    "س",
    "ش",
    "ص",
    "ض",
    "ط",
    "ظ",
    "ع",
    "غ",
    "ف",
    "ق",
    "ك",
    "گ",
    "ل",
    "م",
    "ن",
    "و",
    "ه",
    "ی",
    "ك",
    "گ",
    "دِ",
    "بِ",
    "زِ",
    "ذِ",
    "شِ",
    "سِ",
    "ى",
    "ي",
]
mixchar = []
for i in chars:
    chars2 = chars
    chars2.remove(i)
    for j in chars2:
        mixchar.append(i + j)
for i in names:
    mixchar.append(i)
ids = []
for i in mixchar[::]:
    print(i)
    ids.append(get_id(i))
#%%
for i in ids:
    for j in i:
        id.append(j)
len(list(set(id)))
# %%
t = pd.read_csv("3_clean.csv")
for i in list(t.c.unique()):
    id.append(j)
len(list(set(id)))

t = pd.read_pickle("ids-all.p")
for i in t:
    for j in i:
        id.append(j)
len(list(set(id)))
pickle.dump(list(set(id)), open("ids_all.p", "wb"))
# %%
# %%
