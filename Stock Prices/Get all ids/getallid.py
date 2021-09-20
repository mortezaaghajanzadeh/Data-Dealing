#%%
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import pandas as pd
import json
from bs4 import BeautifulSoup

path = r"C:\Program Files (x86)\chromedriver.exe"
from selenium import webdriver
import requests


#%%
driver = webdriver.Chrome(path)
driver.get("http://www.tsetmc.com/Loader.aspx?ParTree=15131Fe")
print(driver.title)
driver.find_element_by_id("mwp").click()
driver.find_element_by_id("id1").click()
# %%

elements = driver.find_elements_by_class_name("\{c\}")
ids = []
for element in elements:
    ids.append(element.get_attribute("id"))

# %%
import re
import requests


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

#%%

# import pandas as pd
# df = pd.read_csv("3_clean.csv")
# nids = df.c.to_list()
# print(len(ids))
# ids = set(ids)
# ids.update(nids)
# ids = list(ids)
# print(len(ids))
#%%
import pickle

pickle.dump(ids, open("ids.p", "wb"))
# %%
url1 = r"https://tse.ir/json/Listing/ListingByName1.json"
r = requests.get(url1)
soup = BeautifulSoup(r.text, "html.parser")
# len(soup[0]["companies"])
#### Should clean it
decoded_data = r.text.encode().decode("utf-8-sig")
data = json.loads(decoded_data)
for i in data["companies"]:
    print(i["list"])


# %%
