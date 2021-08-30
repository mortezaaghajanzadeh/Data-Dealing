#%%
import requests
import pandas as pd
from bs4 import BeautifulSoup
URL = r"http://www.tsetmc.com/Loader.aspx?Partree=15131G&i={}"



def removeSlash(row):
    X = row.split("/")
    if len(X[1]) < 2:
        X[1] = "0" + X[1]
    if len(X[2]) < 2:
        X[2] = "0" + X[2]
    return int(X[0] + X[1] + X[2])


def replcoma(s):
    if s is None:
        return ""
    x = s.split(",")
    if len(x) > 1:
        return x[0] + x[1]
    else:
        return x[0]

object = pd.read_pickle(r'D:\Dropbox\Python Codes\Soltan\Data generating\ids.p')
#%%
def read(id):
    r = requests.get(URL.format(id),timeout=15)
    soup = BeautifulSoup(r.text, "html.parser")
    header = soup.find_all("table")[0].find("tr")
    list_header = []
    for items in header:
        try:
            list_header.append(items.get_text())
        except:
            continue

    # for getting the data
    HTML_data = soup.find_all("table")[0].find_all("tr")[1:]
    data = []
    for element in HTML_data:
        sub_data = []
        for sub_element in element:
            try:
                sub_data.append(sub_element.get_text())
            except:
                continue
        data.append(sub_data)
    df = pd.DataFrame(data=data, columns=list_header)
    i = ['تاریخ', 'تعدیل شده', 'قبل از تعدیل']
    df[i[0]] = df[i[0]].apply(removeSlash)
    df[i[1]] = df[i[1]].apply(replcoma)
    df[i[2]] = df[i[2]].apply(replcoma)
    df['stock_id'] = id
    return df
data = pd.DataFrame()
for number,i in enumerate(object):
    try:
        data = data.append(read(i))
        print(number)
    except:
        pass
# %%

# %%
