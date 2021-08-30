#%%
import pandas as pd
import requests
from bs4 import BeautifulSoup


def group_id():
    r = requests.get(
        "http://www.tsetmc.com/Loader.aspx?ParTree=111C1213"
    )  # This URL contains all sector groups.
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
    df = pd.DataFrame(data=data, columns=list_header).rename(
        columns = {
            'گروه های صنعت':'group_name',
            "کد گروه های صنعت":"group_id"
        }
    )
    return df                  
def groupindexes(id):
    print(id)
    url = "https://tse.ir/archive/Indices/Industry/Indices_IRX6X{}T0006.xls"
    r = requests.get(url.format(id[:2]), allow_redirects=True)
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
    return df

#%%
groupnameid = group_id()
groupnameid = groupnameid[groupnameid.group_name  != 'شاخص']

data = pd.DataFrame()
error = []
for i in groupnameid.group_id.to_list():
    try:
        t = groupindexes(i)
        data = data.append(t)
    except:
        print("Group {} errored.".format(i))
        error.append(i)
    
# %%

path = r"G:\Economics\Finance(Prof.Heidari-Aghajanzadeh)\Data\Capital Rise\\"

data.to_csv(path + "indexes_1400-06-01.csv",index = False)

# %%
