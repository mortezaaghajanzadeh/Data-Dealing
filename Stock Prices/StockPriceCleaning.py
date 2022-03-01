#%%
import pandas as pd
import numpy as np
import re
import statsmodels.api as sm

# import finance_byu.rolling as rolling
import requests
import pandas as pd
from bs4 import BeautifulSoup
import math

#%%
def convert_ar_characters(input_str):

    mapping = {
        "ك": "ک",
        "گ": "گ",
        "دِ": "د",
        "بِ": "ب",
        "زِ": "ز",
        "ذِ": "ذ",
        "شِ": "ش",
        "سِ": "س",
        "ى": "ی",
        "ي": "ی",
    }
    return _multiple_replace(mapping, input_str)


def _multiple_replace(mapping, text):
    pattern = "|".join(map(re.escape, mapping.keys()))
    return re.sub(pattern, lambda m: mapping[m.group()], str(text))


def vv(row):
    X = row.split("-")
    return int(X[0] + X[1] + X[2])


def vv2(row):
    X = row.split("/")
    return int(X[0] + X[1] + X[2])


def addDash(row):
    row = str(row)
    X = [1, 1, 1]
    X[0] = row[0:4]
    X[1] = row[4:6]
    X[2] = row[6:8]
    return X[0] + "-" + X[1] + "-" + X[2]


def removeSlash(row):
    X = row.split("/")
    if len(X[1]) < 2:
        X[1] = "0" + X[1]
    if len(X[2]) < 2:
        X[2] = "0" + X[2]
    return int(X[0] + X[1] + X[2])


def removeSlash2(row):
    X = row.split("/")
    if len(X[1]) < 2:
        X[1] = "0" + X[1]
    if len(X[0]) < 2:
        X[0] = "0" + X[0]

    return int(X[2] + X[0] + X[1])


def removeDash(row):
    X = row.split("-")
    if len(X[1]) < 2:
        X[1] = "0" + X[1]
    if len(X[2]) < 2:
        X[2] = "0" + X[2]
    return int(X[0] + X[1] + X[2])


path = r"E:\RA_Aghajanzadeh\Data\\"
path = r"G:\Economics\Finance(Prof.Heidari-Aghajanzadeh)\Data\\"
#%%
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
        columns={"گروه های صنعت": "group_name", "کد گروه های صنعت": "group_id"}
    )
    return df


groupnameid = group_id()
groupnameid["group_id"] = groupnameid.group_id.apply(lambda x: x.strip())
#%%
def Overall_index():
    url = (
        r"http://www.tsetmc.com/tsev2/chart/data/Index.aspx?i=32097828799138957&t=value"
    )
    r = requests.get(url)
    jalaliDate = []
    Value = []
    for i in r.text.split(";"):
        x = i.split(",")
        jalaliDate.append(x[0])
        Value.append(float(x[1]))
    df = pd.DataFrame(
        {
            "jalaliDate": jalaliDate,
            "Value": Value,
        },
        columns=["jalaliDate", "Value"],
    )
    return df


# overal_index = Overall_index()

# %%
pdf = pd.read_parquet(path + "Stock_Prices_1400_11_28.parquet")
print(len(pdf))


col = "group_name"
pdf[col] = pdf[col].apply(lambda x: convert_ar_characters(x))
groupnameid[col] = groupnameid[col].apply(lambda x: convert_ar_characters(x))
pdf[col] = pdf.group_name.str.replace("',CgrValCot='ET", "", regex=False)
pdf[col] = pdf.group_name.str.replace("',CgrValCot='QA", "", regex=False)
pdf[col] = pdf.group_name.str.replace("',CgrValCot='EQ", "", regex=False)
mapdict = dict(zip(groupnameid.group_name, groupnameid.group_id))
pdf["group_id"] = pdf.group_name.map(mapdict)
#%%
pdf.loc[pdf.name.str[-1] == " ", "name"] = pdf.loc[pdf.name.str[-1] == " "].name.str[
    :-1
]
pdf.loc[pdf.name.str[0] == " ", "name"] = pdf.loc[pdf.name.str[0] == " "].name.str[1:]
pdf["name"] = pdf["name"].apply(lambda x: convert_ar_characters(x))
pdf.jalaliDate = pdf.jalaliDate.apply(vv)
pdf = pdf.sort_values(by=["name", "date"])
#%%
# pdf = pdf[
#     ~((pdf.title.str.startswith("ح")) & (pdf.name.str.endswith("ح")))
# ]  # delete right offers
pdf = pdf[~(pdf.name.str.endswith("پذيره"))]  # delete subscribed symbols
col = "name"
pdf[col] = pdf[col].apply(lambda x: convert_ar_characters(x))
#%%
for i in [
    "max_price_Adjusted",
    "min_price_Adjusted",
    "open_price_Adjusted",
    "last_price_Adjusted",
    "close_price_Adjusted",
]:
    print(i)
    pdf[i] = pdf[i].fillna(value=np.nan)
    pdf[i] = pdf.groupby("name")[i].fillna(method="ffill")
    pdf[i] = pdf.groupby("name")[i].fillna(method="bfill")

list(pdf[pdf.group_id.isnull()]["group_name"].unique())

#%%
symbolGroup = pdf[["name", "group_name", "group_id"]].drop_duplicates(
    subset=["name", "group_name", "group_id"]
)

symbolGroup.to_excel(path + "SymbolGroup.xlsx", index=False)
#%%
shrout = pd.read_csv(path + "Cleaned_Stocks_Holders_1400_10_06.csv")[
    ["name", "date", "shrout"]
]
shrout = shrout.drop_duplicates(subset=["name", "date"])
shrout.to_csv(path + "SymbolShrout_1400_10_06.csv")
mapdict = dict(zip(shrout.set_index(["name", "date"]).index, shrout.shrout))
i = "date"
pdf[i] = pdf[i].astype(int)

pdf["shrout"] = pdf.set_index(["name", "date"]).index.map(mapdict)
i = "volume"
pdf[i] = pdf[i].astype(float)
d = pd.DataFrame()
d = d.append(pdf)
gg = d.groupby("name")
d["shrout"] = gg["shrout"].fillna(method="bfill")
d["shrout"] = gg["shrout"].fillna(method="ffill")
#%%
pdf = pd.DataFrame()
pdf = pdf.append(d)

i = "group_id"
pdf[i] = pdf[i].astype(float)
i = "close_price"
pdf[i] = pdf[i].astype(float)
i = "quantity"
pdf[i] = pdf[i].astype(float)
i = "close_price_Adjusted"
pdf[i] = pdf[i].astype(float)


gg = pdf.groupby(["name"])
for i in range(-5, 6):
    pdf["Volume({})".format(-i)] = gg.volume.shift(i)
    pdf["price({})".format(-i)] = gg.close_price.shift(i)

for i in ["last_price", "open_price", "value", "quantity", "volume"]:
    print(i)
    pdf.loc[(pdf["Volume(-1)"] == 0) & (pdf["Volume(1)"] == 0), i] = 0
    pdf.loc[(pdf["Volume(-1)"] == 0) & (pdf["Volume(1)"] == 0), i] = 0
    pdf.loc[(pdf["Volume(-2)"] == 0) & (pdf["Volume(1)"] == 0), i] = 0
    pdf.loc[(pdf["Volume(-1)"] == 0) & (pdf["Volume(2)"] == 0), i] = 0
    pdf.loc[(pdf["Volume(-2)"] == 0) & (pdf["Volume(2)"] == 0), i] = 0
    pdf.loc[(pdf["Volume(-3)"] == 0) & (pdf["Volume(1)"] == 0), i] = 0
    pdf.loc[(pdf["Volume(-1)"] == 0) & (pdf["Volume(3)"] == 0), i] = 0
    pdf.loc[(pdf["Volume(-1)"] == 0) & (pdf["Volume(4)"] == 0), i] = 0
    pdf.loc[(pdf["Volume(-2)"] == 0) & (pdf["Volume(4)"] == 0), i] = 0
    pdf.loc[(pdf["Volume(-4)"] == 0) & (pdf["Volume(1)"] == 0), i] = 0
    pdf.loc[(pdf["Volume(-4)"] == 0) & (pdf["Volume(2)"] == 0), i] = 0


# pList = [1.0, 1000.0, 100.0, 10.0]
# pdf = pdf[~((pdf.close_price.isin(pList)) & (pdf.volume == 0))]

# pdf["close"] = pdf.close_price / pdf.AdjustFactor
gg = pdf.groupby(["name"])
pdf["return"] = gg.close_price_Adjusted.pct_change() * 100
pdf["MarketCap"] = pdf.close_price * pdf.shrout
pdf["yclose"] = gg.close_price_Adjusted.shift()


i = "return"
pdf.loc[(pdf["Volume(-1)"] == 0) & (pdf["Volume(1)"] == 0), i] = 0
pdf.loc[(pdf["Volume(-1)"] == 0) & (pdf["Volume(1)"] == 0), i] = 0
pdf.loc[(pdf["Volume(-2)"] == 0) & (pdf["Volume(1)"] == 0), i] = 0
pdf.loc[(pdf["Volume(-1)"] == 0) & (pdf["Volume(2)"] == 0), i] = 0
pdf.loc[(pdf["Volume(-2)"] == 0) & (pdf["Volume(2)"] == 0), i] = 0
pdf.loc[(pdf["Volume(-3)"] == 0) & (pdf["Volume(1)"] == 0), i] = 0
pdf.loc[(pdf["Volume(-1)"] == 0) & (pdf["Volume(3)"] == 0), i] = 0
pdf.loc[(pdf["Volume(-1)"] == 0) & (pdf["Volume(4)"] == 0), i] = 0
pdf.loc[(pdf["Volume(-2)"] == 0) & (pdf["Volume(4)"] == 0), i] = 0
pdf.loc[(pdf["Volume(-4)"] == 0) & (pdf["Volume(1)"] == 0), i] = 0
pdf.loc[(pdf["Volume(-4)"] == 0) & (pdf["Volume(2)"] == 0), i] = 0
# pdf.loc[(pdf["Volume(-1)"] == 0) & (pdf["price(-1)"].isin(pList)), i] = 0


gg = pdf.groupby(["name"])
for i in [
    "max_price_Adjusted",
    "min_price_Adjusted",
    "open_price_Adjusted",
    "last_price_Adjusted",
    "close_price_Adjusted",
    "close_price",
    "last_price",
    "open_price",
    "yesterday_price",
]:
    print(i)
    pdf.loc[(pdf["Volume(-1)"] == 0) & (pdf["Volume(1)"] == 0), i] = np.nan
    pdf.loc[(pdf["Volume(-1)"] == 0) & (pdf["Volume(1)"] == 0), i] = np.nan
    pdf.loc[(pdf["Volume(-2)"] == 0) & (pdf["Volume(1)"] == 0), i] = np.nan
    pdf.loc[(pdf["Volume(-1)"] == 0) & (pdf["Volume(2)"] == 0), i] = np.nan
    pdf.loc[(pdf["Volume(-2)"] == 0) & (pdf["Volume(2)"] == 0), i] = np.nan
    pdf.loc[(pdf["Volume(-3)"] == 0) & (pdf["Volume(1)"] == 0), i] = np.nan
    pdf.loc[(pdf["Volume(-1)"] == 0) & (pdf["Volume(3)"] == 0), i] = np.nan
    pdf.loc[(pdf["Volume(-1)"] == 0) & (pdf["Volume(4)"] == 0), i] = np.nan
    pdf.loc[(pdf["Volume(-2)"] == 0) & (pdf["Volume(4)"] == 0), i] = np.nan
    pdf.loc[(pdf["Volume(-4)"] == 0) & (pdf["Volume(1)"] == 0), i] = np.nan
    pdf.loc[(pdf["Volume(-4)"] == 0) & (pdf["Volume(2)"] == 0), i] = np.nan
    # pdf.loc[(pdf["Volume(-1)"] == 0) & (pdf["price(-1)"].isin(pList)), i] = np.nan

pdf = pdf.drop(
    columns=[
        "Volume(5)",
        "price(5)",
        "Volume(4)",
        "price(4)",
        "Volume(3)",
        "price(3)",
        "Volume(2)",
        "price(2)",
        "Volume(1)",
        "price(1)",
        "Volume(0)",
        "price(0)",
        "Volume(-1)",
        "price(-1)",
        "Volume(-2)",
        "price(-2)",
        "Volume(-3)",
        "price(-3)",
        "Volume(-4)",
        "price(-4)",
        "Volume(-5)",
        "yclose",
        "price(-5)",
        "yclose",
    ]
)
pdf.describe()
# %%
for i in [
    "max_price",
    "min_price",
    "close_price",
    "last_price",
    "open_price",
    "value",
    "volume",
    "quantity",
    "max_price_Adjusted",
    "min_price_Adjusted",
    "open_price_Adjusted",
    "last_price_Adjusted",
    "close_price_Adjusted",
]:
    pdf[i] = pdf[i].astype(float)
# %%
pdf[(pdf.name == "ومشان") & (pdf.jalaliDate > 13980104)][
    ["jalaliDate", "close_price", "close_price_Adjusted"]
].head(20)

#%%
gg = pdf.groupby(["name"])
for i in [
    "max_price",
    "min_price",
    "close_price",
    "last_price",
    "open_price",
    "max_price_Adjusted",
    "min_price_Adjusted",
    "open_price_Adjusted",
    "last_price_Adjusted",
    "close_price_Adjusted",
]:
    print(i)
    pdf[i] = gg[i].fillna(method="bfill")

#%%
pdf[(pdf.name == "ومشان") & (pdf.jalaliDate > 13980104)][
    ["jalaliDate", "close_price", "close_price_Adjusted"]
].head(20)
#%%
list(pdf[pdf.close_price.isnull()][["name", "return", "date"]].name.unique())
#%%
list(pdf[pdf.close_price_Adjusted.isnull()][["name", "return", "date"]].name.unique())
#%%
pdf = pdf[~pdf.close_price.isnull()]
pdf = pdf[~pdf.close_price_Adjusted.isnull()]
#%%
pdf[pdf.shrout.isnull()][["name", "return", "date"]].name.unique()
# %%
pdf.to_parquet(
    path
    + "Cleaned_Stock_Prices_{}".format(
        pdf[pdf.date == pdf.date.max()].jalaliDate.iloc[0]
    )
    + ".parquet"
)
# %%
pdf[pdf.name == "غگز"]
