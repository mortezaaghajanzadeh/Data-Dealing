#%%
import re
import numpy as np
from persiantools.jdatetime import JalaliDate
import pandas as pd
from persiantools.jdatetime import JalaliDate

path2 = r"E:\RA_Aghajanzadeh\Data\\"
path = r"E:\RA_Aghajanzadeh\Data\Stock_holder_new\\"
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


#%%
pdf = pd.read_parquet(path2 + "Cleaned_Stock_Prices_1400_06_29.parquet")
print("read Price")
pdf = pdf.drop(
    columns=[
        "stock_id",
        "title",
        "baseVol",
        "max_price",
        "min_price",
        "last_price",
        "open_price",
        "value",
        "quantity",
    ]
)
# pdf = pdf[pdf["date"] >= 20150324]

df1 = pd.read_parquet(path + "mergerdallData_cleaned" + ".parquet")
print("read Mereged Data")
df1 = df1.drop(df1[df1["name"] == "کرد"].index)
mlist = [
    "jalaliDate",
    "date",
    "Holder",
    "Holder_id",
    "Number",
    "Percent",
    "Change",
    "ChangeAmount",
    "Firm",
    "name",
    "shrout",
    "stock_id",
    "close_price",
]
df1 = df1[df1.Holder != "-"]
df1["Number"] = df1.Number.astype(float)
df1["Percent"] = df1.Percent.astype(float)
df1["Change"] = df1.Change.astype(float)
df1 = df1[mlist]
print("Rename")

pdf["name"] = pdf["name"].apply(lambda x: convert_ar_characters(x))
df1["name"] = df1["name"].apply(lambda x: convert_ar_characters(x))
#%%

gdata = pdf[["group_name", "name"]].drop_duplicates().dropna()
mapingdict = dict(zip(gdata.name, gdata.group_name))
df1["group_name"] = df1["name"].map(mapingdict)
gdata = pdf[["group_id", "group_name"]].dropna().drop_duplicates()
mapingdict = dict(zip(gdata.group_name, gdata.group_id))
df1["group_id"] = df1["group_name"].map(mapingdict)
df1 = df1.dropna()
#%%
df2 = df1[["Holder_id", "Holder", "date"]]
df2 = df2.sort_values(by=["Holder_id"])
Holders = df2.drop_duplicates(
    subset="Holder_id", keep="last", inplace=False
).sort_values(by=["Holder_id"])
try:
    df1 = df1.drop(columns=["Unnamed: 0", "Unnamed: 0.1"])
except:
    try:
        df1 = df1.drop(columns=["Unnamed: 0"])
    except:
        print("No")

# %%
df3 = pd.read_excel(path + "shareholder_names_cleaned_9901_v6.xlsx")
for i in ["shareholder_cleaned", "shareholder_raw"]:
    print(i)
    df3[i] = df3[i].apply(lambda x: convert_ar_characters(x))
print("Holder")
df1["Holder"] = df1["Holder"].apply(lambda x: convert_ar_characters(x))


#%%
df1.loc[df1["Holder_id"] == 53741, "Holder"] = "سرمایه گذاری تدبیر"


indid = [
    62690,
    51770,
    51817,
    62446,
    62465,
    62608,
    62640,
    53836,
    57041,
    63264,
    65212,
    65212,
    53508,
    65197,
    65197,
    65030,
    65030,
    63264,
    62671,
    63117,
    63308,
    63110,
    63316,
]
df1.loc[df1["Holder_id"].isin(indid), "Holder"] = "اشخاص حقیقی"
df1.loc[df1.Holder == "شخص حقيقي", "Holder"] = "اشخاص حقیقی"

#%%

mapingdict = dict(zip(df3["shareholder_raw"], df3["shareholder_cleaned"]))
df1["Holder"] = df1["Holder"].map(mapingdict)

mapingdict = dict(zip(df3["shareholder_cleaned"], df3["type"]))
df1["type"] = df1["Holder"].map(mapingdict)

#%%

df1["type"] = df1["type"].fillna("None")
df1["type"] = df1["type"].replace(" fund", "fund")
df1.loc[
    df1["Holder_id"] == 56965, "Holder"
] = "PRXسبد-شرک43268--موس29115-م.صندوق ت.ف نوین"
df1.loc[df1["Holder_id"] == 63323, "Holder"] = "تجارت و اسکان احیا سپاهان"
df1.loc[df1["Holder_id"] == 63087, "Holder"] = "مدیریت ثروت پایا"

df1.loc[
    df1["Holder_id"] == 60374, "Holder"
] = "BFMصندوق سرمایه گذاری.ا.ب.پاداش پشتیبان پارس"
df1.loc[df1["Holder_id"] == 62744, "Holder"] = "پدیده تاپان سرآمد"
#%%
dropholders = [
    "سایر سهامدارن",
    "اعضا هیئت مدیره",
    "اشخاص حقیقی",
    "اشخاص حقوقی",
    "سهام حقوقی",
    "سهام حقیقی",
    "سهام کارکنان",
    "سهام کارگری",
    "سهام مسدود",
    "سهام وثیقه",
    "شرکت های گروه",
    "شهرداری ها",
    "کارکنان",
    "کارگران",
    "کارگزاران",
    "مدیران شرکت",
    "هیئت مدیره",
    "کد رزرو صندوقهای سرمایه گذاری قابل معامله",
    "کد رزرو صندوق های سرمایه گذاری قابل معامله",
    "کدواسط دستورالعمل اجرایی",
    "سلب حق تقدم",
]
df1 = df1.drop(df1.loc[df1["Holder"].isin(dropholders)].index)
df1.isnull().sum()
#%%
ids = df1[df1["Holder"].isnull()]["Holder_id"].tolist()
Holders[Holders["Holder_id"].isin(ids)]
# %%
Holders[Holders["Holder_id"].isin(ids)].to_excel(path + "NewHolder.xlsx")
# %%
df1 = df1.drop_duplicates(keep="first")
df1 = df1.drop_duplicates(
    keep="first", subset=["name", "date", "Holder", "Number"]
).rename(columns={"shrout": "Total"})
df1 = (
    df1.groupby(
        [
            "stock_id",
            "Total",
            "name",
            "date",
            "close_price",
            "jalaliDate",
            "group_name",
            "group_id",
            "Holder",
            "type",
        ]
    )
    .agg({"Number": sum, "Percent": sum})
    .reset_index()
)
df1 = df1[
    [
        "date",
        "name",
        "stock_id",
        "jalaliDate",
        "group_name",
        "group_id",
        "Holder",
        "Number",
        "type",
        "Percent",
        "Total",
        "close_price",
    ]
]
df1.head()

# %%
def sumPercent(df):
    gg = df.groupby(["date", "name"])
    return gg.Percent.sum()


a = sumPercent(df1)
a[a > 100]
#%%
def Cleaning(g, ff, a, g_keys):
    i = g.name
    for id, value in enumerate(g_keys):
        if value == i:
            print("Group " + str(id))

    tempt = ff[ff["name"] == i[0]]
    notradedays = tempt.loc[tempt["volume"] == 0]["date"].tolist()

    gg = pd.merge(left=a, right=g, how="left", left_on="date", right_on="date")

    if len(gg) == 0:
        return

    ## Filling the Gaps
    if len(g) > 3:

        v1 = gg["stock_id"][~gg["stock_id"].isna()].index[-1]
        v2 = gg["stock_id"][~gg["stock_id"].isna()].index[0]
        gg = gg[(gg.index <= v1) & (gg.index >= v2)]
        gg = gg.reset_index(drop=True)
        gg["Condition"] = "Orginal"
        gg = FillGaps(gg)

    ### Flatting Data
    # gg = FlatFunction(gg)

    ##Flaging

    mapingdict = dict(zip(list(tempt["date"]), list(tempt["close_price"])))
    gg["close_price"] = gg["date"].map(mapingdict)
    gg = gg.fillna(method="ffill")

    d2 = gg["Number"].diff()
    d3 = gg["Percent"].diff()
    d2.iloc[0] = "-"
    d3.iloc[0] = "-"
    gg["Number_Change"] = d2
    gg["Percent_Change"] = d3

    gg["Trade"] = "Yes"
    gg.loc[(gg["date"].isin(notradedays)), "Trade"] = "No"

    return gg


def FlatFunction(gg):

    cg = gg[(gg.Percent_Change != 0) & (~gg.Percent_Change.isnull())]

    cindex = cg.index
    gindex = gg.index

    for index in cindex:
        i = 1
        flatted = 0
        while i < 6 and flatted == 0:
            ids = list(range(index, index + i))
            dg = gg[gg.index.isin(ids)]
            i += 1
            if abs(dg.Percent_Change.sum()) <= 0.00001:
                pg = gg.loc[gg.index == index - 1]
                if len(pg) < 1:
                    continue
                dgindex = dg.index
                for dinex in dgindex:
                    gg.loc[gg.index == dinex, "Number"] = pg["Number"].iloc[0]
                    gg.loc[gg.index == dinex, "Percent"] = pg["Percent"].iloc[0]
                    gg.loc[gg.index == dinex, "Percent_Change"] = 0
                    gg.loc[gg.index == dinex, "Condition"] = "Flatted"

                flatted = 1
                a = set(cindex)
                b = set(ids)
                cindex = list(a.difference(b))
    return gg


def FillGaps(gg):
    ChangeList = [
        "name",
        "stock_id",
        "group_name",
        "group_id",
        "Holder",
        "Number",
        "type",
        "Percent",
        "Total",
        "close_price",
    ]
    nanid = list(gg[gg.name.isnull()].index)
    for value in nanid:
        NextValue = gg[(~gg.name.isnull()) & (gg.index > value)].index[0]
        if NextValue - value <= 4:
            for i in ChangeList:
                gg.loc[gg.index == value, i] = gg.loc[gg.index == NextValue][i].iloc[0]
        gg.loc[gg.index == value, "Condition"] = "Filled"
    gg = gg[~(gg.name.isnull())]
    return gg


df = df1.reset_index(drop=True)
import requests
from bs4 import BeautifulSoup


def removeSlash2(row):
    X = row.split("/")
    if len(X[1]) < 2:
        X[1] = "0" + X[1]
    if len(X[0]) < 2:
        X[0] = "0" + X[0]

    return int(X[2] + X[0] + X[1])


def Overall_index():
    url = r"https://tse.ir/archive/Indices/Main/Indices_IRX6XTPI0006.xls"
    r = requests.get(url)  # This URL contains all sector groups.
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
    df["Date"] = df.Date.apply(removeSlash2)
    return df


overal_index = Overall_index()
overal_index

grouped_data = df1.groupby(["name", "Holder"])  # ,'type'])
g_keys = list(grouped_data.groups.keys())

ff = pdf
index = overal_index
a = index.drop(columns=["Index"]).rename(columns={"Date": "date"})

# closedays = [13960923, 13960924, 13970504, 13970505]
# a = a.drop(a.loc[a["jalaliDate"].isin(closedays)].index)

new_row = {"date": 20171106}
a = a.append(new_row, ignore_index=True).sort_values(by=["date"])
data = pd.DataFrame()
print(len((g_keys)))
