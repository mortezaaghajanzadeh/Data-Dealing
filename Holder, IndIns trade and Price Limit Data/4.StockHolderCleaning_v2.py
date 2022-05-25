#%%
from unittest import result
import pandas as pd
import re
import numpy as np
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
pdf = pd.read_parquet(path2 + "Cleaned_Stock_Prices_14010225.parquet")
print("read Price")
pdf = pdf.drop(
    columns=[
        "stock_id",
        # "title",
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
invalid_names = []
invalid_names = list(
    pdf[
        (
            (((pdf.title.str.startswith("ح")) & (pdf.name.str.endswith("ح"))))
            | (pdf.group_name == "اوراق حق تقدم استفاده از تسهیلات مسکن")
            | (pdf.group_name == "اوراق تامین مالی")
            | (pdf.group_name == "صندوق سرمایه گذاری قابل معامله")
            | (pdf.instId.str.startswith("IRK"))
            | (pdf.title.str.startswith("سپرده"))
            | (pdf.title.str.startswith("ح"))
            | (pdf.title.str.contains("اختيارخ"))
            | (pdf.title.str.contains("اختيارف"))
            | (pdf.title.str.contains("اختيار"))
            | (pdf.title.str.contains("آتي"))
            | (pdf.title.str.contains("عدالت"))
            | (pdf.title.str.contains("صكوك"))
            | (pdf.title.str.contains("مشاركت"))
            | (pdf.title.str.contains("اجاره"))
            | (pdf.title.str.contains("مرابحه"))
            | (pdf.title.str.contains("سلف"))
            | (pdf.name.str.contains("پذیره"))
            | (pdf.name.str.contains("حذف"))
            | (pdf.title.str.contains("حذف"))
            | (pdf.title.str.contains("شركت س استان"))
        )
    ].name.unique()
)

#%%
df1 = pd.read_pickle(path + "mergerdHolderAllData_cleaned.p").replace("-", np.nan)
print("read Mereged Data")

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
df1["Number"] = df1.Number.astype(float)
df1["Percent"] = df1.Percent.astype(float)
df1["Change"] = df1.Change.astype(float)
df1 = df1[mlist]
df1[(df1.name == "فولاد") & (df1.date >= 20170325)].sort_values(by="date").head()
# %%
df1 = df1[~df1.name.isin(invalid_names)]
df1["date"] = df1["date"].astype(str)
a = df1.groupby("date").size().to_frame()
a.plot(y=0, use_index=True)
a[a[0] < 100]
df1["date"] = df1["date"].astype(float)
df1["date"] = df1["date"].astype(int)
# %%
print(len(df1))
df1 = df1[~df1.Holder.isnull()]
df1 = df1[df1.Holder != "nan"]
print(len(df1))
#%%
gdata = pdf[["group_name", "name"]].drop_duplicates().dropna()
mapingdict = dict(zip(gdata.name, gdata.group_name))
df1["group_name"] = df1["name"].map(mapingdict)
gdata = pdf[["group_id", "group_name"]].dropna().drop_duplicates()
mapingdict = dict(zip(gdata.group_name, gdata.group_id))
df1["group_id"] = df1["group_name"].map(mapingdict)
df1 = df1.dropna()
df1[(df1.name == "کماسه") & (df1.date >= 20170325)].sort_values(by="date").head()


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
# df3 = pd.read_excel(path + "shareholder_names_cleaned_9901_v6.xlsx")
df3 = pd.read_excel(path + "shareholder_names_cleaned_140102_v1.xlsx")
for i in ["shareholder_cleaned", "shareholder_raw"]:
    print(i)
    df3[i] = df3[i].apply(lambda x: convert_ar_characters(x))
    df3[i] = df3[i].apply(lambda x: x.strip())
print("Holder")

#%%
tempt_df = pd.DataFrame()
tempt_df = tempt_df.append(df1)
mapingdict = dict(zip(df3["shareholder_raw"], df3["type"]))
tempt_df["type"] = tempt_df["Holder"].map(mapingdict)
mapingdict = dict(zip(df3["shareholder_raw"], df3["shareholder_cleaned"]))
tempt_df["Holder"] = tempt_df["Holder"].map(mapingdict)

# %%
ids = tempt_df[tempt_df["Holder"].isnull()]["Holder_id"].tolist()
Holders[Holders["Holder_id"].isin(ids)]
#%%
Holders[Holders["Holder_id"].isin(ids)].to_excel(path + "NewHolder.xlsx")

# %%
df1 = pd.DataFrame().append(tempt_df)
df1.head()

#%%
df1 = df1.drop_duplicates(keep="first")
df1.head()
#%%
df1 = df1.drop_duplicates(
    keep="first", subset=["name", "date", "Holder_id", "Number"]
).rename(columns={"shrout": "Total"})
#%%
tempt = df1[~df1.Holder.isin(["شخص حقیقی", "اشخاص حقیقی",])][
    ["Holder", "type", "Holder_id"]
].drop_duplicates(keep="last", subset=["Holder", "type"])
tempt["Holder_id"] = tempt.Holder_id.astype(int)

mapingdict = dict(zip(tempt.set_index(["Holder", "type"]).index, tempt["Holder_id"]))
df1["Holder_id2"] = df1.set_index(["Holder", "type"]).index.map(mapingdict)
df1.loc[df1.Holder_id2.isnull(), "Holder_id2"] = df1.loc[
    df1.Holder_id2.isnull()
].Holder_id
df1["Holder_id"] = df1.Holder_id2
df1 = df1.drop(columns=["Holder_id2"])
#%%
df1[df1.Holder == "شستا"].drop_duplicates(subset=["Holder_id"])[
    ["Holder_id", "name", "date", "Holder", "type", "Number", "Total"]
]
#%%
df1 = (
    df1.groupby(
        [
            "stock_id",
            "name",
            "Firm",
            "date",
            "group_name",
            "group_id",
            "jalaliDate",
            "Holder_id",
            "Holder",
            "Total",
            "close_price",
            "type",
        ]
    )
    .agg({"Number": sum, "Percent": sum})
    .reset_index()
)
#%%
df1[(df1.name == "کماسه") & (df1.date >= 20170325)].sort_values(by="date").head()

# %%
df1 = df1[
    [
        "date",
        "name",
        "stock_id",
        "jalaliDate",
        "group_name",
        "group_id",
        "Holder",
        "Holder_id",
        "Number",
        "type",
        "Percent",
        "Total",
        "close_price",
    ]
]
df1.head()
df1[(df1.name == "آ س پ") & (df1.date == 20140511.0)]
#%%
a = df1.groupby("date").size().to_frame().reset_index()
a.plot(y=0, use_index=True)
# %%
def sumPercent(df):
    gg = df.groupby(["date", "name"])
    return gg.Percent.sum()


a = sumPercent(df1)
a[a > 100]

#%%
df1[df1.name == "کفرا"][df1.Holder_id == 99.0][df1.date >= 20081222][
    ["name", "date", "jalaliDate", "Number", "Percent", "Total", "close_price"]
].head(50)
#%%

def Cleaning(gg):  # , g_keys):
    # i = g.name

    ## Filling the Gaps
    if len(gg) > 3:
        v1 = gg["stock_id"][~gg["stock_id"].isna()].index[-1]
        v2 = gg["stock_id"][~gg["stock_id"].isna()].index[0]
        gg = gg[(gg.index <= v1) & (gg.index >= v2)]
        gg = gg.reset_index(drop=True)
        # gg["Condition"] = "Orginal"
        # gg = FillGaps(gg)

    #####

    gg["percent_2"] = (gg["Number"] / gg["Total"] * 100).round(2)
    gg.loc[abs(gg.Percent - gg.percent_2) > 0.010000001, "Percent"] = np.nan
    gg.loc[gg.Percent.isnull(), "Total"] = np.nan
    gg["Percent"] = gg.Percent.fillna(method="bfill").fillna(method="ffill")
    gg["Total"] = gg.Total.fillna(method="bfill").fillna(method="ffill")

    gg = gg.drop(columns=["percent_2"])

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


def removeSlash(row):
    X = row.split("/")
    if len(X[1]) < 2:
        X[1] = "0" + X[1]
    if len(X[2]) < 2:
        X[2] = "0" + X[2]

    return int(X[0] + X[1] + X[2])


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
    df["jalaliDate"] = df.jalaliDate.apply(removeSlash)
    return df


grouped_data = df1.groupby(["name", "Holder_id"])  # ,'type'])
g_keys = list(grouped_data.groups.keys())

data = pd.DataFrame()
print(len((g_keys)))
#%%
i = g_keys[3195]
i = ("کفرا", 99.0)
gg = Cleaning(grouped_data.get_group(i))
gg[gg.date > 20081229.0]
# %%
from tqdm import tqdm

tqdm.pandas()
data = grouped_data.progress_apply(Cleaning)
data = (
    (data.reset_index(drop=True).dropna())
    .sort_values(by=["name", "date", "Percent"])
    .reset_index(drop=True)
)
#%%
data[data.name == 'کفرا'][data.date == 20081224]



#%%
data['percentSum'] = data.groupby(['name','date']).Percent.transform(sum)
data.loc[data.percentSum > 100, "Percent"] = np.nan
data.loc[data.Percent.isnull(), "Total"] = np.nan
data["Percent"] = data.groupby(["name", "Holder_id"]).Percent.fillna(method="bfill").fillna(method="ffill")
data["Total"] = data.groupby(["name", "Holder_id"]).Total.fillna(method="bfill").fillna(method="ffill")

####

data['percentSum'] = data.groupby(['name','date']).Percent.transform(sum)
data.loc[data.percentSum > 100, "Percent"] = np.nan
data.loc[data.Percent.isnull(), "Total"] = np.nan
data["Percent"] = data.groupby(["name", "Holder_id"]).Percent.fillna(method="ffill").fillna(method="bfill")
data["Total"] = data.groupby(["name", "Holder_id"]).Total.fillna(method="ffill").fillna(method="bfill")
data = data.drop(columns = ['percentSum'])



#%%
def findLow(data, dif):
    a = data.groupby("date").apply(lambda x: len(x)).to_frame().reset_index()
    a.plot(y=0, use_index=True)
    a["moving_average"] = a[0].rolling(10, min_periods=1).mean()
    a["change"] = a.moving_average - a[0]
    tempt = (a.loc[a[0] < a.moving_average - dif]).sort_values(by="date")
    return a, tempt


####
def clean(tempt, data, a):
    result = pd.DataFrame()
    for i in tqdm(tempt.date):
        pdate = a[a.date < i].tail(1).date.iloc[0]
        new = pd.concat([pd.DataFrame(), data.loc[data.date == pdate]])
        new["date"] = i
        data = data.loc[data.date != i]
        result = pd.concat([result, new])
    data = pd.concat([data, new]).sort_values(by=["name", "date", "Percent"])
    data.groupby("date").apply(lambda x: len(x)).to_frame().reset_index().plot(
        y=0, use_index=True
    )
    return data


#%%
a, tempt = findLow(data, 50)
data = clean(tempt, data, a)
a, tempt = findLow(data, 25)
data = clean(tempt, data, a)
#%%
tempt = pdf[["jalaliDate", "date"]].drop_duplicates()
mapingdict = dict(zip(tempt.date, tempt.jalaliDate))
data["jalaliDate"] = data.date.map(mapingdict)
# %%
data[data.name == 'البرز'][data.date == 20110713]
#%%