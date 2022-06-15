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
tempt = pdf[["jalaliDate", "date"]].drop_duplicates().sort_values(by=["date"])
mapingdict = dict(zip(tempt.date, tempt.jalaliDate))

#%%
df1["jalaliDate"] = df1.date.map(mapingdict)
df1.loc[df1.date == 20090914, "jalaliDate"] = 13880623
df1.loc[df1.date == 20090916, "jalaliDate"] = 13880625
df1["jalaliDate"] = df1.jalaliDate.astype(int)
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
df1[df1.name == "جم"][df1.jalaliDate == 14000107][
    ["name", "Holder", "Percent", "jalaliDate", "date", "Holder_id"]
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
    .sort_values(by=["name", "date", "Percent"])
    .reset_index()
)

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

df1[df1.name == "جم"][df1.jalaliDate == 13991226][
    ["name", "Holder", "Percent", "jalaliDate", "date", "Holder_id"]
]
#%%
a = df1.groupby("date").size().to_frame().reset_index()
a.plot(y=0, use_index=True)
#%%

data = pd.DataFrame()
data = pd.concat([data,df1])
data = (
    (data.reset_index(drop=True).dropna())
    .sort_values(by=["name", "date", "Percent"])
    .drop(columns=["close_price"])
    .drop_duplicates(subset=["name", "date", "Holder_id"])
    .reset_index(drop=True)
)
#%%
data.loc[data.name == "جم"][data.jalaliDate == 13991226][
    ["name", "Holder", "Percent", "jalaliDate", "date", "Holder_id"]
]


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
data.loc[data.date == 20090914,'jalaliDate'] = 13880623
data.loc[data.date == 20090916,'jalaliDate'] = 13880625
# %%
data[data.jalaliDate.isnull()].date.unique()
#%%
data.head()
#%%
data.to_csv(path2 + "Cleaned_Stocks_Holders_1401_02_21.csv",index = False) 

# %%
