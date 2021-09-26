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
df1.loc[df1.Holder == "شخص حقيقي","Holder"] = "اشخاص حقیقی"

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
# dropholders = [
#     "سایر سهامدارن",
#     "اعضا هیئت مدیره",
#     "اشخاص حقیقی",
#     "اشخاص حقوقی",
#     "سهام حقوقی",
#     "سهام حقیقی",
#     "سهام کارکنان",
#     "سهام کارگری",
#     "سهام مسدود",
#     "سهام وثیقه",
#     "شرکت های گروه",
#     "شهرداری ها",
#     "کارکنان",
#     "کارگران",
#     "کارگزاران",
#     "مدیران شرکت",
#     "هیئت مدیره",
#     "کد رزرو صندوقهای سرمایه گذاری قابل معامله",
#     "کد رزرو صندوق های سرمایه گذاری قابل معامله",
#     "کدواسط دستورالعمل اجرایی",
#     "سلب حق تقدم",
# ]
# df1 = df1.drop(df1.loc[df1["Holder"].isin(dropholders)].index)
df1.isnull().sum()
#%%
ids = df1[df1["Holder"].isnull()]["Holder_id"].tolist()
Holders[Holders["Holder_id"].isin(ids)]
# %%
Holders[Holders["Holder_id"].isin(ids)].to_excel(path + "NewHolder.xlsx")
# %%
# %%
