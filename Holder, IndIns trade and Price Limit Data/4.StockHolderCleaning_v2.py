#%%
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
pdf = pd.read_parquet(path2 + "Cleaned_Stock_Prices_14001127.parquet")
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
            # | (pdf.group_name == "صندوق سرمایه گذاری قابل معامله")
            | (pdf.instId.str.startswith("IRK"))
            | (pdf.title.str.startswith("سپرده"))
            | (pdf.title.str.startswith("ح"))
            | (pdf.title.str.contains("اختيارخ"))
            | (pdf.title.str.contains("اختيارف"))
            | (pdf.title.str.contains("اختيار"))
            | (pdf.title.str.contains("آتي"))
            | (pdf.title.str.contains("بازار اوراق بدهي"))
            | (pdf.title.str.startswith("سلف"))
            | (pdf.title.str.contains("بورس کالا"))
            | (pdf.title.str.contains("اوراق"))
        )
    ].name.unique()
)
invalid_names.append("سنگ آهن")
invalid_names.append("کرد")
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
df1 = df1[df1.Holder != 'nan']
print(len(df1))
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
df3 = pd.read_excel(path + "shareholder_names_cleaned_140101_v1.xlsx")
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