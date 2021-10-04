# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import pandas as pd
import statsmodels.api as sm
import numpy as np
import matplotlib as plt
import re


# %%
def vv4(row):
    row = str(row)
    X = [1, 1, 1]
    X[0] = row[0:4]
    X[1] = row[4:6]
    X[2] = row[6:8]
    return X[0] + "-" + X[1] + "-" + X[2]


def vv(row):
    X = row.split("-")
    return X[0] + X[1] + X[2]


def vv2(row):
    X = row.split("/")
    return X[0]


def DriveYearMonthDay(d):
    d["jalaliDate"] = d["jalaliDate"].astype(str)
    d["Year"] = d["jalaliDate"].str[0:4]
    d["Month"] = d["jalaliDate"].str[4:6]
    d["Day"] = d["jalaliDate"].str[6:8]
    d["jalaliDate"] = d["jalaliDate"].astype(int)
    return d


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


# %%
path = r"E:\RA_Aghajanzadeh\Data\\"
n = path + "Cleaned_Stocks_Holders_1400_06_28.csv"
df = pd.read_csv(n)
#######
df = df.drop(df.loc[df["Holder"] == "شخص حقیقی"].index)
df = df.drop(
    df[
        (df["Trade"] == "No")
        & (
            (df["close_price"] == 10)
            | (df["close_price"] == 1000)
            | (df["close_price"] == 10000)
            | (df["close_price"] == 100000)
        )
    ].index
)
df = df.drop(df[(df["symbol"] == "وقوام") & (df["close_price"] == 1000)].index)
symbols = [
    "سپرده",
    "هما",
    "وهنر-پذیره",
    "نکالا",
    "تکالا",
    "اکالا",
    "توسعه گردشگری ",
    "وآفر",
    "ودانا",
    "نشار",
    "نبورس",
    "چبسپا",
    "بدکو",
    "چکارم",
    "تراک",
    "کباده",
    "فبستم",
    "تولیددارو",
    "قیستو",
    "خلیبل",
    "پشاهن",
    "قاروم",
    "هوایی سامان",
    "کورز",
    "شلیا",
    "دتهران",
    "نگین",
    "کایتا",
    "غیوان",
    "تفیرو",
    "سپرمی",
    "بتک",
]
df = df.drop(df[df["symbol"].isin(symbols)].index)
df = df.drop(df[(df.symbol == "اتکای") & (df.close_price == 1000)].index)
HolderData = df


# %%
HolderData.tail()


# %%
n1 = path + "Cleaned_Stock_Prices_1400_06_29" + ".parquet"
df = pd.read_parquet(n1)
df = df.drop_duplicates()
df = df[df.volume != 0]
df = df.sort_values(by=["name", "jalaliDate"]).rename(columns={"name": "symbol"})

df = DriveYearMonthDay(df)

df.columns


# %%
df.tail()


# %%
n2 = path + "balance sheet - 9811" + ".xlsx"
df2 = pd.read_excel(n2)
df2 = df2.iloc[:, [0, 4, 13, -7]]
df2.rename(
    columns={
        df2.columns[0]: "symbol",
        df2.columns[1]: "date",
        df2.columns[2]: "BookValue",
        df2.columns[3]: "Capital",
    },
    inplace=True,
)
df2["shrout"] = df2["Capital"] * 100
df2["Year"] = df2["date"].apply(vv2)
df2["Year"] = df2["Year"].astype(str)
df2 = df2.drop(columns=["date", "Capital"])
col = "symbol"
df2[col] = df2[col].apply(lambda x: convert_ar_characters(x))


# %%
len(set(df2.symbol))


# %%
PriceData = df
PriceData = PriceData.merge(df2, on=["symbol", "Year"], how="left")
PriceData['shrout'] = np.nan
PriceData.loc[PriceData.shrout.isnull(),'shrout'] = PriceData.loc[PriceData.shrout.isnull()].shrout_x
PriceData.loc[PriceData.shrout.isnull(),'shrout'] = PriceData.loc[PriceData.shrout.isnull()].shrout_y
PriceData = PriceData.drop(columns = ['shrout_x','shrout_y'])
PriceData[["BookValue", "shrout"]
          ] = PriceData[["BookValue", "shrout"]
                                               ].fillna(
    method="ffill"
)
PriceData['MarketCap'] = PriceData.close_price * PriceData.shrout

# %%
PriceData = PriceData[
    ["jalaliDate", "date", "symbol", "close_price", "shrout", "BookValue","return"]
]
PriceData["date1"] = PriceData["date"].apply(vv4)
PriceData["date1"] = pd.to_datetime(PriceData["date1"])
PriceData["day_of_year"] = PriceData["date1"].dt.day
PriceData["week_of_year"] = PriceData["date1"].dt.week
PriceData["year_of_year"] = PriceData["date1"].dt.year


# %%
PriceData.tail()


# %%
gg = PriceData.groupby(["symbol"])
PriceData["yearRet"] = gg["close_price"].pct_change(periods=250) * 100
PriceData["Ret"] = PriceData['return']

# %%
PriceData = PriceData.reset_index(drop=True)
PriceData = PriceData.sort_values(by=["symbol", "date"])


# %%
shrout = HolderData[["symbol", "shrout", "date", "jalaliDate"]].drop_duplicates()
fkey = zip(list(shrout.symbol), list(shrout.date))
mapingdict = dict(zip(fkey, shrout.shrout))
PriceData["shrout2"] = PriceData.set_index(["symbol", "date"]).index.map(mapingdict)
PriceData.loc[~PriceData.shrout2.isnull(), "shrout"] = PriceData.loc[
    ~PriceData.shrout2.isnull()
]["shrout2"]
PriceData = PriceData.drop(columns=["shrout2"])



# %%

import requests


def removeSlash(row):
    X = row.split("/")
    if len(X[1]) < 2:
        X[1] = "0" + X[1]
    if len(X[2]) < 2:
        X[2] = "0" + X[2]

    return X[0] +"-" + X[1] +"-" + X[2]

def Overall_index():
    url = r"http://www.tsetmc.com/tsev2/chart/data/Index.aspx?i=32097828799138957&t=value"
    r = requests.get(
                url
            )
    jalaliDate = []
    Value = []
    for i in r.text.split(";"):
        x = i.split(',')
        jalaliDate.append(x[0])
        Value.append(float(x[1]))
    df = pd.DataFrame({'jalaliDate' :jalaliDate,
                'Value' : Value,
                }, 
                columns=['jalaliDate','Value'])
    df["jalaliDate"] = df.jalaliDate.apply(removeSlash)
    return df

def removeDash(row):
    X = row.split("-")
    if len(X[1]) < 2:
        X[1] = "0" + X[1]
    if len(X[2]) < 2:
        X[2] = "0" + X[2]

    return int(X[0] + X[1] + X[2])

overal_index = Overall_index()
mapingdict = dict(
    zip(
        df.jalaliDate,df.date
    )
)

overal_index['jalaliDate'] = overal_index.jalaliDate.apply(removeDash)
index = overal_index.dropna().rename(columns = {"Value":"Index"})

index["Market_return"] = index["Index"].pct_change(periods=1) * 100
index
#%%

Index = PriceData.merge(index, on="jalaliDate")
Index = (
    Index[
        ["jalaliDate", "date", "week_of_year", "year_of_year", "Index", "Market_return"]
    ]
    .drop_duplicates()
    .sort_values(by=["date"])
)

# %%
PriceData = (
    PriceData.merge(index, on=["jalaliDate"])
    .drop_duplicates()
    .sort_values(by=["symbol", "date"])
)


# %%

PriceData["MarketCap"] = PriceData["close_price"] * PriceData["shrout"] / 1e5
PriceData["MarketCap"] = PriceData["MarketCap"].astype(int)
PriceData["BookToMarket"] = PriceData["BookValue"] / PriceData["MarketCap"]
PriceData = PriceData.reset_index(drop=True)
PriceData.tail()


# %%
def Factor(g):
    Large = g.loc[g["MarketCap"] >= g["MarketCap"].quantile(0.9)]["Ret"].mean()
    Small = g.loc[g["MarketCap"] <= g["MarketCap"].quantile(0.1)]["Ret"].mean()

    Value = g.loc[g["BookToMarket"] >= g["BookToMarket"].quantile(0.9)]["Ret"].mean()
    Growth = g.loc[g["BookToMarket"] <= g["BookToMarket"].quantile(0.1)]["Ret"].mean()

    Winner = g.loc[g["yearRet"] >= g["yearRet"].quantile(0.9)]["Ret"].mean()
    Loser = g.loc[g["yearRet"] <= g["yearRet"].quantile(0.1)]["Ret"].mean()

    g["Winner_Loser"] = Winner - Loser
    g["SMB"] = Small - Large
    g["HML"] = Value - Growth

    g = g[
        ["jalaliDate", "date", "SMB", "HML", "Winner_Loser", "Market_return"]
    ].drop_duplicates(["date"])
    return g


WL = PriceData.groupby(["date"])
Factors = WL.apply(Factor)
Factors = Factors.reset_index(drop=True).drop(0)


# %%
Factors.tail()


# %%
Factors.to_excel(path + "\Factors_Daily_1400_06_28.xlsx", index=False)


# %%


# %%


# %%


# %%


# %%


# %%
