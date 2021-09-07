#%%
# %%
import pandas as pd
import pandas as pd

path = r"G:\Economics\Finance(Prof.Heidari-Aghajanzadeh)\Data\\"
pdf = pd.read_parquet(path + "Cleaned_Stock_Prices_1400_06_16.parquet")

#%%
gg = pdf.groupby(["date", "group_id"])
pdf = pdf.set_index(["date", "group_id"])
pdf["Weight"] = gg.MarketCap.sum()
pdf["Weight"] = pdf.MarketCap / pdf.Weight
pdf["industry_return"] = pdf["return"] * pdf.Weight
pdf = pdf.reset_index()
gg = pdf.groupby(["date", "group_id"])
pdf = pdf.set_index(["date", "group_id"])
pdf["industry_return"] = gg.industry_return.sum()
mapingdf = gg.size().to_frame()
mapdict = dict(zip(mapingdf.index, mapingdf[0]))
pdf["industry_size"] = pdf.index.map(mapdict)
data2 = pdf.reset_index()
#%%
# def marketCapAndWeight(g):
#     g["Weight"] = g.MarketCap / (g.MarketCap.sum())
#     g["industry_return"] = (g["return"] * g["Weight"]).sum()
#     g["industry_size"] = len(g)
#     return g
# data2 = gg.apply(marketCapAndWeight)
#%%

pdf2 = pd.DataFrame()
pdf2 = pdf2.append(data2).reset_index(drop=True).sort_values(by=["name", "date"])
pdf2.isnull().sum()
pdf2["industry_index"] = 1
first = (
    pdf2.groupby(["group_id", "date"])
    .first()[["group_name", "industry_return", "industry_index", "industry_size"]]
    .reset_index()
)
#%%
first["industry_index"] = first.industry_return / 100 + 1
first["industry_index"] = first.groupby("group_id").industry_index.cumprod()
first.to_csv(path + "IndustryIndexes_1400-06-16.csv", index=False)
first
# %%
