#%%
import pandas as pd
import re
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

path = r"E:\RA_Aghajanzadeh\Data\PriceTradeData\\"
pathR = r"E:\RA_Aghajanzadeh\Data\\{}"
df = pd.read_parquet(path + "mergerdPriceAllData_cleaned.parquet")
# %%
df["name"] = df["name"].apply(lambda x: convert_ar_characters(x))
df["Firm"] = df["Firm"].apply(lambda x: convert_ar_characters(x))
df["market"] = df["market"].apply(lambda x: convert_ar_characters(x))
#%%
df.to_parquet(pathR.format("ind_ins_trade_{}.parquet".format(str(df.jalaliDate.max()))))
#%%