#%%
import pandas as pd

a = pd.read_csv(r"E:\RA_Aghajanzadeh\Data\\" + "Cleaned_Stocks_Holders_1400_06_28.csv")
# %%

# a[a.jalaliDate>14000000][a.jalaliDate<14000400].to_csv(
#     r"E:\RA_Aghajanzadeh\Data\Samples\Cleaned_Stocks_Holders_Sample.csv", index=False
#     )
len(a[a.jalaliDate==13970105].name.unique()),len(
    a[a.jalaliDate==13980105].name.unique())