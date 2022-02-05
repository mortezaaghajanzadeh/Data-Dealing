#%%
import finpy_tse as fpy
import tqdm
import pandas as pd
pathd = r"C:\Users\RA\Desktop\RA_Aghajanzadeh\Data\\"
pathd = r"E:\RA_Aghajanzadeh\Data\\"

names = pd.read_pickle(r"E:\RA_Aghajanzadeh\GitHub\Data-Dealing\Stock Prices\\" + "names.p")
#%%

Data = pd.DataFrame()
for i in tqdm.tqdm(names):
    t = fpy.Get_Price_History(stock=i,ignore_date= True,adjust_price = True,double_date=True).reset_index()

# %%
