#%%
import pandas as pd
import pytse_client as tse
path = r"E:\RA_Aghajanzadeh\Data\Price\\"
tickers = tse.download(symbols="all", include_jdate=True)
#%%
data = pd.DataFrame()
for i in tickers:
    df = tickers[i]
    df['name'] = i
    data = data.append(df)
    print(len(data))
#%%
df[df.volume == 0]