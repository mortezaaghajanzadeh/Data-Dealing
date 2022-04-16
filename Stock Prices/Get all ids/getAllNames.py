#%%
path = r"C:\Program Files (x86)\chromedriver.exe"
from selenium import webdriver
#%%
driver = webdriver.Chrome(path)
driver.get("https://tse.ir/listing.html?cat=cash&section=alphabet")
#%%
print(driver.title)
chars = [
    "ا",
    "ب",
    "پ",
    "ت",
    "ث",
    "ج",
    "چ",
    "ح",
    "خ",
    "د",
    "ذ",
    "ر",
    "ز",
    "ژ",
    "س",
    "ش",
    "ص",
    "ض",
    "ط",
    "ظ",
    "ع",
    "غ",
    "ف",
    "ق",
    "ك",
    "گ",
    "ل",
    "م",
    "ن",
    "و",
    "ه",
    "ی",
    "ك",
    "گ",
    "دِ",
    "بِ",
    "زِ",
    "ذِ",
    "شِ",
    "سِ",
    "ى",
    "ي",
]
names = []
for i in chars:
    try:
        lis = driver.find_element_by_id(
            "c_table_{}".format(i)
        ).find_elements_by_class_name("status_A")
        for report in lis:
            url = report.text.split("1")
            print(url[0])
            names.append(url[0])
    except:
        continue
len(names)
#%%
names = list(set(names))
len(names)
#%%
import finpy_tse as fpy
f_stock_list = fpy.Build_Market_StockList(bourse = True, farabourse = True, payeh = True, detailed_list = True, show_progress = True, 
                                           save_excel = True, save_csv = False, save_path = 'E:\RA_Aghajanzadeh\Data')
for i in list(f_stock_list.index.unique() ):
    names.append(i)
#%%
names = list(set(names))
len(names)
#%%
import pickle
pickle.dump(names, open("names.p", "wb"))

# %%
