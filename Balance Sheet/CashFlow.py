import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

path = r"C:\Program Files (x86)\chromedriver.exe"
from selenium.webdriver.chrome.options import Options

chrome_options = Options()
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--headless")
driver = webdriver.Chrome(path, options=chrome_options)

path = r"E:\RA_Aghajanzadeh\Data\Financial Statements\\"
links = pd.read_pickle(path + "Codal_reports_14000725.p")

#%%
# Balance Sheet
Links = [i + "1" for i in links]


def drivetable(soup, number):

    header = soup.find_all("table")[number].find("tr")
    list_header = []
    for items in header:
        try:
            list_header.append(items.get_text())
        except:
            continue

    # for getting the data
    HTML_data = soup.find_all("table")[number].find_all("tr")[1:]
    data = []
    for element in HTML_data:
        sub_data = []
        for sub_element in element:
            try:
                sub_data.append(sub_element.get_text())
            except:
                continue
        data.append(sub_data)
    df = pd.DataFrame(data=data, columns=list_header).T
    return df


#%%
Balance_Sheets = pd.DataFrame()
path = "E:\RA_Aghajanzadeh\Data\Financial Statements\CashFlow\\"
error = []
print(len(Links))


def first_type(link):
    driver.get(link)
    name = driver.find_element_by_id("ctl00_txbSymbol").text
    period = driver.find_element_by_id("ctl00_lblPeriod").text
    date = driver.find_element_by_id("ctl00_lblPeriodEndToDate").text[:4]
    r = driver.page_source
    soup = BeautifulSoup(r, "html.parser")
    st_df = drivetable(soup, 0).T
    return st_df, name, period, date


def sec_type(link):
    driver.get(link)
    name = driver.find_element_by_id("ctl00_txbSymbol").text
    period = driver.find_element_by_id("ctl00_lblPeriod").text
    date = driver.find_element_by_id("ctl00_lblPeriodEndToDate").text[:4]
    table = driver.find_element_by_id("ctl00_cphBody_UpdatePanel1")
    dfbase = pd.DataFrame()
    table = (
        WebDriverWait(driver, 10)
        .until(
            EC.visibility_of_element_located(
                (
                    By.CSS_SELECTOR,
                    "#ctl00_cphBody_UpdatePanel1",
                )
            )
        )
        .get_attribute("outerHTML")
    )
    df = pd.read_html(str(table))[1]
    dfbase = dfbase.append(df, ignore_index=True)
    return dfbase, name, period, date


def gen_file(link):
    driver.get(link)
    r = driver.page_source
    soup = BeautifulSoup(r, "html.parser")
    st_df = drivetable(soup, 0).T
    if len(st_df) < 13:
        df, name, period, date = sec_type(link)
    else:
        df, name, period, date = first_type(link)
    return df, name, period, date


l = r"https://my.codal.ir/fa/statement/279580/1"
df, name, period, date = gen_file(l)
df

#%%
for number, link in enumerate(Links):
    print(number)
    try:
        df, name, period, date = gen_file(link)
        df.to_excel(path + "{}_{}_{}.xlsx".format(period, name, date))
    except:
        error.append(link)

# %%

# %%


# %%
import pickle

pickle.dump(error, open(path + "errors.p", "wb"))
