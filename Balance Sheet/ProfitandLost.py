import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver

path = r"C:\Program Files (x86)\chromedriver.exe"
from selenium.webdriver.chrome.options import Options

chrome_options = Options()
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--headless")
driver = webdriver.Chrome(path, options=chrome_options)

path = r"E:\RA_Aghajanzadeh\Data\\"
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


def gen_sheet(link):
    driver.get(link)
    name = driver.find_element_by_id("ctl00_txbSymbol").text
    period = driver.find_element_by_id("ctl00_lblPeriod").text
    date = driver.find_element_by_id("ctl00_lblPeriodEndToDate").text[:4]
    r = driver.page_source
    soup = BeautifulSoup(r, "html.parser")
    return name, period, date, drivetable(soup, 0).T


#%%
path = "E:\RA_Aghajanzadeh\Data\ProfitandLost\\"
error = []
print(len(Links))
name, period, date, df = gen_sheet(Links[0])
df
#%%
for number, link in enumerate(Links):
    print(number)
    try:
        name, period, date, df = gen_sheet(link)
        df.to_excel(path + "{}+{}+{}.xlsx".format(period, name, date))
    except:
        error.append(link)

# %%
