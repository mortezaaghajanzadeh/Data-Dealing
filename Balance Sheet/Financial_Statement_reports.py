#%%
import pandas as pd
import  pickle
from selenium import webdriver

path = r"C:\Program Files (x86)\chromedriver.exe"
from selenium.webdriver.chrome.options import Options

chrome_options = Options()
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--headless")
driver = webdriver.Chrome(path, options=chrome_options)
#%%

period = ""
per_page = 100
page = 1


def gen_codal_link(period, per_page, page):
    codal_link = "https://my.codal.ir/fa/statements/?company_id=&my_basket=&statement_type=146&period={}&financial_years=&company_type=0&status=1&tracing_number=&publisher_state=&title=&from_date=&to_date=&parent_or_subset=1&consolidated_or_not=&per_page={}&page={}".format(
        period, per_page, page
    )
    return codal_link


def get_all_links():
    links = []
    for i in range(101):
        reports = (
            driver.find_element_by_id("tTable")
            .find_element_by_class_name("grid-txt")
            .find_elements_by_xpath(f"//*[@id='template-container']/tr[{i}]/td[4]/a")
        )
        for report in reports:
            url = report.get_attribute("href")
            links.append(url)
    return links


#%%
all_links = []
for page in range(1, 166):
    print(page)
    codal_link = gen_codal_link(period, per_page, page)
    driver.get(codal_link)
    links = get_all_links()
    for link in links:
        all_links.append(link)

path = r"E:\RA_Aghajanzadeh\Data\\"
pickle.dump(
    all_links,
    open(path + "Codal_reports_14000725.p", "wb")
)

# %%
