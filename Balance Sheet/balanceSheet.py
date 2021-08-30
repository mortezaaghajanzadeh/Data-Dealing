#%%
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import pandas as pd
from bs4 import BeautifulSoup

path = r"C:\Program Files (x86)\chromedriver.exe"
from selenium import webdriver

driver = webdriver.Chrome(path)
driver.get(
    "https://codal.ir/ReportList.aspx?search&LetterType=6&AuditorRef=-1&PageNumber=1&Audited&NotAudited=false&IsNotAudited=false&Childs=false&Mains&Publisher=false&CompanyState=-1&Length=12&Category=1&CompanyType=-1&Consolidatable&NotConsolidatable=false"
)
print(driver.title)


#%%
links = []


def crawlonepage(links):
    try:
        reports = (
            driver.find_element_by_id("divLetterFormList")
            .find_element_by_class_name("scrollContent")
            .find_elements_by_xpath("//a[@ng-if='letter.HasHtml']")
        )
        driver.implicitly_wait(2)
        for report in reports:
            url = report.get_attribute("href")
            links.append(url)
        return links
    except:
        return crawlonepage(links)


lastpage = True


numbers = int(driver.find_elements_by_class_name("ng-binding")[-1].text)


for i in range(1, int(numbers / 20) + 2):
    try:
        element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "divLetterFormList"))
        )

    except:
        print("No")

    links = crawlonepage(links)

    try:
        driver.find_element_by_xpath("//li[@title='صفحه بعدی']").click()
    except:
        print("error")

# #%%
# while lastpage:
#     try:
#         element = WebDriverWait(driver, 10).until(
#             EC.presence_of_element_located((By.ID, "divLetterFormList"))
#         )

#     except:
#         print("No")

#     links = crawlonepage(links)

#     try:
#         driver.find_element_by_xpath(
#                 "//li[@title='صفحه بعدی']"
#                 ).click()
#     except:
#         lastpage = False
# break
#%%
def gotobalamcesheet(first):
    element = driver.find_element_by_xpath("//div[@title='صفحه بعدی']")
    flag = False
    if first != "ترازنامه":
        element.click()
        # time.sleep(0.25)
        next = driver.find_element_by_xpath("//option[@selected='selected']").text
        if next != "ترازنامه":
            flag = True
        while flag:
            element = driver.find_element_by_xpath("//div[@title='صفحه بعدی']")
            element.click()
            next = driver.find_element_by_xpath("//option[@selected='selected']").text
            # print(next)
            if next == "ترازنامه":
                flag = False
            elif next == first:
                flag = False

    else:
        True


def balancesheetpage(url):
    driver.get(url)
    first = driver.find_element_by_xpath("//option[@selected='selected']").text
    element = driver.find_element_by_xpath("//div[@title='صفحه بعدی']")
    gotobalamcesheet(first)
    page = driver.find_element_by_xpath("//option[@selected='selected']").text
    if page != "ترازنامه":
        print("No balance sheet")
        return False
    return True


def replslash(s):
    if s is None:
        return ""
    return s.replace("\n", "")


def replcoma(s):
    if s is None:
        return ""
    x = s.split(",")
    if len(x) > 1:
        return x[0] + x[1]
    else:
        return x[0]


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
    for i in df:
        df[i] = df[i].apply(replslash)
        df[i] = df[i].apply(replcoma)

    return df


def cleandffirsttype(df1, name, firm):
    df1 = df1.reset_index()
    for number, i in enumerate(df1["index"]):
        if "پایان عملکرد واقعی منتهی به" in i:
            x = i.split("پایان عملکرد واقعی منتهی به")[1]
            df1.loc[df1.index == number, 0] = x
            x = i.replace(x, "")
            df1.loc[df1.index == number, "index"] = x

        if "پایان سال مالی قبل" in i:
            x = i.split("پایان سال مالی قبل")[1]
            df1.loc[df1.index == number, 0] = x
            x = i.replace(x, "")
            df1.loc[df1.index == number, "index"] = x
    df1 = df1.set_index("index")
    t1 = df1.iloc[0:5, :].T
    t1 = t1[t1["ItemId"] != ""].T
    t1 = t1.rename(columns=t1.iloc[0]).drop(index="ItemId")
    t2 = df1.iloc[5:, :]
    t2 = t2.rename(columns=t2.iloc[0]).drop(index="ItemId")
    t = t1.merge(t2, left_index=True, right_index=True)
    t["name"] = name
    t["firm"] = firm
    return t


def cleandfsecondtype(df, name, firm):
    now = df.iloc[0, 0]
    previous = df.iloc[1, 0]
    df = df.drop(columns=0)
    df.loc[df.index == "پایان عملکرد واقعی منتهی به", 1] = now
    df.loc[df.index == "پایان سال مالی قبل", 1] = previous
    df = df.reindex(sorted(df.columns), axis=1)
    t1 = df.iloc[0:4, :].T
    t1 = t1[t1["شرح"] != ""].T
    t1 = t1.rename(columns=t1.iloc[0]).drop(index="شرح")
    t2 = df.iloc[4:, :]
    t2 = t2.rename(columns=t2.iloc[0]).drop(index="شرح")
    t = t1.merge(t2, left_index=True, right_index=True)
    t["نماد"] = name
    t["شرکت"] = firm
    return t


#%%
tempt = pd.DataFrame()
tempt2 = pd.DataFrame()
tempt3 = pd.DataFrame()
tempt4 = pd.DataFrame()
for number, i in enumerate(links):
    print(number)
    Flag = balancesheetpage(i)
    if not Flag:
        continue
    name = driver.find_element_by_id("ctl00_lblDisplaySymbol").text
    firm = driver.find_element_by_id("ctl00_txbCompanyName").text
    r = driver.page_source
    soup = BeautifulSoup(r, "html.parser")
    try:
        df1 = drivetable(soup, 1)
        tempt = tempt.append(cleandffirsttype(df1, name, firm))
    except:
        df = drivetable(soup, 0)
        try:
            tempt2 = tempt2.append(cleandfsecondtype(df, name, firm))
        except:
            try:
                tempt3 = tempt3.append(cleandfsecondtype(df, name, firm))
            except:
                tempt4 = tempt4.append(cleandfsecondtype(df, name, firm))


#%%
tempt3
#%%


#%%
# thisYear = driver.find_element_by_xpath(
#                 "//table[@tabindex='6']"
#                 ).find_element_by_xpath(
#                 "//tr[@class='GridHeader']"
#                 ).find_elements_by_xpath(
#                 "//th[@class='YearColumn']"
#                 )[0].text.split('\n')[1]
# lastYear = driver.find_element_by_xpath(
#                 "//table[@tabindex='6']"
#                 ).find_element_by_xpath(
#                 "//tr[@class='GridHeader']"
#                 ).find_elements_by_xpath(
#                 "//th[@class='YearColumn']"
#                 )[1].text.split('\n')[1]
# driver.find_element_by_xpath(
#     "//td[@class='GridItem YearColumn CurrentPeriod Asset']"
#                           ).text

#%%
#%%
#%%
#%%
driver.find_element_by_link_text("جستجوی اطلاعیه‌ها").click()
driver.find_element_by_id("reportType").click()
driver.find_element_by_link_text("اطلاعات و صورت مالی سالانه")
#%%


driver.find_element_by_link_text("تلفیقی").click()

#%%

driver.find_element_by_xpath(
    "//span[@class='text' and text()='همه اطلاعیه‌ها']"
).click()
driver.find_elements_by_link_text("صورت‌های مالی")[1].click()
driver.find_element_by_link_text("اطلاعات و صورتهای مالی میاندوره ای").click()
#%%
driver.find_element_by_xpath("//span[@class='text' and text()=' همه وضعیت‌ها']").click()
driver.find_element_by_link_text("حسابرسی شده").click()
#%%
main = driver.find_elements_by_xpath("//a[@data-toggle ='tooltip']")
