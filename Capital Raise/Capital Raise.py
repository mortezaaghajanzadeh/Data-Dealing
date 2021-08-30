#%%
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import requests

path = r"C:\Program Files (x86)\chromedriver.exe"
from selenium import webdriver

driver = webdriver.Chrome(path)
#%%
driver.get("https://codal360.ir/fa/")
print(driver.title)

try:
    element = WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.CLASS_NAME, "close"))
    )
    time.sleep(2)
    driver.find_element_by_class_name("close").click()
except:
    print("No pop up")


# %%
driver.find_element_by_link_text("افزایش سرمایه").click()
driver.find_element_by_xpath(
    "//span[@class='text' and text()='همه اطلاعیه‌ها']"
).click()
driver.find_element_by_link_text("افزایش سرمایه /سود").click()
driver.find_element_by_link_text("اطلاعیه").click()
driver.find_element_by_link_text("آگهی ثبت افزایش سرمایه").click()

#%%
def getEachReport():
    try:
        linklist = []
        main = driver.find_elements_by_xpath(
            "//td[@class ='grid-txt statement-td statement-collapse']"
        )
        time.sleep(0.5)
        for number, i in enumerate(main):
            # print(number)
            try:
                link = i.find_element_by_link_text(
                    "آگهی ثبت افزایش سرمایه"
                ).get_attribute("href")
                linklist.append(link)
            except:
                try:
                    link = i.find_element_by_link_text(
                        "آگهی ثبت افزایش سرمایه(اصلاحیه)"
                    ).get_attribute("href")
                    linklist.append(link)
                except:
                    link = i.find_element_by_link_text(
                        "مشمول اصل 44 - آگهی ثبت افزایش سرمایه"
                    ).get_attribute("href")
                    linklist.append(link)
        return linklist
    except:
        return getEachReport()


#%%
x = driver.find_element_by_id("total_record").text.split(",")
links = []
for i in range(1, int(int(x[0] + x[1]) / 10) + 2):
    print(i)
    l = getEachReport()
    if l is None:
        print("Is None")
    links.append(l)

    if (i % 4 == 0) & (i != 0):
        driver.find_element_by_link_text("»").click()
    else:
        try:
            driver.find_element_by_link_text(str(i + 1)).click()
        except:
            break

#%%
t = []
for i in links:
    for j in i:
        t.append(j)
#%%
links = []
for id, url in enumerate(t):
    print(id)
    driver.get(url)
    element = WebDriverWait(driver, 5).until(
        EC.presence_of_element_located((By.CLASS_NAME, "container-iframe"))
    )
    time.sleep(0.5)
    i = driver.find_element_by_class_name("container-iframe")
    url = i.find_element_by_id("statementContent").get_attribute("src")
    links.append(url)


#%%
import requests
from bs4 import BeautifulSoup
import logging


def calsum(l):
    text = ""
    for i in l:
        text += i
    return text


def capitalRaise(text):
    char = "                                            \r"
    texts = list(filter(lambda x: x != char, text))
    revaluation, cash, profit, reserve, premium = 0, 0, 0, 0, 0
    for number, i in enumerate(texts):
        print(i)
        i = i.replace("\r", "").replace("\xa0", "")
        # print(i)
        if "نماد" in i:
            name = i.split("نماد: ")[1]
        elif "مجمع عمومی" in i:
            EXORDate = texts[number + 1]
        elif "سرمایۀ شرکت از مبلغ" in i:
            x = texts[number + 1].split(",")
            BeforeCap = float(calsum(x))
        elif "به‌مبلغ" in i:
            x = texts[number + 1].split(",")
            AfterCap = float(calsum(x))
        elif "تجدید ارزیابی" in i:
            x = texts[number + 1]
            revaluation = float(x)
        elif "مطالبات و آورده نقدی" in i:
            x = texts[number + 1]
            cash = float(x)
        elif "سود انباشته" in i:
            x = texts[number + 1]
            profit = float(x)
        elif "اندوخته" in i:
            x = texts[number + 1]
            reserve = float(x)
        elif "صرفه" in i:
            x = texts[number + 1]
            premium = float(x)
        elif "در تاریخ" in i:
            RegisterDate = texts[number + 1]

    return (
        name,
        EXORDate,
        RegisterDate,
        BeforeCap,
        AfterCap,
        profit,
        cash,
        revaluation,
        reserve,
        premium,
    )


def crawling(url):
    logging.captureWarnings(True)
    r = requests.get(url, verify=False)

    soup = BeautifulSoup(r.text, "html.parser")

    text = []
    for i in soup.findAll("table")[2].text.split("\n"):
        
        if i != "":
            text.append(i)

    (
        name,
        EXORDate,
        RegisterDate,
        BeforeCap,
        AfterCap,
        profit,
        cash,
        revaluation,
        reserve,
        premium,
    ) = capitalRaise(text)
    return [
        name,
        EXORDate,
        RegisterDate,
        BeforeCap,
        AfterCap,
        profit,
        cash,
        revaluation,
        reserve,
        premium,
    ]


#%%
data = []
for i in links[:5]:
    data.append(crawling(i))

#%%
url = links[0]
crawling(url)

# %%
logging.captureWarnings(True)
r = requests.get(url, verify=False)

soup = BeautifulSoup(r.text, "html.parser")
#%%
text = []
for i in soup.findAll("table")[2].text.split("\n"):
    if i != "":
        text.append(i)
text