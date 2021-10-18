

#%%
# Balance Sheet
Balance_Links = [i + "0" for i in links]


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


def gen_balance_sheet(link):
    driver.get(link)
    name = driver.find_element_by_id("ctl00_txbSymbol").text
    period = driver.find_element_by_id("ctl00_lblPeriod").text
    date = driver.find_element_by_id("ctl00_lblPeriodEndToDate").text[:4]
    r = driver.page_source
    soup = BeautifulSoup(r, "html.parser")
    return name, period, date, drivetable(soup, 0).T

#%%
Balance_Sheets = pd.DataFrame()
path = "E:\RA_Aghajanzadeh\Data\BalanceSheet\\"
error = []
print(len(Balance_Links))
for number, link in enumerate(Balance_Links):
    print(number)
    try:
        name, period, date, df = gen_balance_sheet(link)
        df.to_excel(path + "{}+{}+{}.xlsx".format(period,name, date))
    except:
        error.append(link)

# %%
