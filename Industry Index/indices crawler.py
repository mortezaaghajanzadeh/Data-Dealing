# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import requests
import pandas as pd
import datetime
from persiantools.jdatetime import JalaliDate


# %%
industry_ids = [
    "34408080767216529",
    "19219679288446732",
    "13235969998952202",
    "62691002126902464",
    "59288237226302898",
    "69306841376553334",
    "58440550086834602",
    "30106839080444358",
    "25766336681098389",
    "12331083953323969",
    "36469751685735891",
    "32453344048876642",
    "1123534346391630",
    "11451389074113298",
    "33878047680249697",
    "24733701189547084",
    "61848754958448778",
    "20213770409093165",
    "58231368623465359",
    "29331053506731535",
    "21948907150049163",
    "40355846462826897",
    "54843635503648458",
    "15508900928481581",
    "3615666621538524",
    "33626672012415176",
    "41934470778361119",
    "65986638607018835",
    "57616105980228781",
    "70077233737515808",
    "14651627750314021",
    "34295935482222451",
    "72002976013856737",
    "25163959460949732",
    "24187097921483699",
    "41867092385281437",
    "61247168213690670",
    "61985386521682984",
    "4654922806626448",
    "8900726085939949",
    "18780171241610744",
    "47233872677452574",
    "65675836323214668",
    "59105676994811497",
    "32097828799138957",
    "67130298613737946",
]

indusrty_group_ids = [
    "01",
    "10",
    "13",
    "14",
    "17",
    "19",
    "20",
    "21",
    "22",
    "23",
    "25",
    "27",
    "28",
    "29",
    "31",
    "32",
    "33",
    "34",
    "35",
    "36",
    "38",
    "39",
    "40",
    "42",
    "43",
    "44",
    "45",
    "47",
    "49",
    "53",
    "54",
    "56",
    "57",
    "58",
    "60",
    "64",
    "65",
    "67",
    "70",
    "72",
    "73",
    "74",
    "11",
    "66",
    "overall_index",
    "EWI",
]


all_indexes = list()
for i, id in enumerate(industry_ids):
    try:
        print(i)
        url = "http://www.tsetmc.com/tsev2/chart/data/Index.aspx?i={}&t=value".format(
            id
        )
        t = requests.get(url)
        r = t.text.split(";")
        dates = list()
        nums = list()
        index_Ids = list()
        index_id = indusrty_group_ids[i]
        ind = pd.DataFrame()
        for item in r:
            s = item.split(",")
            dates.append(s[0])
            nums.append(s[1])
            index_Ids.append(index_id)
        ind["index_id"] = index_Ids
        ind["date"] = dates
        ind["index"] = nums
        all_indexes.append(ind)
    except:
        print("error in", id)
All_indexes = pd.concat(all_indexes)


now = datetime.datetime.now()
jalali = JalaliDate.to_jalali(now.year, now.month, now.day)
All_indexes.to_csv("indexes_%s.csv" % jalali)
print("End")


# %%
