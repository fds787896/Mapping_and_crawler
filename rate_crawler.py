# import packages
from datetime import date
from selenium import webdriver
from bs4 import BeautifulSoup as bs
from selenium.webdriver.support.ui import Select
import datetime as dt
import time
import pandas as pd



def Chrome(path, url):
    driver = webdriver.Chrome(path)
    driver.get(url)
    time.sleep(5)
    return driver


def driver_ops(driver, date, cur):
    driver.find_element_by_xpath("//input[@id='erectDate']").send_keys(date)
    time.sleep(1)
    driver.find_element_by_xpath("//input[@id='nothing']").send_keys(date)
    time.sleep(1)
    Select(driver.find_element_by_xpath("//select[@id='pjname']")).select_by_value(cur)
    time.sleep(1)
    driver.find_element_by_xpath("//tbody/tr[1]/td[7]/input[1]").click()
    time.sleep(5)
    return driver


def getinfo(driver, lst):
    soup = bs(driver.page_source, "html.parser")
    ta = soup.find("tr", attrs={"class": "odd"})
    for i, td in enumerate(ta.find_all("td")):
        if i == 2:
            lst.append(float(td.text) / 100)
        else:
            pass
    return lst


def to_df(lst):
    df = pd.DataFrame({"币别": ["现金美金", "甲米地L8比索", "现金比索", "索莱尔L17比索", "H88厅比索"],
                       "人民币汇率": [None] * 5, "披索汇率": [None] * 5})
    df['人民币汇率'].loc[0] = lst[0]
    df['人民币汇率'].loc[1:4] = lst[1]
    df['披索汇率'].loc[0] = lst[0] / lst[1]
    df['披索汇率'].loc[1:4] = 1
    return df


def main():
    today = date.today().replace(day=1)
    ystd = (today - dt.timedelta(days=1)).strftime("%Y-%m-%d")
    driver = Chrome("C:\Program Files\JetBrains\PyCharm Community Edition 2021.1\chrome_driver\chromedriver.exe", "https://www.bankofchina.com/sourcedb/whpj/")
    driver = driver_ops(driver, ystd, "美元")
    lst = []
    lst = getinfo(driver, lst)
    time.sleep(5)
    driver.get("https://www.bankofchina.com/sourcedb/whpj/")
    driver = driver_ops(driver, ystd, "菲律宾比索")
    lst = getinfo(driver, lst)
    driver.close()
    df = to_df(lst)
    df.to_excel(
        r"Z:\09-tableau-data\mapping表\{}\exchange.xlsx".format((today - dt.timedelta(days=1)).strftime("%Y-%m")),
        index=False)
    return df


if __name__ == "__main__":
    main()