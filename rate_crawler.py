# import packages
from datetime import date
from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import Select
import datetime as dt
import time
import pandas as pd
from sqlalchemy import create_engine
import json
import re
import shutil
import urllib
import zipfile


class Chrome:
    def __init__(self, path, url):
        self.path = path
        self.url = url

    def PointPage(self):
        Driver = webdriver.Chrome(self.path)
        Driver.get(self.url)
        time.sleep(5)
        return Driver

    def CurrencyPage(self, typedate, currency):
        Driver = self.PointPage()
        Driver.find_element_by_xpath("//input[@id='erectDate']").send_keys(typedate)
        time.sleep(1)
        Driver.find_element_by_xpath("//input[@id='nothing']").send_keys(typedate)
        time.sleep(1)
        Select(Driver.find_element_by_xpath("//select[@id='pjname']")).select_by_value(currency)
        time.sleep(1)
        Driver.find_element_by_xpath("//tbody/tr[1]/td[7]/input[1]").click()
        time.sleep(3)
        return Driver


class GetChrome(Chrome):
    def __init__(self, path, url):
        super().__init__(path, url)

    def CheckVersion(self):
        try:
            check = self.PointPage()
            check.close()
        except Exception as ex:
            error = str(ex)
            current_version = re.findall('(?<=is ).*?(?= with)', error)[0].split(".")[0]
            return current_version

    def GetVersionNo(self):
        checkNo = self.CheckVersion()
        url = "https://chromedriver.chromium.org/downloads"
        request = urllib.request.Request(url)
        with urllib.request.urlopen(request) as response:
            page = response.read()
        soap = BeautifulSoup(page, "html.parser", from_encoding="utf-8")
        tag = soap.find("ul", attrs={"class": "n8H08c UVNKR"}).find_all("li", attrs={"class": "TYR86d wXCUfe zfr3Q"})
        for item in tag[:-1]:
            if re.findall('(?<=version ).*?(?=,)', item.text)[0] == checkNo:
                versionNo = re.findall(r"(?<=ChromeDriver).*", item.text)[0].replace(" ", "")
                return versionNo

    def DownloadChrome(self, path):
        downloadversion = self.GetVersionNo()
        url = "https://chromedriver.storage.googleapis.com/{downloadversion}/chromedriver_win32.zip".format(
            downloadversion=downloadversion)
        urllib.request.urlretrieve(url, path)


def ZipFile(zippath, extractpath):
    with zipfile.ZipFile(zippath, "r") as zip_ref:
        zip_ref.extractall(extractpath)


def MoveFile(source, destination):
    shutil.move(source, destination)


def GetInfo(driver, lst, itemnumber):
    soup = BeautifulSoup(driver.page_source, "html.parser")
    target = soup.find("tr", attrs={"class": "odd"})
    for i, td in enumerate(target.find_all("td")):
        if i == itemnumber:
            lst.append(float(td.text) / 100)
        else:
            pass
    return lst


def MakeDataFrame(lst, pointdate):
    df = pd.DataFrame({"币别": ["现金美金", "甲米地L8比索", "现金比索", "索莱尔L17比索", "H88厅比索", "林吉特"],
                       "人民币汇率": [None] * 6, "披索汇率": [None] * 6})
    df['人民币汇率'].loc[0] = lst[0]
    df['人民币汇率'].loc[1:4] = lst[1]
    df['披索汇率'].loc[0] = lst[0] / lst[1]
    df['披索汇率'].loc[1:4] = 1
    df["人民币汇率"].loc[5] = lst[2]
    df[["人民币汇率", "披索汇率"]] = df[["人民币汇率", "披索汇率"]].astype(float)
    df = df.rename(columns={"币别": "账户"})
    df["日其"] = dt.datetime.strptime(pointdate, "%Y-%m")
    return df


def db_info():
    with open("config.json", mode="r") as file:
        info = json.load(file)
    return info


class SQLConnection:
    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

    def CreateConnection(self):
        engine = create_engine(
            'mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver=SQL+Server'.format(user=self.user,
                                                                                                 password=self.password,
                                                                                                 host=self.host,
                                                                                                 port=self.port,
                                                                                                 database=self.database))
        con = engine.raw_connection()
        cursor = con.cursor()
        return engine, con, cursor


class HandleDataFrame(SQLConnection):
    def __init__(self, df, savepath, host, port, user, password, database):
        super().__init__(host, port, user, password, database)
        self.df = df
        self.savepath = savepath

    def DataFrameToSQL(self):
        self.df.to_sql("exchange_rate",
                       con=self.CreateConnection()[0], if_exists="append", index=False, chunksize=1000)

    def DataFrameSave(self):
        self.df.to_excel(self.savepath, index=False)


def main():
    GetChromeObj = GetChrome(
        "C:\Program Files\JetBrains\PyCharm Community Edition 2021.1\chrome_driver\chromedriver.exe",
        "https://www.bankofchina.com/sourcedb/whpj/")
    ChromeObj = Chrome("C:\Program Files\JetBrains\PyCharm Community Edition 2021.1\chrome_driver\chromedriver.exe",
                       "https://www.bankofchina.com/sourcedb/whpj/")
    FirstDay = date.today().replace(day=1)
    pointday = (FirstDay - dt.timedelta(days=1)).strftime("%Y-%m-%d")
    LastMonth = (FirstDay - dt.timedelta(days=1)).strftime("%Y-%m")
    JasonInformation = db_info()
    savepath = r"Z:\09-tableau-data\mapping表\{date}\exchange.xlsx".format(date=LastMonth)

    def Crawler(callback):
        def Inner():
            callback()
            CurrencyList = []
            for currency in ["美元", "菲律宾比索", "林吉特"]:
                Driver = ChromeObj.CurrencyPage(pointday, currency)
                if currency != "林吉特":
                    CurrencyList = GetInfo(Driver, CurrencyList, 2)
                else:
                    CurrencyList = GetInfo(Driver, CurrencyList, 1)
            time.sleep(2)
            Driver.close()
            df = MakeDataFrame(CurrencyList, LastMonth)
            DataFrameObj = HandleDataFrame(df, savepath, "localhost", 1433, JasonInformation["user"],
                                           JasonInformation["password"], "testdb")
            DataFrameObj.DataFrameSave()
            DataFrameObj.DataFrameToSQL()

        return Inner

    @Crawler
    def UpdateChrome():
        GetChromeObj.DownloadChrome(r"D:\work\cost_living2\Driver.zip")
        ZipFile(r"D:\work\cost_living2\Driver.zip", r"D:\work\cost_living2")
        MoveFile(r"D:\work\cost_living2\chromedriver.exe",
                 r"C:\Program Files\JetBrains\PyCharm Community Edition 2021.1\chrome_driver\chromedriver.exe")

    @Crawler
    def Empty():
        print(None)

    if GetChromeObj.CheckVersion() is None:
        Empty()
    else:
        UpdateChrome()


if __name__ == "__main__":
    main()