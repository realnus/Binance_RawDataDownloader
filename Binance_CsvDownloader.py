#Download from Url

from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen
import pandas as pd
import numpy as np
# or: requests.get(url).content
import requests
import pyodbc
from datetime import datetime
import db_credentials

print("input Cumulative for Daily or Monthly (cumulative) , default = Daily, Daily=1, Monthly = 2")
Cumulative = input()
if(Cumulative == ""):
    Cumulative = "Daily"
elif(Cumulative == "1"):
    Cumulative = "Daily"
elif(Cumulative == "2"):
    Cumulative = "Monthly"

print("Cumulative-",Cumulative, "selected")

print("input CsvDataType , default = kline, kline=1, trades = 2, aggtrades = 3 ")
CsvDataType = input()
if(CsvDataType == ""):
    CsvDataType = "kline"
elif(CsvDataType == "1"):
    CsvDataType = "kline"
elif(CsvDataType == "2"):
    CsvDataType = "trades"    
elif(CsvDataType == "3"):
    CsvDataType = "aggtrades"  

print("CsvDataType-",CsvDataType, "selected")

print("input interval for kline: 1m for 1 minute ... Hit Enter for 1m or other time frame")
interval = input()
if(interval == ""):
    interval = "1m"

Symbol = "" 
# server = 'myserver,port' # to specify an alternate port
server =  db_credentials.server 
database = 'Binance'
username = db_credentials.username 
password = db_credentials.password 
conn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

#Read Symbols List
sqlSymbols = "select distinct(Pair) from OneMinute where Pair Not IN (select Distinct(Symbol) from  AggTrades with(nolock)) order by 1 asc"

"""
error pairwise
**mtlusdt HATA VERDİ bak ona, bakalım tekrar hata verecek mi
ADAUSDC Invalid String length hatası verdi
 ALICEUSDT hata verdi
 ALPHABTC
 BTCSTBUSD
 DOGEBIDR
"""


df_Pairs = pd.read_sql_query(sqlSymbols, conn)

for index1, row1 in df_Pairs.iterrows():
    Symbol = row1["Pair"]

    #Symbol = "SANTOSUSDT"
    file_in_Zip= Symbol + "-"+ interval  +"-2022-04.csv"

    fileNameToDownload = Symbol + "-aggTrades-2022-04.zip"
    url = "https://data.binance.vision/data/spot/monthly/aggTrades/"+Symbol+"/" + fileNameToDownload

    savezipFilePath = "E:\\MyProjects2\\Live_Services\\RawDataDownloader\\MonthlyDownloaded\\xtras\\"

    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') , Symbol , " Downloading..." )
    req = requests.get(url)
    # Split URL to get the file name
    if(req.status_code == 200):
    
        filename = url.split('/')[-1]
        
        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') , Symbol , " Saving..." )

        # Writing the file to the local file system
        with open(savezipFilePath + filename, 'wb') as f:
            f.write(req.content)
