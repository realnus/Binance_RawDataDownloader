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

Symbol = "" 
server =  db_credentials.server 
database = 'Binance'
username = db_credentials.username 
password = db_credentials.password
conn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

#Read Symbols List
sqlSymbols = "select distinct(Pair) from Symbols where Pair Not IN (Select distinct(Pair) from [dbo].[OneMinute])"

df_Pairs = pd.read_sql_query(sqlSymbols, conn)

for index1, row1 in df_Pairs.iterrows():
    Symbol = row1["Pair"]

    Symbol += "asdasd"
    #Symbol = "SANTOSUSDT"
    file_in_Zip= Symbol + "-aggTrades-2022-04.csv"

    fileNameToDownload = Symbol + "-aggTrades-2022-04.zip"
    url = "https://data.binance.vision/data/spot/monthly/aggTrades/"+Symbol+"/" + fileNameToDownload

    savezipFilePath = "C:\\MyProjects2\\CC\\Binance_Api\\RawDataDownloader\\MonthlyDownloaded\\"

    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') , Symbol , " Downloading..." )
    req = requests.get(url)
    # Split URL to get the file name
    if(req.status_code == 200):
    
        filename = url.split('/')[-1]
        
        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') , Symbol , " Saving..." )

        # Writing the file to the local file system
        with open(savezipFilePath + filename, 'wb') as f:
            f.write(req.content)

        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') , Symbol , " Dataframing..." )
        df = pd.read_csv(savezipFilePath + fileNameToDownload , compression='zip', header=0, sep=',')
        df.columns = ['TradeId', 'Price', 'Quantity', 'FirstTradeId', 'LastTradeId', 'TradeTime','IsTheBuyerTheMarketMaker', 'Ignore']

        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') , Symbol , " Df Column Conversions..." )
        #Add TradeCount to the Dataframe by using (LastTradeId - FirstTradeId)
        df["TradeCount"] = df["LastTradeId"] -  df["FirstTradeId"]

        #drop FirstTradeId , LastTradeId and Ignore columns
        df.drop(['LastTradeId','FirstTradeId', 'Ignore'], inplace=True, axis=1)

        #Convert TradeTime Unix to HumanReadible yyyy-MM-dd HH:mm:ss.fff ( unit='ms' 6 digit milisecond cikariyor,datetime64[ms] ile 3'e indiriyoruz. )
        df['TradeDateTime'] = pd.to_datetime(df['TradeTime'], unit='ms').astype('datetime64[ms]') 

        #drop TradeTime (Unix)
        df.drop(['TradeTime'], inplace=True, axis=1)

        #Convert to 1 and 0 ( 1 for True, 0 for False)
        df['IsTheBuyerTheMarketMaker_conv'] = np.where(df['IsTheBuyerTheMarketMaker'] == 'TRUE', 1, 0)

        #Drop IsTheBuyerTheMarketMaker column
        df.drop(['IsTheBuyerTheMarketMaker'], inplace=True, axis=1)

        #Rename IsTheBuyerTheMarketMaker_conv to IsTheBuyerTheMarketMaker
        df.rename(columns = {'IsTheBuyerTheMarketMaker_conv':'IsTheBuyerTheMarketMaker'}, inplace = True)

        #print(df.head(5))
        #print("a")

        #Insert To Db

        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') , Symbol , " Db Processing..." )

        server =  db_credentials.server 
        database = 'Binance'
        username = db_credentials.username 
        password = db_credentials.password
        conn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

        Counter = 0
        sqlInsert = ""
        for index, row in df.iterrows():
            Symbol = Symbol #Yukarıda tanımlıydı burayada tanımlayalım
            TradeId = row["TradeId"]
            Price = row["Price"]
            Quantity = row["Quantity"]
            TradeCount = row["TradeCount"]
            TradeDateTime = str(row["TradeDateTime"])[:-3] #removing last 3 characters '2022-04-01 00:00:31.410000'
            IsTheBuyerTheMarketMaker = row["IsTheBuyerTheMarketMaker"]

            sqlInsert += "Insert Into Binance..AggTrades (TradeId,Symbol,Price,Quantity,TradeCount,TradeDateTime,IsTheBuyerTheMarketMaker) Values("+ str(TradeId) +",'"+ Symbol +"',"+ str(Price) + "," + str(Quantity) +","+str(TradeCount)+",'"+str(TradeDateTime)+"',"+str(IsTheBuyerTheMarketMaker)+")  \n"
            #Bulk update if reaches to 100 records
            Counter +=1

            if(Counter >= 100):  
                cursor = conn.cursor()
                cursor.execute(sqlInsert)
                conn.commit()
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), Symbol, " Inserted ", Counter, " records")
                sqlInsert = ""
                Counter = 0

        #Loop bitti, kalan datalar varsa guncellenmesi gereken onları da guncelleyelim
        cursor = conn.cursor()
        cursor.execute(sqlInsert)
        conn.commit()
        print(Symbol, " Inserted remaining last ", Counter, " records")
        sqlInsert = ""  
        Counter = 0  
    