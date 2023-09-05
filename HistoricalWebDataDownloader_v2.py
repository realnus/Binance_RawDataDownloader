

    #Download from Url
import os
from calendar import month
from dataclasses import dataclass
from io import BytesIO
import re
from zipfile import ZipFile
from urllib.request import urlopen
import pandas as pd
import numpy as np
# or: requests.get(url).content
import requests
import pyodbc
from datetime import datetime, timedelta
from dateutil import relativedelta
import binance_functions
import binance_api_keys
import db_credentials



binance_api_key = binance_api_keys.binance_api_key
binance_api_secret = binance_api_keys.binance_api_secret

url = 'https://api.binance.com/api/v3/klines'

Symbol = "" 
server =  db_credentials.server 
database = 'Binance'
username = db_credentials.username 
password = db_credentials.password 
conn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

#Read Symbols List
sqlSymbols = "select distinct(Pair) from OneMinute where Pair Not IN (select Distinct(Symbol) from  AggTrades with(nolock)) order by 1 asc"

start_datetime = datetime(2022,5,1)
end_datetime = datetime(2022,6,1)
DataType = "spot"
OuterPeriod = "monthly"
DataConsolidation = "klines" #klines aggTrades
TimeFrame = "1m"
fileNameToDownload = ""
savezipFilePath = "E:\\MyProjects2\\Live_Services\\Data\\Binance\\" + DataType + "\\" + OuterPeriod + "\\" + DataConsolidation + "\\"

def GellAllSymbolsAtThatDate(start_date, end_date):
    ## Loop All symbols to get their values
    df_symbols = binance_functions.Binance_getSymbols(binance_api_key,binance_api_secret)
    return df_symbols

#Todo Single Pair de sorgulayabiliriz
#df_Pairs = pd.read_sql_query(sqlSymbols, conn)

#Loop Edelin gunleri


#Tries to get the file and returns http response with content      return req.status_code, req.content
def download_Binance_Csv_File(Symbol = "BTCUSDT", DataType = "spot", OuterPeriod = "monthly", DataConsolidation = "klines", TimeFrame = "1m", DateTime = datetime.now()):
    #   https://data.binance.vision/data/spot/monthly/klines/1INCHBTC/1m/1INCHBTC-1m-2022-05.zip
        #https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2022-04.zip
    
    url = "https://data.binance.vision/data/" + DataType + "/" + OuterPeriod + "/" + DataConsolidation + "/" + Symbol
    
    #1INCHBTC-1m-2022-05.csv
    #Convert month to two digits  (For csv filename format needed)
    year = str(DateTime.year)
    month = DateTime.strftime('%m')

    """
    if(len(month) == 1):
        month = "0" + str(month)
    """

    fileNameToDownload  = ""

    if(DataConsolidation == "klines"):
        fileNameToDownload = Symbol + "-" + TimeFrame + "-" + year + "-" + month + ".zip"
        url += "/" + TimeFrame + "/" + fileNameToDownload
        #1INCHBTC-1m-2022-05.csv
        file_in_Zip = Symbol + "-" + TimeFrame + "-" + year + "-" + month + ".csv"

    elif(DataConsolidation == "aggtrades"):
        #1INCHBTC-aggTrades-2022-05.csv
        fileNameToDownload =  Symbol + "-" + "aggtrades" + "-" + year + "-" + month + ".zip"
        url += "/" + fileNameToDownload
        file_in_Zip= Symbol + "-aggTrades-" + year + "-" + month + ".csv"
        #1INCHBTC-aggTrades-2022-05.zip

    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') , Symbol , " Downloading..." )
    response = requests.get(url)    
    
    filename = url.split('/')[-1]

    return response , filename

current_datetime_start = start_datetime
current_datetime_end = ""

if(OuterPeriod == "monthly"):
    current_datetime_end = current_datetime_start + relativedelta.relativedelta(months=1)
elif(OuterPeriod == "daily"):
    current_datetime_end = current_datetime_start + timedelta(days=1)




while current_datetime_start < end_datetime:
    
    df_Pairs = GellAllSymbolsAtThatDate(current_datetime_start,current_datetime_end)

    print(current_datetime_start.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],"to", current_datetime_end.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])

    for index1, row1 in df_Pairs.iterrows():
        Symbol = row1["symbol"]
        filename = Symbol + "-" + TimeFrame + "-" + str(current_datetime_start.year) + "-" + current_datetime_start.strftime('%m') + ".zip"

        #Check if file exists
        if(os.path.exists(savezipFilePath + "\\" +  filename)) == False:

            #Download Csv File
            request = requests.request
            request, filename =  download_Binance_Csv_File(Symbol, DataType, OuterPeriod, DataConsolidation, TimeFrame,  current_datetime_start)
            
            #Save Csv File
            # Split URL to get the file name
            if(request.status_code == 200):
            
                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') , Symbol , " Saving..." )

                # Writing the file to the local file system
                with open(savezipFilePath +  filename, 'wb') as f:
                    f.write(request.content)

                print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') , Symbol , " Dataframing..." )
                df = pd.read_csv(savezipFilePath + "\\" +  filename , compression='zip', header=0, sep=',')

                if(DataConsolidation == "aggTrades"):
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

                    #Insert To Db

                    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') , Symbol , " Db Processing..." )

                    # server = 'myserver,port' # to specify an alternate port
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

                elif(DataConsolidation == "klines"):
                    df.columns = ['OpenTime', 'O', 'H', 'L', 'C', 'V','CloseTime', 'Quote_asset_volume', 'Number_of_trades', 'Taker_buy_base_asset_volume', 'Taker_buy_quote_asset_volume', 'Ignore']

                    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') , Symbol , " Df Column Conversions..." )

                    #Convert TradeTime Unix to HumanReadible yyyy-MM-dd HH:mm:ss.fff ( unit='ms' 6 digit milisecond cikariyor,datetime64[ms] ile 3'e indiriyoruz. )
                    df['TradeDateTime'] = pd.to_datetime(df['CloseTime'], unit='ms').astype('datetime64[ms]')

                    #Insert To Db

                    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') , Symbol , " Db Processing..." )

                    # server = 'myserver,port' # to specify an alternate port
                    server =  db_credentials.server 
                    database = 'Binance'
                    username = db_credentials.username 
                    password = db_credentials.password
                    conn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

                    Counter = 0
                    sqlInsert = ""
                    for index, row in df.iterrows():
                        Symbol = Symbol #Yukarıda tanımlıydı burayada tanımlayalım
                        interval = TimeFrame
                        TradeDateTime = str(row["TradeDateTime"])[:-3] #removing last 3 characters '2022-04-01 00:00:31.410000'
                        OpenTime = row["OpenTime"]
                        O = row["O"]
                        H = row["H"]
                        L = row["L"]
                        C = row["C"]
                        V = row["V"]
                        CloseTime = row["CloseTime"]
                        Quote_asset_volume = row["Quote_asset_volume"]
                        Number_of_trades = row["Number_of_trades"]
                        Taker_buy_base_asset_volume = row["Taker_buy_base_asset_volume"]
                        Taker_buy_quote_asset_volume = row["Taker_buy_quote_asset_volume"]

                        sqlInsert += """
                        Insert Into Binance_Partitioned..[Csv_Kline_1m] ([Symbol]
                                                                        ,[interval]
                                                                        ,[TradeDateTime]
                                                                        ,[OpenTime]
                                                                        ,[O]
                                                                        ,[H]
                                                                        ,[L]
                                                                        ,[C]
                                                                        ,[V]
                                                                        ,[CloseTime]
                                                                        ,[Quote_asset_volume]
                                                                        ,[Number_of_trades]
                                                                        ,[Taker_buy_base_asset_volume]
                                                                        ,[Taker_buy_quote_asset_volume])
                                                                        Values(
                                                                                '"""+ Symbol  +"""',
                                                                                '"""+ interval  +"""',
                                                                                '"""+ TradeDateTime  +"""',
                                                                                '"""+ str(OpenTime)  +"""',
                                                                                """+ str(O)  +""",
                                                                                """+ str(H)  +""",
                                                                                """+ str(L)  +""",
                                                                                """+ str(C)  +""",
                                                                                """+ str(V)  +""",
                                                                                """+ str(CloseTime)  +""",
                                                                                """+ str(Quote_asset_volume)  +""",
                                                                                """+ str(Number_of_trades)  +""",
                                                                                """+ str(Taker_buy_base_asset_volume)  +""",
                                                                                """+ str(Taker_buy_quote_asset_volume)  +"""
                                                                                );
                        
                                                                        """

                        #sqlInsert += "Insert Into Binance..AggTrades (TradeId,Symbol,Price,Quantity,TradeCount,TradeDateTime,IsTheBuyerTheMarketMaker) Values("+ str(TradeId) +",'"+ Symbol +"',"+ str(Price) + "," + str(Quantity) +","+str(TradeCount)+",'"+str(TradeDateTime)+"',"+str(IsTheBuyerTheMarketMaker)+")  \n"
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
        else:
            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') , Symbol , " Existsi skipping..." )

    if(OuterPeriod == "monthly"):
        current_datetime_start = current_datetime_start + relativedelta.relativedelta(months=1)
    elif(OuterPeriod == "daily"):
        current_datetime_start = current_datetime_start + timedelta(days=1)