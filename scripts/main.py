import yfinance
import datetime
import time
import oracledb

def example1():
    ticker_String = "AAPL"
    ticker = yfinance.Ticker(ticker_String)

    historical_data = ticker.history(period="1mo")  # data for the last month

    #Display a summary of the fetched data
    print(f"Summary of Historical Data for {ticker_String}:")
    print(historical_data[['Open', 'High', 'Low', 'Close', 'Volume']])
    print(historical_data['Low'])
    print(historical_data.tail(1))


def message_handler(message):
    print("Received message:", message)


def example2():
    with yfinance.WebSocket() as ws:
        ws.subscribe(["AAPL", "BTC-USD"])
        ws.listen(message_handler)


def connecttoDB():
    print("Connecting to Oracledb")
    db = oracledb.connect(
        user="DEV4",
        password="Comp430proj!",
        dsn="(description=(retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1522)(host=adb.ca-montreal-1.oraclecloud.com))(connect_data=(service_name=g87364870a1b374_stockalgorithimictrading_medium.adb.oraclecloud.com))(security=(ssl_server_dn_match=yes)))",
        wallet_location="C:/Database_Utils/Wallet_stockAlgorithimicTrading",
        wallet_password="Comp430proj!"
    )
    print("Connected to Oracledb user "+ db.username)
    return db

# only one 'DATEKEY' 'STOCKINFOKEY' 'SOURCEKEY' when checking if input is valid
def convertdatetokeys(date):
    #converts datetime to key's found in db
    pass


def getData(toget,db):
    sql = '''SELECT * FROM '''+toget
    cursor = db.cursor()

    try:
        cursor.execute(sql)
    except Exception as e:
        print("Error:", e)
    return cursor.fetchall()


def insertData(params,db):
    print("--------------- inside insert ---------------")
    sql = '''INSERT INTO DEV4.TEST_STOCK_FACT(STOCKFACTKEY, DATEKEY, STOCKINFOKEY, SOURCEKEY, OPEN_PRICE, VOLUME, TIME_KEY, HIGH_PRICE ,LOW_PRICE) VALUES(:STOCKFACTKEY, :DATEKEY, :STOCKINFOKEY, :SOURCEKEY, :OPEN_PRICE, :VOLUME, :TIME_KEY, :HIGH_PRICE , :LOW_PRICE)'''

    cursor = db.cursor()
    try:
        cursor.execute(sql,params)
        db.commit()
    except Exception as e:
        print("Error:", e)


def main():
    loopcounter = 0
    ticker = yfinance.Ticker("AAPL")
    db = connecttoDB()
    factdata = getData('''DEV4.TEST_STOCK_FACT''',db)
    print(factdata[-1][0])


    #Main loop for grabbing data from yfinance and uploading it to oracledb
    while True:
        try:
            data = ticker.history(period="1d", interval="1m")
            latest = data.iloc[-2]

            print(latest)
            print("Datetime: "+ str(latest.name))
            print("Open: "+str(latest.Open))
            print("High: "+str(latest.High))
            print("Low: "+str(latest.Low))
            print("Close: "+str(latest.Close))
            print("Volume: "+str(latest.Volume))

            params = {
                'STOCKFACTKEY': factdata[-1][0] + 1 + loopcounter, #<-- grabs the last entery in fact table and sets the fact key to 1 + it's fact key value + loopcounter
                'DATEKEY': 1,
                'STOCKINFOKEY': 1,
                'SOURCEKEY': 1,
                'OPEN_PRICE': latest.Open,
                'VOLUME': latest.Volume,
                'TIME_KEY': 1,
                'HIGH_PRICE': latest.High,
                'LOW_PRICE': latest.Low
            }
            print(params)
            #insertData(params,db)

            #print(datetime.datetime.now(), latest)

        except Exception as e:
            print("Error:", e)
        loopcounter += 1
        time.sleep(60)


#example1()
#example2()
main()