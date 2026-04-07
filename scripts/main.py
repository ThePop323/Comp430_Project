import yfinance
import datetime
import time
import oracledb
import pandas


def connecttoDB():
    print("-- Connecting to Oracledb")
    db = oracledb.connect(
        user="DEV4",
        password="Comp430proj!",
        dsn="(description=(retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1522)(host=adb.ca-montreal-1.oraclecloud.com))(connect_data=(service_name=g87364870a1b374_stockalgorithimictrading_medium.adb.oraclecloud.com))(security=(ssl_server_dn_match=yes)))",
        wallet_location="C:/Database_Utils/Wallet_stockAlgorithimicTrading",
        wallet_password="Comp430proj!"
    )
    print("-- Connected to Oracledb user "+ db.username)
    return db


def getData(toget,db):
    print("Getting " +toget+ " from db")
    sql = '''SELECT * FROM '''+toget
    cursor = db.cursor()

    try:
        cursor.execute(sql)
    except Exception as e:
        print("Error:", e)
    return cursor.fetchall()


def insertData(params,db):
    #print("--------------- inside insert ---------------")
    sql = '''INSERT INTO DEV.MARKET_FACT(DATEKEY, ASSETKEY, SOURCEKEY, OPEN_PRICE, VOLUME, TIME_KEY, HIGH_PRICE ,LOW_PRICE) VALUES(:DATEKEY, :ASSETKEY, :SOURCEKEY, :OPEN_PRICE, :VOLUME, :TIME_KEY, :HIGH_PRICE , :LOW_PRICE)'''

    cursor = db.cursor()
    try:
        cursor.execute(sql,params)
        db.commit()
    except Exception as e:
        print("Error:", e)


def getDateKey(date_dim,time_dim,stockdatetime):
    currentdatetime = stockdatetime.to_pydatetime()
    datekey = -1
    timekey = -1

    for i in date_dim:
        if i[0] == currentdatetime.day and i[1] == currentdatetime.month and i[3] == currentdatetime.year:
            datekey = i[5]
            break

    for i in time_dim:
        if i[1] == currentdatetime.hour and i[2] == roundTime(currentdatetime.minute):
            timekey = i[0]
            break

    return datekey, timekey


def roundTime(mins):
    if mins >= 0 and mins < 15:
        return 0
    elif mins >= 15 and mins < 30:
        return 15
    elif mins >= 30 and mins < 45:
        return 30
    elif mins >= 45 and mins <= 60:
        return 45
    else:
        return -1

# Grabs given assetname's asset key and asset type from asset_dim
def getAssetInfo(asset_dim,assetname):
    assetkey = -1
    assettype = -1
    for i in asset_dim:

        if assetname == i[1]:
            assetkey = i[0]
            assettype = i[5]
            return assetkey, assettype
    return assetkey, assettype

def checkDuplicate(market_dim,datekey,timekey,assetkey):
    sourcekey = 1
    for i in market_dim:
        #print("datekey: "+str(datekey)+" . "+str(i[1])+" | timekey: "+str(timekey)+" . "+str(i[6])+" | assetkey: "+str(assetkey)+" . "+str(i[2]))
        if datekey == i[1] and timekey == i[6] and assetkey == i[2] and sourcekey == i[3]:
            return True

    return False


def checkValidInt(tocheck):
    if tocheck <= 0:
        return None
    return tocheck


def checkValidKeys(key):
    if key == -1:
        return False
    return True


def checkVolume(latest):
    if latest.Volume < 0:
        return -1
    if latest.Volume == 0 and latest.Open != latest.Close:
        return -1
    return latest.Volume


def singleStock(db, assetname, date_dim, time_dim, asset_dim):

    # collect assetkey
    assetkey, assettype = getAssetInfo(asset_dim,assetname)
    # gather yfinance data if stock exsists in asset_dim
    if assetkey == -1:
        print("------- "+assetname+" does not exsist inside of database, please check assetnamelist and correct to valid input")
        return
    else:
        ticker = yfinance.Ticker(assetname)
        data = ticker.history(period="1d", interval="15m")
        latest = data.iloc[-2]

    #gets the market fact table that is isolated to the stock or bond in question
    market_fact = getData(f'''DEV.MARKET_FACT WHERE ASSETKEY = {assetkey}''',db)

    #collect date and time keys
    datekey, timekey = getDateKey(date_dim,time_dim,latest.name)


# checks to see if values are non-zero and non-negative, if so set to None
    latest.Open = checkValidInt(latest.Open)
    latest.High = checkValidInt(latest.High)
    latest.Low = checkValidInt(latest.Low)
    latest.Close = checkValidInt(latest.Close)
    #checks that volume is non zero, as well comfirms that if it is zero that the open and close prices are the same
    if assettype != "Bond":
        latest.Volume = checkVolume(latest)

    params = {
        'DATEKEY': datekey,
        'ASSETKEY': assetkey,
        'SOURCEKEY': 1, #always 1 since this is the refferance to the yfinance sourcekey inside the source_dim
        'OPEN_PRICE': latest.Open,
        'VOLUME': latest.Volume,
        'TIME_KEY': timekey,
        'HIGH_PRICE': latest.High,
        'LOW_PRICE': latest.Low
    }

    complete = False
    if not checkDuplicate(market_fact,datekey,timekey,assetkey):
        if checkValidKeys(latest.Volume):
            # checks both datekey and timekey to assure that they are not == -1, as if they are then they are not valid
            if checkValidKeys(datekey) and checkValidKeys(timekey):
                print("------- Inserting data for "+assetname+" with asset key "+str(assetkey)+" to db")
                insertData(params,db)
                complete = True
            else:
                print("---- Refusing to input due to invalid date time keys")
                complete = True
        else:
            print("---- Refusing to input due to invalid Volume")
            complete = True
    else:
        print("---- Refusing to input due to params already being inside fact table")
    if complete == False:
        print("Failed to collect. Waiting 1 min then retrying")
        time.sleep(60)
        singleStock(db, assetname, date_dim, time_dim, asset_dim)
        print("Retrying collection")

def main():

    running = True
    assetnamelist = ["GOOGL","^FVX","GC=F","^DJI"]
    db = connecttoDB()


    ##Gets all the dims for compliance checks
    date_dim = getData("DEV.DATE_DIM",db) # day = 0 month = 1 year = 3 datekey = 5
    time_dim = getData("DEV.TIME_DIM",db)
    asset_dim = getData("DEV.ASSET_DIM",db)
    while running:
        starttime = time.time()
        for i in assetnamelist:

            try:
                singleStock(db, i, date_dim, time_dim, asset_dim)
            except Exception as e:
                print("Error:", e)
        print("---------------- completed loop ----------------")
        #used to consistantly run the program every 15 min. Done by subtracting the 15min (900s) by the runtime.
        sleeptime = max(0,900 - (time.time() - starttime))
        time.sleep(sleeptime)



# run le program
main()
