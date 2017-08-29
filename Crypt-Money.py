import urllib.request, json
import mysql.connector as mariadb
#Converting timestamps
import datetime
#Exit gracefully on ctrl-c
import sys, signal
#Delay to stop overpolling API
import time

#Code to run when ctrl-c is passed
def signal_handler(signal, frame):
    print("Program interruped with ctrl-c, closing connection to DB")
    #Clean up DB connections once finished
    cursor.close()
    mariadb_connection.close()
    print("DB connection closed. Exiting")
    sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)


#address to grab JSON from
url="https://api.coinmarketcap.com/v1/ticker/?limit=10"

#Specify login details for database
#I need to get this separated out to a file
#    so passwords, etc. are not hard coded.
dbuser = "rex"
dbpassword = "rexhunt1"
dbhost = "rexoregan.linkpc.net"
dbdatabase = "cryptovalue"

#Connect to database specified above & create cursor for working with it.
mariadb_connection = mariadb.connect(user = dbuser, password = dbpassword,
                                     host = dbhost, database = dbdatabase)
cursor = mariadb_connection.cursor(buffered=True)

#Specify the SQL statement to load data into the database
dbinsert = "INSERT INTO Table3 (curid,price_btc,last_updated) VALUES (%s,%s,%s)"
#Specify the SQL query to check if we have more recent data
dblatest = "SELECT MAX(last_updated) FROM Table3 WHERE curid = (%s)"

#All runonce code finished. Following is code to loop infinitly
while True:
    #Catch exceptions with grabbing data from website
    try:
        #Load JSON data into memory
        with urllib.request.urlopen(url) as jsonurl:
            data = json.loads(jsonurl.read().decode())
    except TimeoutError:
        print("Timeout error has occurred.")
        print("Waiting 2min before retrying")
        time.sleep(120)
        continue
    time.sleep(6)
    #Loop through all currencies from the JSON
    count=0
    while count < len(data):
        curid = data[count]["id"]
        price_btc = data[count]["price_btc"]
        last_updated = str(datetime.datetime.fromtimestamp(int(
            data[count]["last_updated"])))
        #Check to see if we actually need to update the DB
        cursor.execute(dblatest,(curid,))
        result = cursor.fetchone()[0]
        if result != "(None,)":
            db_date = result
        else:
            db_date = 0
        last_updated = datetime.datetime.strptime(last_updated,
                                             "%Y-%m-%d %H:%M:%S")
        if last_updated == db_date:
            print("Timestamps are =, keeping old DB values")
        else:
            print("Timestamps are <> Adding to DB.")
            cursor.execute(dbinsert,(curid, price_btc, last_updated))
        count = count + 1
    
    mariadb_connection.commit()
    print("Finished inserting to database")




