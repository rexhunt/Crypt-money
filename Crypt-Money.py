import urllib.request, json
import mysql.connector as mariadb
#Converting timestamps
import datetime

#address to grab JSON from
url="https://api.coinmarketcap.com/v1/ticker/?limit=10"

#Load JSON data into memory
with urllib.request.urlopen(url) as jsonurl:
    data = json.loads(jsonurl.read().decode())

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


#Loop through all currencies from the JSON
count=0
while count < len(data):
    curid = data[count]["id"]
    price_btc = data[count]["price_btc"]
    last_updated = str(datetime.datetime.fromtimestamp(int(
        data[count]["last_updated"])))
    #Check to see if we actually need to update the DB
    cursor.execute(dblatest,(curid,))
    result = str(cursor.fetchone())
    if result != "(None,)":
        db_date = datetime.datetime.strptime(result,
                                         "(datetime.datetime(%Y, %m, %d, %H, %M, %S),)")
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

#Clean up once finished
cursor.close()
mariadb_connection.close()


