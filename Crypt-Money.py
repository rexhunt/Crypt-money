import urllib.request, json
import mysql.connector as mariadb

#address to grab JSON from
url="https://api.coinmarketcap.com/v1/ticker/"

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
cursor = mariadb_connection.cursor()

#Specify the SQL statement to load data into the database
dbinsert = "INSERT INTO Table3 (curid,price_btc,last_updated) VALUES (%s,%s,%s)"


#Loop through all currencies from the JSON
count=0
while count < len(data):
    curid = data[count]["id"]
    price_btc = data[count]["price_btc"]
    last_updated = data[count]["last_updated"]
    
    print(curid)
    print(price_btc)
    print(last_updated)
    print()
    cursor.execute(dbinsert,(curid, price_btc, last_updated))
    mariadb_connection.commit()
    count = count + 1



