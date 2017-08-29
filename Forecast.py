#Read/Write DB
import mysql.connector as mariadb
#Work with Times & Dates
import datetime


#curid to forecast
for_curid = "ethereum"

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

#Specify SQL to run to find difference
dbdiff = "SELECT price_btc, last_updated FROM Table3 WHERE curid = (%s)"

cursor.execute(dbdiff, (for_curid,))
result = cursor.fetchall()
row = 0
value1 = (0,datetime.datetime.min)
value2 = (0,datetime.datetime.min)
value3 = (0,datetime.datetime.min)
for i in result:
    value1 = result[row]
    print(value1[0])
    if value1[1] > value2[1]:
        value1 = value2
        value2 = result[row]
    if value2[1] > value3[1]:
        tmp = value2
        value2 = value3
        value3 = tmp
    row = row + 1
print(value1[1])
print(value2[1])
print(value3[1])

