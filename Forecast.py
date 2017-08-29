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
    if value1[1] > value2[1]:
        value1 = value2
        value2 = result[row]
    if value2[1] > value3[1]:
        tmp = value2
        value2 = value3
        value3 = tmp
    row = row + 1
#Calculate the rate of difference over the last 3 data points
diff1_2=(value1[0] - value2[0],
         abs((value1[1]-value2[1]).total_seconds()))
diff2_3=(value2[0] - value3[0],
         abs((value1[1]-value2[1]).total_seconds()))
diffps1_2=diff1_2[0]/diff1_2[1]
diffps2_3=diff2_3[0]/diff2_3[1]

#Get average change
avgdiffps = diffps1_2+diffps2_3/2
avgtime = datetime.timedelta(seconds=int(diff1_2[1]+diff2_3[1]/2))

#Calculate new values
newprice_btc = value1[0] + (avgdiffps * avgtime.total_seconds())
print(avgtime)
newtime=value1[1] + avgtime

#Specify SQL to push calculated values
dbnew = "INSERT INTO Forecasts (curid, price_btc, forecast_time, time_now) VALUES (%s, %s, %s, %s)"
#Specify SQL to check if last forecast has changed
dbforeold = "SELECT MAX(forecast_time) FROM Forecasts where curid = (%s)"

#Grab last forecast from database
cursor.execute(dbforeold, (for_curid,))
oldfore = cursor.fetchone()[0]
print(oldfore)

if oldfore == newtime:
    print("The last forecast was for the same time. Not sending duplicate calc.")
else:
    #Insert the values to the forecast table
    cursor.execute(dbnew,(for_curid, newprice_btc, newtime, datetime.datetime.now()))
    mariadb_connection.commit()
    print("Wrote new price_btc of:", newprice_btc, " at ", newtime, " for ", for_curid)
