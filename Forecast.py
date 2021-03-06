#Read/Write DB
import mysql.connector as mariadb
#Work with Times & Dates
import datetime
#Exit gracefully on ctrl-c
import sys, signal
#Delay to slow down forecasting
import time

#Round to nearest time
#Grabbed from https://stackoverflow.com/questions/3463930/how-to-round-the-minute-of-a-datetime-object-python/10854034#10854034
def roundTime(dt=None, roundTo=60):
   """Round a datetime object to any time laps in seconds
   dt : datetime.datetime object, default now.
   roundTo : Closest number of seconds to round to, default 1 minute.
   Author: Thierry Husson 2012 - Use it as you want but don't blame me.
   """
   if dt == None : dt = datetime.datetime.now()
   seconds = (dt.replace(tzinfo=None) - dt.min).seconds
   rounding = (seconds+roundTo/2) // roundTo * roundTo
   return dt + datetime.timedelta(0,rounding-seconds,-dt.microsecond)

#curid to forecast
for_curid = "ethereum"

#Specify login details for database
#I need to get this separated out to a file
#    so passwords, etc. are not hard coded.
dbuser = "rex"
dbpassword = "rexhunt1"
dbhost = "rexoregan.linkpc.net"
dbdatabase = "cryptovalue"

#Code to run when ctrl-c is passed
def signal_handler(signal, frame):
    print("Program interruped with ctrl-c, closing connection to DB")
    #Clean up DB connections once finished
    cursor.close()
    mariadb_connection.close()
    print("DB connection closed. Exiting")
    sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)

#Connect to database specified above & create cursor for working with it.
mariadb_connection = mariadb.connect(user = dbuser, password = dbpassword,
                                     host = dbhost, database = dbdatabase)
cursor = mariadb_connection.cursor(buffered=True)

#Specify SQL to run to find difference
dbdiff = "SELECT price_btc, last_updated FROM Table3 WHERE curid = (%s)"
#Specify SQL to get list of curids on DB
dbids = "SELECT DISTINCT curid from Table3"
id_row = 0

#Loop until killed
while True:

    #Work on all of the different curids on DB
    cursor.execute(dbids)
    ids = cursor.fetchall()
    for id_row in range(0, len(ids)):
        for_curid = ids[id_row][0]

        cursor.execute(dbdiff, (for_curid,))
        result = cursor.fetchall()
        result_row = 0
        value1 = (0,datetime.datetime.min)
        value2 = (0,datetime.datetime.min)
        value3 = (0,datetime.datetime.min)
        for i in result:
            value1 = result[result_row]
            if value1[1] > value2[1]:
                value1 = value2
                value2 = result[result_row]
            if value2[1] > value3[1]:
                tmp = value2
                value2 = value3
                value3 = tmp
            result_row = result_row + 1
        #Calculate the rate of difference over the last 3 data points
        diff1_2=(value1[0] - value2[0],
                 abs((value1[1]-value2[1]).total_seconds()))
        diff2_3=(value2[0] - value3[0],
                 abs((value1[1]-value2[1]).total_seconds()))
        diffps1_2=diff1_2[0]/diff1_2[1]
        diffps2_3=diff2_3[0]/diff2_3[1]
        
        #Get average change
        avgdiffps = diffps1_2+diffps2_3/2
        #avgtime = datetime.timedelta(seconds=int(diff1_2[1]+diff2_3[1]/2))
        #Force forcast to be 30 min into the future.
        avgtime = datetime.timedelta(minutes=30)
        
        #Calculate new values
        newprice_btc = value1[0] + (avgdiffps * avgtime.total_seconds())
        #Round to nearest 5 min
        newtime=roundTime(value1[1] + avgtime,roundTo=5*60)
        
        #Specify SQL to push calculated values
        dbnew = "INSERT INTO Forecasts (curid, price_btc, forecast_time, time_now) VALUES (%s, %s, %s, %s)"
        #Specify SQL to check if last forecast has changed
        dbforeold = "SELECT MAX(forecast_time) FROM Forecasts where curid = (%s)"
        
        #Grab last forecast from database
        cursor.execute(dbforeold, (for_curid,))
        oldfore = cursor.fetchone()[0]
        dump = cursor.fetchall()
        
        if oldfore == newtime:
            #print("The last forecast was for the same time. Not sending duplicate calc.")
            #Ignored to clean up output.
            pass
            #Do Nothing
        else:
            #Insert the values to the forecast table
            cursor.execute(dbnew,(for_curid, newprice_btc, newtime, datetime.datetime.now()))
            print("Wrote new price_btc of:", newprice_btc, " at ", newtime, " for ", for_curid)
        mariadb_connection.commit()
        
