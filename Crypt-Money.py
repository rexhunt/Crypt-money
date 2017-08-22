import urllib.request, json
import mysql.connector as mariadb

url="https://api.coinmarketcap.com/v1/ticker/"

with urllib.request.urlopen(url) as jsonurl:
    data = json.loads(jsonurl.read().decode())

print(type(data[1]))

count=0
while count < len(data):
    curid = data[count]["id"]
    price_btc = data[count]["price_btc"]
    last_updated = data[count]["last_updated"]
    
    print(curid)
    print(price_btc)
    print(last_updated)
    print()
    count = count + 1


