import requests
import time
import pytz
from datetime import datetime as date
from elasticsearch import Elasticsearch
from datetime import timezone


def utc_to_est(utc_dt):
    return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=pytz.timezone('US/Eastern'))

# connect to amazon elasticsearch
# place elasticsearch url into '.es-url' file in the same folder
es = Elasticsearch([open('.es-url', 'r').readline().strip()])

# continue updating stocks while market is open (9am EST to 4pm EST)
while 9 <= utc_to_est(date.utcnow()).hour < 16:
    print("getting web page...")
    # get stock info, for Amazon, Google, Apple, and Microsoft
    page = requests.get('http://finance.yahoo.com/d/quotes.csv?s=AMZN+GOOG+AAPL+MSFT&f=sl')
    print("Done.")

    # update elasticsearch
    for line in page.iter_lines():
        # split data
        data = [x.strip('"') for x in line.decode("UTF-8").split(",")]
        l = [x.strip().strip('"') for x in data.pop(1).split('-')]
        # special case for time of the stock price
        l[1] = l[1][len('<b>'):-len("</b>") - 1]

        # assemble json for ES
        doc = {'symbol': data[0], 'price': float(l[1])}

        # insert into ES
        es.index(index='stock_data', doc_type='ticker', body=doc, id= data[0] + "|" + date.today().strftime("%Y%m%d") + '@' + l[0])
        print(doc)
    print('\n')

    # recheck for new data in 1.5 minutes
    time.sleep(90)
