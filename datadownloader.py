import yfinance as yf
import numpy as np
import pandas as pd
import os
import json
from s3helper import *
from botocore.exceptions import ClientError


stocks = json.loads(os.environ.get("STOCKS"))
interval = str(os.environ.get("INTERVAL"))
bucketname = str(os.environ.get("BUCKET"))

period = "max"
if interval == "1m":
	period = "7d"
elif interval == "1h":
	period = "720d"
elif interval == "5m":
	period = "60d"


print(stocks)

## first download existing data
old_dfs = dict()
for stock in stocks:
	try:
		filename = "%s.csv.gz"%(stock)
		download_file(bucketname,filename,filename)
		data = pd.read_csv(filename,compression="gzip")
		old_dfs.update({stock:data})
	except ClientError as e:
		print(stock, " no stock data found, will write for the first time" ,str(e))

data = yf.download(stocks,period=period,interval=interval)

data = data.reorder_levels([1,0],axis=1)
# remove weekends bc no trading activity
data = data[data.index.dayofweek < 5]

#print(data.head())

for stock in stocks:
	filename = "%s.csv.gz"%(stock)
	# stack old and new data together
	new = data[stock]
	old = old_dfs.get(stock)
	if old is not None:
		df = pd.concat([old,new])
	else:
		print("using only new data bc no old data found.",old)
		df = new
	print(df)
	print(df.shape)
	df = df[~df.index.duplicated(keep="first")]
	print(df.shape)
	df.to_csv(filename,compression="gzip",index=False)
	upload_file(filename,bucketname)
