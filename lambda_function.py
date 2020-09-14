import json
import yfinance as yf
import numpy as np
import pandas as pd
import os
from s3helper import *
from botocore.exceptions import ClientError
import boto3

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def download_file(bucketname,objectname,filename):
	s3 = boto3.client('s3')
	s3.download_file(bucketname,objectname,filename)

def do_stock_update():
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




def lambda_handler(event, context):
    # TODO implement
    do_stock_update()
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
