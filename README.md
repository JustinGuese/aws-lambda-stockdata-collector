# aws-lambda-stockdata-collector
Calls the yfinance yahoo weekly to construct a huge database of minute data stock data provided by yahoo finance, where the maximum period for minute data is 1 week, and saves it to s3

# What does it do?

Yahoo finance minute data download is limited to 7 days, that is why I want to call this script weekly to build up a huge minute-data database for selected stocks

# Installation

- Upload the zip file to lambda layers which includes pandas, yfinance, numpy and boto3, which are libraries needed to download data
- Copy contents of lambda_function.py to lambda console with the same name
- give s3 access to your target bucket via lambda iam policies
- set maximum execution time to e.g. 3 minutes
- for weekly execution set a cloudwatch rule for weekly
- set environment variables
  - BUCKET : name of bucket that files should be retrieved and copied to
  - STOCKS : list of stocks to be downloaded, e.g. ["MSFT","AAPL"]. Get valid stock names from yahoo finance
  - INTERVAL : interval according to yfinance. e.g. minute data = "1m", hourly data = "1h", daily data = "1d"
