import numpy as np
import pandas as pd
from bsedata.bse import BSE
import time
import sqlalchemy as db
from sqlalchemy import create_engine, inspect,text
# from sqlalchemy import SQLAlchemyError
import pymysql
import os
from dotenv import load_dotenv

load_dotenv()


pd.set_option('display.max_rows',500)
pd.set_option('display.max_columns',500)
pd.set_option('display.width',1000)

github_excel_url = 'https://raw.githubusercontent.com/jangid6/Stock-ETL-Project/main/Equity.xlsx'
engine = 'openpyxl'  #openpyxl is a Python library to read/write Excel 2010 xlsx/xlsm/xltx/xltm files.
Equity_df = pd.read_excel(github_excel_url,engine = engine)
Equity_df.head(2)  #view first two rows of the dataframe
print("-------equity df ------",Equity_df.head(2))
print("length of dataframe",len(Equity_df))
# convert security code to string
Equity_df['Security Code'] = Equity_df["Security Code"].astype(str)

# calling base api to fetch the stocks data for eg: price, code, updated data etc..
# creating the 50 data frame basis on the security id (Hard code the dataframe)

nifty50_stock_symbols = [ "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
    "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV", "BPCL", "BHARTIARTL",
    "BRITANNIA", "CIPLA", "COALINDIA", "DIVISLAB", "DRREDDY", "EICHERMOT",
    "GRASIM", "HCLTECH", "HDFCBANK", "HDFCLIFE", "HEROMOTOCO", "HINDALCO",
    "HINDUNILVR", "ICICIBANK", "ITC", "INDUSINDBK", "INFY", "JSWSTEEL",
    "KOTAKBANK", "LTIM", "LT", "M&M", "MARUTI", "NTPC", "NESTLEIND",
    "ONGC", "POWERGRID", "RELIANCE", "SBILIFE", "SBIN", "SUNPHARMA",
    "TCS", "TATACONSUM", "TATAMOTORS", "TATASTEEL", "TECHM", "TITAN",
    "UPL", "ULTRACEMCO", "WIPRO"
]

# we want to filter equity pdf by looking of 'security id' col value present in nifty50_stock_symbols

nifty50_SQDF = Equity_df[Equity_df['Security Id'].isin(nifty50_stock_symbols).reset_index(drop = True)]
nifty50_SQDF.rename(columns = {'Group':'CompanyGroup'}, inplace = True)
nifty50_SQDF.columns = nifty50_SQDF.columns.str.replace(' ','')  # remove empty space from column name
print('--------------------',len(nifty50_SQDF))

nifty50_SQDF['SecurityCode'].values
print("--------------",nifty50_SQDF['SecurityCode'].values)

# Creating Bse Object
bseObject = BSE(update_codes=True)
print("--------------------",bseObject)
list_of_stocks_dict = []
sqcode_Listnf50 = nifty50_SQDF['SecurityCode'].values
for sqcode in sqcode_Listnf50:
    print('---------sqcode=================',sqcode)
    try:
        stock_data = bseObject.getQuote(sqcode) #key value pair
        # stock_data.pop('buy',None)
        # stock_data.pop("sell",None)
        stock_df = pd.DataFrame([stock_data])
        list_of_stocks_dict.append(stock_df)
        time.sleep(0.5)

    except:
        print("error in fetching data for ",sqcode)

niftyDaily50tbale = pd.concat(list_of_stocks_dict, ignore_index=True).iloc[:,:-2]

# niftyDaily50tbale = pd.DataFrame([list_of_stocks_dict])
print("---------------niftyDaily50tbale---------",niftyDaily50tbale)
print("---------------length---------",len(niftyDaily50tbale))

print("-------*****--------",niftyDaily50tbale.head())


#=============== Data cleaning and Data Processing========

# Rename the colum name
niftyDaily50tbale.rename(columns={'group':'sharegroup'},inplace=True)
niftyDaily50tbale.rename(columns={'52weekHigh': 'fiftytwoweekHigh'}, inplace=True)
niftyDaily50tbale.rename(columns={'52weekLow': 'fiftytwoweekLow'}, inplace=True)
niftyDaily50tbale.rename(columns={'2WeekAvgQuantity': 'twoWeekAvgQuantity'}, inplace=True)
nifty50DailyTableTest_SF = niftyDaily50tbale.copy()
print("Columns in nifty50DailyTableTest_SF:", nifty50DailyTableTest_SF.columns)

if 'updatedOn' not in nifty50DailyTableTest_SF.columns:
    nifty50DailyTableTest_SF.rename(columns={'originalColumnName': 'updatedOn'}, inplace=True)

# Convert 'updatedOn' column to datetime and extract date
if 'updatedOn' not in nifty50DailyTableTest_SF.columns:
    print("The 'updatedOn' column is missing.")
else:
     nifty50DailyTableTest_SF['updatedOn'] = pd.to_datetime(nifty50DailyTableTest_SF['updatedOn'],format='%d %b %y | %I:%M %p', errors='coerce' )


# Check is there any missing or invalid date values----------
if pd.isna(nifty50DailyTableTest_SF['updatedOn'].any()):
    print("-----------There are missing and invaild date values in updated column Data")
else:
    # extract the date form "updated date" and convert the column to data frame
    nifty50DailyTableTest_SF['updatedOn'] = pd.to_datetime(nifty50DailyTableTest_SF["updatedOn"].dt.date)


# remove the value "cr" from the colum
if 'totalTradedValueCr' not in nifty50DailyTableTest_SF.columns:
    nifty50DailyTableTest_SF = nifty50DailyTableTest_SF.rename(columns={'change': 'Updated_change'})

    # Convert to numeric and handle 'Cr.'
    nifty50DailyTableTest_SF['totalTradedValueCr'] = pd.to_numeric(nifty50DailyTableTest_SF['totalTradedValue'].str.replace(',', '').str.replace(' Cr.', '', regex=True), errors='coerce')  
    print("-----------------",nifty50DailyTableTest_SF['totalTradedValueCr'])
     # Convert to numeric and handle 'Lakh'
    nifty50DailyTableTest_SF['totalTradedQuantityLakh'] = pd.to_numeric(nifty50DailyTableTest_SF['totalTradedQuantity'].str.replace(',', '').str.replace(' Lakh', '', regex=True), errors='coerce') 
    print("-----------------",nifty50DailyTableTest_SF['totalTradedQuantityLakh'])
    # Convert to numeric and handle 'Lakh'
    nifty50DailyTableTest_SF['twoWeekAvgQuantityLakh'] = pd.to_numeric(nifty50DailyTableTest_SF['twoWeekAvgQuantity'].str.replace(',', '').str.replace(' Lakh', '', regex=True), errors='coerce')  
    print("-----------------",nifty50DailyTableTest_SF['twoWeekAvgQuantityLakh'])
    # Convert to numeric and handle 'Cr.'
    nifty50DailyTableTest_SF['marketCapFullCr'] = pd.to_numeric(nifty50DailyTableTest_SF['marketCapFull'].str.replace(',', '').str.replace(' Cr.', '', regex=True), errors='coerce')  
    print("-----------------",nifty50DailyTableTest_SF['marketCapFullCr'])
    # Convert to numeric and handle 'Cr.'
    nifty50DailyTableTest_SF['marketCapFreeFloatCr'] = pd.to_numeric(nifty50DailyTableTest_SF['marketCapFreeFloat'].str.replace(',', '').str.replace(' Cr.', '', regex=True), errors='coerce') 
    print("-----------------",nifty50DailyTableTest_SF['marketCapFreeFloatCr'])
    # Drop original columms
    columns_to_drop = ['totalTradedValue', 'totalTradedQuantity', 'twoWeekAvgQuantity', 'marketCapFull', 'marketCapFreeFloat']
    columns_to_drop = [col for col in columns_to_drop if col in nifty50DailyTableTest_SF.columns]
    
    nifty50DailyTableTest_SF.drop(columns_to_drop, axis=1, inplace=True)


    print("----------nifty50DailyTableTest_SF-------",nifty50DailyTableTest_SF.head(5))

# Connect to the my sql Data base with the help of sqlAlchamy
# DEFINE THE DATABASE CREDENTIALS
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
database = os.getenv('DB_NAME')

# CREATE A CONNECTION OBJECT
engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")
print("----------engine------",engine)
try:
    # tring to coonect my sql db with with above engline help of sqlAlchamy
    with engine.connect() as conn:
        print('connection successful')
        inspector = inspect(engine)
        nifty50_table_name  ='nifty50_dailydata'

        # check if table exists
        if not inspector.has_table(nifty50_table_name):
            print("--------------going to this first if condition-------------")
            nifty50_table_name_schema = text(f'''
            CREATE TABLE {nifty50_table_name} (
            companyName VARCHAR(255),
            currentValue FLOAT,
            Updated_change FLOAT,  
            pChange FLOAT,
            updatedOn DATE,
            securityID VARCHAR(255),
            scripCode VARCHAR(255),
            sharegroup VARCHAR(255),
            faceValue FLOAT,
            industry VARCHAR(255),
            previousClose FLOAT,
            previousOpen FLOAT,
            dayHigh FLOAT,
            dayLow FLOAT,
            fiftytwoweekHigh FLOAT,
            fiftytwoweekLow FLOAT,
            weightedAvgPrice FLOAT,
            totalTradedQuantityLakh FLOAT,
            totalTradedValueCr FLOAT,
            twoWeekAvgQuantityLakh FLOAT,
            marketCapFullCr FLOAT,
            marketCapFreeFloatCr FLOAT
            );
            ''')
        
            conn.execute(nifty50_table_name_schema)
            print(f"-------------table is crreated scuuessfull-------------")
        else:
            print(f"-------------table {nifty50_table_name} already exists-------------")

    # Inserting the data in the table
    with engine.begin() as engineconn:
        sql_max_updateOn = pd.read_sql_query(db.text(f'select max(updatedOn) from {nifty50_table_name}'),engineconn).iloc[0,0]
        df_max_updateOn = nifty50DailyTableTest_SF['updatedOn'].max()
        if (pd.isnull(sql_max_updateOn) or (not pd.isnull(df_max_updateOn))):
            nifty50DailyTableTest_SF.to_sql(nifty50_table_name, engineconn,index=False, if_exists='append', method='multi')
            print(f"-------------Daily Data didn't exist, but now inserted successfully.-------------")
        else:
            if(df_max_updateOn> pd.Timestamp(sql_max_updateOn)):
                nifty50DailyTableTest_SF.to_sql(nifty50_table_name, engineconn,index=False, if_exists='append', method='multi')
                print("-------Data append successfullly----------")
            else:
                print("----------No new data append-------")

    with engine.connect() as conn:
        comapny_table_name = 'nifty50_companydata'
        if not inspector.has_table(comapny_table_name):
            print("--------------going to this comapny_table_name if condition-------------")
            #  Define the table schema based on the 'Equity' DataFrame columns
            company_table_schema = text(f''' 
                CREATE TABLE {comapny_table_name} (
                securityCode VARCHAR(255),
                issuerName VARCHAR(255),
                securityId VARCHAR(255),
                securityName VARCHAR(255),
                status VARCHAR(255),
                CompanyGroup VARCHAR(255),
                faceValue FLOAT,
                isinNo VARCHAR(255),
                industry VARCHAR(255),
                instrument VARCHAR(255),
                sectorName VARCHAR(255),
                industryNewName VARCHAR(255),
                igroupName VARCHAR(255),
                iSubgroupName VARCHAR(255)
            );
            ''')
            print("----------table schema-------------------",company_table_schema)

        # Execute the schema for comapny Table
            conn.execute(company_table_schema)
            print("-------------engine connection --------------------",conn)
            print(f"-------------table is created scuuessfull:::::))))-------------")
        else:
            print(f"-------------table {comapny_table_name} already exists-------------")

    # Check and print the content of nifty50_SQDF
    print("Data to be inserted into 'nifty50_companydata':")
    print(nifty50_SQDF.head())

    # Insert company records
    with engine.begin() as engineconn:
        nifty50_SQDF.to_sql(comapny_table_name, engineconn, index=False, if_exists='append', method='multi')
        print("Data inserted into 'nifty50_companydata' table successfully.")

except Exception as e:
    print(f'------------------sql Alchamy connection error-----------------',e)

finally:
    print("-------------connection closed-------------")
    engine.dispose()









    

