import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as func
import pyspark.sql.types as types

def pandaReadFile(fileName):
    #get stock information
    import pandas as pd
    stocks = pd.read_csv(fileName)
    stock_sector = pd.Series(stocks.Sector.array,index=stocks.Symbol).to_dict()
    sector_stock = {}
    for i in zip(stocks["Symbol"],stocks["Sector"]):
        if i[1] in sector_stock:
            sector_stock[i[1]].add(i[0])
        else:
            sector_stock[i[1]] = {i[0]}
    return stock_sector,sector_stock

def dataFactory(spark,path,sym):
    #reformat historical finance data
    df = spark.read.csv(path,header=True)
    df = df.withColumn("date",func.concat(df.Date,func.lit(" 00:00:00")))
    df = df.withColumn("timestamp", df["date"].cast("timestamp"))
    df = df.withColumn("Open", df["Open"].cast("double"))
    df = df.withColumn("Close", df["Close"].cast("double"))
    df = df.withColumn("Diff", df["Close"]-df["Open"])
    df = df.withColumn("percent", df["Diff"]/df["Open"])
    df = df.withColumn("percent", df["percent"]*100+100)
    df = df.withColumn("percent",func.round(df["percent"],5))
    df = df.withColumn("symbol",func.lit(sym))
    df = df.select("timestamp","symbol","percent")
    df = df.sort("timestamp")
    return df

def writeHistorical(spark,stock_sector,config):
    #save data
    for i in stock_sector:
        try:
            s = i.upper()
            df = dataFactory(spark,config['path']['s3']+"/data/"+s+".csv",s)
            df.write.csv(config['path']['s3']+'/spark/'+s,header=True,timestampFormat="yyyy-MM-dd HH:mm:ss")
        except:
            print(s)

def main():
    import ast
    import configparser
    config = configparser.ConfigParser()
    config.read('config.ini')
    spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3a.access.key', config['s3']['fs.s3a.access.key'])
    spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3a.secret.key',config['s3']['fs.s3a.secret.key'])
    # spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3a.impl','org.apache.hadoop.fs.s3a.S3AFileSystem')
    stock_sector = config['stock_sector']
    writeHistorical(spark,stock_sector,config)

    spark.stop()
    

if __name__ == "__main__":
    main()
