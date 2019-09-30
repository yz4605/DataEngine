import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as func
import pyspark.sql.types as types

def pandaReadFile(fileName):
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

def percentByDay(df):
    from pyspark.sql.window import Window
    windowspec = Window.orderBy("Date")
    df = df.withColumn("Prev", func.lag(df.Open).over(windowspec))
    df = df.withColumn("Diff", func.when(func.isnull(df.Open - df.Prev), 0).otherwise(df.High - df.Prev))
    df = df.withColumn("percent", df.Diff/df.Open)
    return df

def idx_sym(hashMap):
    def func(data):
        return hashMap[data]
    return func

def maxCol(df):
    maxVal = func.udf(lambda x: max(x), types.DoubleType())
    maxIdx = func.udf(lambda x: x.index(x[-1]), types.IntegerType())
    df = df.withColumn("maxVal",maxVal(func.array(df.columns[1:])))
    df = df.withColumn("maxIdx",maxIdx(func.array(df.columns[1:])))
    maxSym = func.udf(idx_sym(df.columns[1:]), types.StringType())
    df = df.withColumn("maxSym",maxSym("maxIdx"))
    return df

def dataFactory(spark,path,sym):
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

def writeHistorical(spark,stock_sector):
    for i in stock_sector:
        s = i.upper()
        df = dataFactory(spark,"data/"+s+".csv",s)
        df.write.csv('new/'+s,header=True,timestampFormat="yyyy-MM-dd HH:mm:ss")

def sectorWrap(data):
    def func(arg):
        return arg.upper() in data
    return func

def filterStock(df,sectorSet):
    sector = func.udf(sectorWrap(sectorSet),types.BooleanType())
    df_filter = df.filter(sector(df.symbol))
    return df_filter

def getMax(df):
    df_max = df.groupBy("timestamp").max("percent")
    df_max = df_max.withColumnRenamed("max(percent)","percent")
    df_max = df.join(df_max,["timestamp","percent"])
    return df_max

def getAvg(df):
    df_avg = df.groupBy("timestamp").avg("percent")
    df_avg = df_avg.withColumnRenamed("avg(percent)","percent")
    return df_avg

def getVol(df):
    df_std = df.agg(func.stddev_samp(df.percent))
    df_std = df_std.withColumnRenamed("stddev_samp(percent)","volatility")
    return df_std

def getTop(df):
    df_count = df.groupBy("symbol").count()
    top = df_count.groupBy().max("count").collect()[0][0]
    df_top = df_count.filter(df_count["count"] == top)
    return df_top

def process(df,sector):
    df = filterStock(df,sector)
    df_max = getMax(df)
    top = getTop(df_max).collect()[0][0]
    df_avg = getAvg(df)
    vol = getVol(df_avg).collect()[0][0]
    return top,vol

def main():
    import ast
    import configparser
    config = configparser.ConfigParser()
    config.read('spark.ini')
    spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3a.access.key', config['s3']['fs.s3a.access.key'])
    spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3a.secret.key',config['s3']['fs.s3a.secret.key'])
    # spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3a.impl','org.apache.hadoop.fs.s3a.S3AFileSystem')
    # stock_sector = config['stock_sector']
    # writeHistorical(spark,stock_sector)
    sector_stock = config['sector_stock']
    sectors = {}
    for i in sector_stock:
        sectors[i] = ast.literal_eval(sector_stock[i])
    df = spark.read.csv(path="data/*/*.csv", header=True, schema="timestamp TIMESTAMP, symbol STRING, percent DOUBLE", timestampFormat="yyyy-MM-dd HH:mm:ss")
    df = df.na.drop()
    for i in sectors:
        top,vol = process(df,sectors[i])
        print("Top: "+str(top)+" Volatility: "+str(vol))

    spark.stop()
    
main()

#export PYSPARK_PYTHON=python3
#spark/bin/spark-submit --master local[*] --driver-memory 4g --executor-memory 4g run.py