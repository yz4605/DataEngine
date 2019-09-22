from pyspark.sql import SparkSession
#from pyspark.sql.window import Window
import pyspark.sql.functions as func
import pandas as pd

def getRela(fileName):
    stocks = pd.read_csv(fileName)
    stock_sector = pd.Series(stocks.Sector.array,index=stocks.Symbol).to_dict()
    sector_stock = {}
    for i in zip(stocks["Symbol"],stocks["Sector"]):
        if i[1] in sector_stock:
            sector_stock[i[1]].append(i[0])
        else:
            sector_stock[i[1]] = [i[0]]
    return stock_sector,sector_stock

def getPer(sym):
    df = spark.read.csv('data/'+sym+'.csv',header=True)
    df = df.withColumn("Date", df["Date"].cast("date"))
    df = df.withColumn("Open", df["Open"].cast("float"))
    df = df.withColumn("Close", df["Close"].cast("float"))
    df = df.withColumn("Diff", df["Close"]-df["Open"])
    df = df.withColumn("Percent", df["Diff"]/df["Open"])
    # windowspec = Window.orderBy("Date")
    # df = df.withColumn("Prev", func.lag(df.Open).over(windowspec))
    # df = df.withColumn("Diff", func.when(func.isnull(df.Open - df.Prev), 0).otherwise(df.High - df.Prev))
    # df = df.withColumn("Percent", df.Diff/df.Open)
    df = df.select("Date","Percent")
    df = df.withColumn("Percent",func.round(df["Percent"],5))
    df = df.withColumnRenamed("Percent",sym)
    df = df.sort("Date")
    return df

def joinStock(sector_stock,sector):
    dflist = []
    for i in sector_stock[sector]:
        dflist.append(getPer(i))
    df = dflist.pop(0)
    for i in dflist:
        df = df.join(i,"Date","outer")
    return df

stock_sector,sector_stock = getRela("stock.csv")

spark = SparkSession.builder.appName("SimpleApp").getOrCreate()

for i in sector_stock:
    df = joinStock(sector_stock,i)
    print("Show\n")
    print(df.show(n=1))
    print("Done\n")

spark.stop()

#export PYSPARK_PYTHON=python3
#spark/bin/spark-submit --master local[*] --driver-memory 8g --executor-memory 4g run.py