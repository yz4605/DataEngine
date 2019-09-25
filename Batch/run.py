import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as func
import pyspark.sql.types as types
import pandas as pd

def getMap(fileName):
    stocks = pd.read_csv(fileName)
    stock_sector = pd.Series(stocks.Sector.array,index=stocks.Symbol).to_dict()
    sector_stock = {}
    for i in zip(stocks["Symbol"],stocks["Sector"]):
        if i[1] in sector_stock:
            sector_stock[i[1]].append(i[0])
        else:
            sector_stock[i[1]] = [i[0]]
    return stock_sector,sector_stock

def getPercent(sym):
    df = spark.read.csv('data/'+sym+'.csv',header=True)
    df = df.withColumn("Date",func.concat(df.Date,func.lit(" 00:00:00")))
    df = df.withColumn("date", df["Date"].cast("timestamp"))
    df = df.withColumn("Open", df["Open"].cast("float"))
    df = df.withColumn("Close", df["Close"].cast("float"))
    df = df.withColumn("Diff", df["Close"]-df["Open"])
    df = df.withColumn("percent", df["Diff"]/df["Open"])
    df = df.withColumn("percent", df["percent"]*100+100)
    df = df.withColumn("percent",func.round(df["percent"],5))
    df = df.withColumn("symbol",func.lit(sym))
    df = df.select("date","symbol","percent")
    df = df.sort("date")
    return df

def percentByDay(df):
    from pyspark.sql.window import Window
    windowspec = Window.orderBy("Date")
    df = df.withColumn("Prev", func.lag(df.Open).over(windowspec))
    df = df.withColumn("Diff", func.when(func.isnull(df.Open - df.Prev), 0).otherwise(df.High - df.Prev))
    df = df.withColumn("Percent", df.Diff/df.Open)
    return df

def joinStock(sector_stock,sector):
    dflist = []
    for i in sector_stock[sector]:
        dflist.append(getPercent(i))
    df = dflist.pop(0)
    for i in dflist:
        df = df.join(i,"Date","outer")
    return df

def idx_sym(f):
    def func(data):
        return f[data]
    return func

def getMax(df):
    maxVal = func.udf(lambda x: max(x), types.DoubleType())
    maxIdx = func.udf(lambda x: x.index(x[-1]), types.IntegerType())
    df = df.withColumn("maxVal",maxVal(func.array(df.columns[1:])))
    df = df.withColumn("maxIdx",maxIdx(func.array(df.columns[1:])))
    maxSym = func.udf(idx_sym(df.columns[1:]), types.StringType())
    df = df.withColumn("maxSym",maxSym("maxIdx"))
    return df

def readCurrent(p):
    df = spark.read.csv(path=p, schema="timestamp TIMESTAMP, symbol STRING, percent FLOAT", timestampFormat="yyyy-MM-dd HH:mm:ss")
    return df

def sectorWrap(data):
    def func(arg):
        return arg in data
    return func

def filterStock(sectorSet)
    sector = func.udf(sectorWrap(sectorSet),types.BooleanType())
    df_filter = df.filter(sector(df.symbol))

def getTop(df):
    df_max = df.groupBy("timestamp").max("percent")
    df_max = df_max.withColumnRenamed("max(percent)","percent")
    df_top = df.join(df_max,["timestamp","percent"])

def getAvg(df):
    df_avg = df.groupBy("timestamp").avg("percent")
    df_avg = df_avg.withColumnRenamed("avg(percent)","percent")


# def m(line):
#     s = line.split(',')
#     return (s[0],s[1]+','+s[2])

# sc = spark.sparkContext
# text_file = sc.textFile('price')
# counts = text_file.map(m).reduceByKey(lambda a, b: a +';'+ b)
# counts.sortByKey()

stock_sector,sector_stock = getMap("stock.csv")
spark = SparkSession.builder.appName("SimpleApp").getOrCreate()

for i in sector_stock:
    df = joinStock(sector_stock,i)
    print("Show\n")
    df = getMax(df)
    print(df.show(n=10))
    print("Done\n")

# df = joinStock(sector_stock,'Telecommunication Services')
# df = getMax(df)
# print(df.show(n=100))


spark.stop()
#export PYSPARK_PYTHON=python3
#spark/bin/spark-submit --master local[*] --driver-memory 8g --executor-memory 8g run.py