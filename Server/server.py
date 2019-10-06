import os
import ast
import time
import psycopg2
from kafka import KafkaConsumer
from kafka import KafkaProducer

trending = KafkaConsumer('trending',group_id='web-trending',bootstrap_servers=['localhost:9092'])
metric = KafkaConsumer('monitor',group_id='web-metric',bootstrap_servers=['localhost:9092'])
delays = KafkaProducer(bootstrap_servers=['localhost:9092'])

def updateTrend():
    msg = trending.poll(100,10)
    if len(msg) == 0:
        return ""
    content = msg[list(msg)[0]]
    trendsList = []
    for i in content:
        s = i.value.decode()
        trends = s.split(";")
        for t in trends:
            if t == '':
                continue
            symbolPrice = t.split(",",1)
            l = ast.literal_eval(symbolPrice[1])
            l.append(symbolPrice[0])
        trendsList.append(l)
    trendsList.sort(key=lambda x:x[0],reverse=True)
    filterTrends = set()
    newTrends = {}
    count = 1
    for i in trendsList:
        if i[-1] in filterTrends:
            continue
        filterTrends.add(i[-1])
        newTrends[i[-1]] = [i[-1],i[0],i[-2]]
        count += 1
        if count > 5:
            break
    msg = ""
    if len(newTrends) > 0:
        msg = "trending@" + str(newTrends) + "\n"
    return msg

def elastic():
    cmd = "nohup ./elastic.sh &"
    os.system(cmd)

peak_v = 10000

def updateMetric():
    global peak_v
    msg = metric.poll(100,10)
    if len(msg) == 0:
        return ""
    content = msg[list(msg)[0]]
    volume = 0
    delayedList = {}
    for i in content:
        k = i.key.decode()
        s = i.value.decode()
        if k == "volume":
            volume += int(s)
        elif k == "delay":
            delaySplit = s.split(";")
            for t in delaySplit:
                if t == '':
                    continue
                symbolPrice = t.split(",",1)
                delayedList[symbolPrice[0]] = symbolPrice[1]
    msg = ""
    if volume > peak_v:
        elastic()
        peak_v = peak_v + 10000
    if volume > 0:
        msg += "volume@" + str(volume) + "\n"
    if len(delayedList) > 0:
        msg += "delayed@" + str(delayedList) + "\n"
    else:
        msg += "delayed@None\n"
    return msg

def sendDelay():
    k = "[SYM]"
    v = "90.0@2019-10-01 00:00:00\n"
    future = delays.send('order', key=k.encode(),value=v.encode())
    future.get(timeout=100)

if __name__ == "__main__":
    conn = psycopg2.connect("dbname=app user=postgres")
    cur = conn.cursor()
    while 1:
        start = time.time()
        #sendDelay()
        msg = updateTrend()
        msg += updateMetric()
        print("msg: \n" + msg)
        if msg != "":
            cur.execute("INSERT INTO buffer (msg) VALUES (%s);",(msg,))
            conn.commit()
        end = time.time()
        time.sleep(5-min(end-start,5))