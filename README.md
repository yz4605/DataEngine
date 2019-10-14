# Elastic Trends Spot
An auto scalable data pipeline to spot the trends from streaming financial market data. Leveraged Kafka streams library to process the real time data, rank trending stocks and catch delayed messages. Analyzed daily market volatility with Spark from S3 and stored results in TimescaleDB for visualization through Dash.


Structure:  
- Stream -- Kafka streaming application with gradle file  
- Batch -- Spark program to clean the data and run the analysis  
- Server -- Include web, server and database  
- Docker -- Dockerfile to build the container  

![alt text](https://github.com/yz4605/DataEngine/raw/master/Data%20Pipeline.png)

# Command:
```sh
/Server
$ python3 server.py
#set up kafka consumer and back end server for website
$ python3 app.py
#start web service

/Stream
#compile with main class in App.java
$ java -jar stream.jar

/Batch
$ export PYSPARK_PYTHON=python3
$ spark-submit run.py

/Docker
$ docker build .
```

http://localhost/ Access webpage here. The web service is stateless and all the data is processed in back end.
