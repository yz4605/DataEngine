# Elastic Trends Spot

Structure:  
- Stream -- Kafka streaming application with gradle file  
- Batch -- Spark program to clean the data and run the analysis  
- Server -- Include web, server and database  
- Docker -- Dockerfile to build the container  

# Command:
```sh
/Server
$ python3 server.py
#set up kafka consumer and back end server for website
$ python3 app.py
#start web service

/Stream
#main class in App.java

/Batch
$ export PYSPARK_PYTHON=python3
$ spark-submit run.py

/Docker
$ docker build .
```

http://localhost/ Access webpage here. The web service is stateless and all the data is processed in back end.
