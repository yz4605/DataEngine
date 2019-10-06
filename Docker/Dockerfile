FROM ubuntu
COPY . /
RUN apt update
RUN apt install -y openjdk-11-jre
ENTRYPOINT ["java", "-jar", "stream.jar"]