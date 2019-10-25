FROM ubuntu:latest
COPY target/ target/
EXPOSE 8082
RUN apt-get update
RUN apt-get install openjdk-8-jdk
RUN apt-get install curl
ENTRYPOINT ["java", "-jar","target/backend-java-0.0.1-SNAPSHOT.jar"]