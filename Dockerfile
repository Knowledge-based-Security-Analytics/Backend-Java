FROM openjdk:latest
COPY target/ target/
EXPOSE 8082
#RUN apt-get update
#RUN apt-get -y install openjdk-8-jdk
#RUN apt-get -y install curl
ENTRYPOINT ["java", "-jar","target/backend-java-0.0.1-SNAPSHOT.jar"]