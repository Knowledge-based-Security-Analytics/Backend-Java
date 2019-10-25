FROM ubuntu:latest
COPY target/ target/
EXPOSE 8080
ENTRYPOINT ["bash"]
#ENTRYPOINT ["java", "-jar","target/backend-java-0.0.1-SNAPSHOT.jar"]