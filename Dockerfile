FROM openjdk:11-jre-slim
COPY target/ target/
EXPOSE 8080
ENTRYPOINT ["java", "-jar","target/backend-java-0.0.1-SNAPSHOT.jar"]