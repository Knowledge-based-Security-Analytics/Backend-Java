FROM openjdk:11-jre-slim
EXPOSE 8080
ENTRYPOINT ["java", "-jar","target/backend-java-0.0.1-SNAPSHOT.jar"]