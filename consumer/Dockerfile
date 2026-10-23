FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY consumer/build/libs/consumer-1.0.0-all.jar /app/app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
