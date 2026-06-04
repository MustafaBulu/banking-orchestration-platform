FROM eclipse-temurin:21-jre

WORKDIR /app

ARG JAR_FILE=target/*.jar
COPY ${JAR_FILE} app.jar

EXPOSE 8085
EXPOSE 9094

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
