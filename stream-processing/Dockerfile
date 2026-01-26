FROM maven:3.9.9-eclipse-temurin-21-alpine AS build

COPY pom.xml /usr/pom.xml
RUN mvn -f /usr/pom.xml clean package

COPY . /usr/
RUN mvn -f /usr/pom.xml clean package -o

FROM eclipse-temurin:21-jre-alpine
COPY --from=build /usr/target /usr
CMD ["java", "-cp", "/usr/kafka-streams-app-1.0-SNAPSHOT.jar:/usr/target/libraries/*", "com.flight_analytics.SimpleStreamProcessor"]
