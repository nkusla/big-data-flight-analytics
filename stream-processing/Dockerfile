FROM maven:3.9.9-eclipse-temurin-21-alpine AS build

COPY pom.xml /usr/pom.xml
RUN mvn -f /usr/pom.xml clean package

COPY . /usr/
RUN mvn -f /usr/pom.xml clean package -o

FROM eclipse-temurin:21-jre-alpine
RUN apk add --no-cache libstdc++
COPY --from=build /usr/target/kafka-streams-app-1.0-SNAPSHOT.jar /app/
COPY --from=build /usr/target/libraries /app/libraries
CMD ["java", "-cp", "/app/kafka-streams-app-1.0-SNAPSHOT.jar:/app/libraries/*", "com.flight_analytics.StreamProcessor"]
