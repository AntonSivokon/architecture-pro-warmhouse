FROM maven:3.9.9-eclipse-temurin-17-alpine AS build

WORKDIR /app
COPY pom.xml .
RUN mvn --batch-mode --no-transfer-progress dependency:go-offline

COPY src ./src
RUN mvn --batch-mode --no-transfer-progress -DskipTests package

FROM eclipse-temurin:17-jre-alpine

RUN apk add --no-cache libstdc++

WORKDIR /app
COPY --from=build /app/target/telemetry-service-1.0.0.jar app.jar
EXPOSE 8081
USER 10001
ENTRYPOINT ["java", "-jar", "app.jar"]