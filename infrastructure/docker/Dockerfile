FROM maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /workspace
ARG SERVICE
COPY . .
RUN --mount=type=cache,target=/root/.m2,sharing=locked mvn -B -ntp -pl "${SERVICE}" -am package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
ARG SERVICE
COPY --from=build "/workspace/${SERVICE}/target/${SERVICE}-0.1.0-SNAPSHOT.jar" app.jar
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 eventflow
USER 10001
EXPOSE 8080
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-XX:+ExitOnOutOfMemoryError", "-jar", "/app/app.jar"]
