# Build context is the repo root, so proto/ is visible to the Gradle build.
FROM eclipse-temurin:25-jdk AS build
WORKDIR /src/coordinator
COPY coordinator/gradlew coordinator/settings.gradle.kts coordinator/build.gradle.kts ./
COPY coordinator/gradle gradle
RUN --mount=type=cache,target=/root/.gradle ./gradlew --no-daemon -q dependencies > /dev/null
COPY proto /src/proto
COPY coordinator/src src
RUN --mount=type=cache,target=/root/.gradle ./gradlew --no-daemon -q bootJar

FROM eclipse-temurin:25-jre
WORKDIR /app
# Datadog's Java agent, pinned by SHA-256 (checked against the SHA-1 Maven
# Central publishes for it). The image always carries it; only
# docker-compose.prod.yml attaches it, with -javaagent in JAVA_TOOL_OPTIONS.
ADD --checksum=sha256:beb620dc2e4783fd328a1ae23f8fd4fcb757ac23e5bb05efb04c6aa3e51a5e43 \
    https://repo1.maven.org/maven2/com/datadoghq/dd-java-agent/1.67.1/dd-java-agent-1.67.1.jar \
    /app/dd-java-agent.jar
COPY --from=build /src/coordinator/build/libs/coordinator.jar coordinator.jar
EXPOSE 9090
ENTRYPOINT ["java", "-jar", "/app/coordinator.jar"]
