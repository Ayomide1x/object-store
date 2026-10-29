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
COPY --from=build /src/coordinator/build/libs/coordinator.jar coordinator.jar
EXPOSE 9090
ENTRYPOINT ["java", "-jar", "/app/coordinator.jar"]
