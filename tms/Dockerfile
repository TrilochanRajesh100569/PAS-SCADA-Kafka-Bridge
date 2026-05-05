# ── Stage 1: Build ───────────────────────────────────────────────────────────
FROM eclipse-temurin:17-jdk-alpine AS build
WORKDIR /build

# Cache Maven dependencies before copying source
COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2 \
    mvn dependency:go-offline -B 2>/dev/null || true

# Copy source and build the fat JAR (skip tests — run them separately in CI)
COPY src ./src
RUN --mount=type=cache,target=/root/.m2 \
    apk add --no-cache maven && \
    mvn package -DskipTests -B && \
    mv target/PAS-SCADA-Kafka-Bridge-*.jar target/app.jar

# ── Stage 2: Runtime ─────────────────────────────────────────────────────────
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app

# Non-root user for security
RUN addgroup -S bridge && adduser -S bridge -G bridge
USER bridge

COPY --from=build /build/target/app.jar app.jar

EXPOSE 8085

ENTRYPOINT ["java", "-jar", "app.jar"]
