FROM maven:3.9.8-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn -q dependency:go-offline
COPY src ./src
RUN mvn -q clean package -DskipTests
# Unpack the fat jar into a thin jar + lib/ on the plain classpath. Flink's embedded
# MiniCluster resolves job classes through the system classloader, which cannot see classes
# nested inside a Spring Boot fat jar (ClassNotFoundException: ...ExecutionConfig).
RUN java -Djarmode=tools -jar target/flinklab-*.jar extract --destination target/extracted

FROM eclipse-temurin:17-jre
WORKDIR /app
RUN mkdir -p /data/flink-fs/inbox /data/flink-fs/outbox /data/flink-fs/checkpoints
COPY --from=build /app/target/extracted/ ./
EXPOSE 8080 8082
# Flink's type serializers and Kryo reflect into JDK internals, which Java 17 closes by default
ENTRYPOINT ["java", \
  "--add-opens", "java.base/java.util=ALL-UNNAMED", \
  "--add-opens", "java.base/java.lang=ALL-UNNAMED", \
  "--add-opens", "java.base/java.lang.reflect=ALL-UNNAMED", \
  "--add-opens", "java.base/java.time=ALL-UNNAMED", \
  "--add-opens", "java.base/java.nio=ALL-UNNAMED", \
  "--add-opens", "java.base/java.net=ALL-UNNAMED", \
  "--add-opens", "java.base/java.io=ALL-UNNAMED", \
  "--add-opens", "java.base/java.util.concurrent=ALL-UNNAMED", \
  "-jar", "flinklab-0.0.1-SNAPSHOT.jar"]
