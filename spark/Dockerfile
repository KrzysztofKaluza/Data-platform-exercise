FROM maven:3.9.6-eclipse-temurin-17 AS builder

WORKDIR /build

COPY pom.xml .

RUN mvn dependency:copy-dependencies

FROM apache/spark:3.5.1

USER root

COPY --from=builder /build/target/dependency/*.jar /opt/spark/jars/

USER spark