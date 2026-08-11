# ==========================================
# ETAP 1: Pobieranie i walidacja zależności przez Maven
# ==========================================
FROM maven:3.9.6-eclipse-temurin-17 AS builder

WORKDIR /build

COPY pom.xml .

# Pobieramy zależności i kopiujemy je jawnie do target/dependency
RUN mvn dependency:copy-dependencies

# ==========================================
# ETAP 2: Obraz produkcyjny PySpark 3.5.1
# ==========================================
FROM apache/spark:3.5.1

USER root

# Kopiowanie pobranych bibliotek bezpośrednio do Sparka
COPY --from=builder /build/target/dependency/*.jar /opt/spark/jars/

USER spark