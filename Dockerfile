FROM maven:3.9.9-amazoncorretto-23-alpine AS builder

WORKDIR /app

COPY src ./src

COPY pom.xml .

RUN mvn dependency:go-offline -B

RUN mvn clean package -DskipTests -DfinalName=keycloak-provider-kafka

FROM quay.io/keycloak/keycloak:26.1

COPY --from=builder /app/target/keycloak-provider-kafka-*-jar-with-dependencies.jar /opt/keycloak/providers/keycloak-provider-kafka.jar

ENTRYPOINT ["/opt/keycloak/bin/kc.sh"]
