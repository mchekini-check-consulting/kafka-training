# Dockerfile multi-stage unique pour les 6 services :
#   docker build --build-arg SERVICE=order-service -t order-service .
# Le stage "build" ne référence pas SERVICE : il est donc identique pour les
# 6 images et BuildKit ne le construit qu'une seule fois (cache partagé).
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY . .
RUN mvn -q package -DskipTests

FROM eclipse-temurin:21-jre
ARG SERVICE
WORKDIR /app
COPY --from=build /build/${SERVICE}/target/${SERVICE}-*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
