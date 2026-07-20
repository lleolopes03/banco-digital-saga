# syntax=docker/dockerfile:1
FROM eclipse-temurin:21-jdk AS build
WORKDIR /app
COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
RUN chmod +x mvnw && ./mvnw -q -B dependency:go-offline
COPY src ./src
RUN ./mvnw -q -B package -DskipTests

# Classifier "exec" (definido no pom) é o fat jar com todas as dependências embutidas — o jar
# sem classifier é "thin" e só serve como dependência de biblioteca (usado pelo módulo e2e-tests).
FROM eclipse-temurin:21-jre AS runtime
WORKDIR /app
RUN useradd --system --create-home appuser
COPY --from=build /app/target/*-exec.jar app.jar
USER appuser
EXPOSE 8082
ENTRYPOINT ["java", "-jar", "app.jar"]
