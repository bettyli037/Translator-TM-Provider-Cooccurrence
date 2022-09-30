FROM maven:3.8.6-jdk-11 AS build
COPY src /home/app/src
COPY pom.xml /home/app/
RUN mvn -f /home/app/pom.xml clean install -DskipTests

FROM amazoncorretto:11
COPY --from=build /home/app/target/*.jar app.jar
ENTRYPOINT ["java","-Xmx16G","-jar","/app.jar"]