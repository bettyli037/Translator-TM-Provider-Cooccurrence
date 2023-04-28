FROM maven:3.8.6-jdk-11 AS build
COPY src /home/app/src
COPY pom.xml /home/app/
RUN mvn -f /home/app/pom.xml clean install -DskipTests

FROM amazoncorretto:11
COPY --from=build /home/app/target/*.jar app.jar
# Download and install the SonarQube scanner
RUN apt-get update && apt-get install -y unzip \
    && curl -L -o sonar-scanner-cli.zip https://binaries.sonarsource.com/Distribution/sonar-scanner-cli/sonar-scanner-cli-4.5.0.2216-linux.zip \
    && unzip sonar-scanner-cli.zip \
    && rm sonar-scanner-cli.zip \
    && mv sonar-scanner-* /opt/sonar-scanner \
    && ln -s /opt/sonar-scanner/bin/sonar-scanner /usr/bin/sonar-scanner

# Set environment variables for the SonarQube scanner
ENV SONAR_HOST_URL=https://my-sonarqube-server.com
ENV SONAR_LOGIN=my-sonarqube-login
ENV SONAR_PASSWORD=my-sonarqube-password
ENV SONAR_PROJECT_KEY=my-project-key

# Run the SonarQube analysis
RUN sonar-scanner
ENTRYPOINT ["java","-Xmx16G","-jar","/app.jar"]