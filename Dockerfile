FROM maven:3.8.6-jdk-11 AS build
COPY src /home/app/src
COPY pom.xml /home/app/
RUN mvn -f /home/app/pom.xml clean install -DskipTests

FROM amazoncorretto:11
COPY --from=build /home/app/target/*.jar app.jar
# Download and install the SonarQube scanner
# Install necessary packages for the SonarQube scanner
RUN yum -y update && yum -y install unzip \
    && curl -L -o sonar-scanner-cli.zip https://binaries.sonarsource.com/Distribution/sonar-scanner-cli/sonar-scanner-cli-4.5.0.2216-linux.zip \
    && unzip sonar-scanner-cli.zip \
    && rm sonar-scanner-cli.zip \
    && mv sonar-scanner-* /opt/sonar-scanner \
    && ln -s /opt/sonar-scanner/bin/sonar-scanner /usr/bin/sonar-scanner

# Set environment variables for the SonarQube scanner
ENV SONAR_HOST_URL=http://sonarqube-bl-1456515170.us-east-1.elb.amazonaws.com
#ENV SONAR_LOGIN=sqp_b5130e5a80c13f615ca5244f4c1bdbfc7e4235e3
ENV SONAR_PASSWORD=my-sonarqubepassword
#ENV SONAR_PROJECT_KEY=TMKP-Cooccurrence

# Run the SonarQube analysis
RUN sonar-scanner -Dsonar.login=sqp_b5130e5a80c13f615ca5244f4c1bdbfc7e4235e3 -Dsonar.projectKey=TMKP-Cooccurrence
ENTRYPOINT ["java","-Xmx16G","-jar","/app.jar"]