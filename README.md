# Translator-TM-Provider-Cooccurrence

TRAPI endpoint for text mined cooccurrence metrics.

## How to Run

This application is packaged as a jar which has Tomcat embedded. No Tomcat or JBoss installation is necessary. You run it using the ```java -jar``` command.

* Clone this repository
* Make sure you are using JDK 11 and Maven 3.x
* Edit the ```spring.datasource.*``` values in ```src/resources/application.properties```  as needed, or set environment variables for ```SPRING_DATASOURCE_USERNAME```, ```_PASSWORD```, and ```_URL``` 
* Build the project and run the tests by running ```mvn clean install```
* Or build the project without running tests with ```mvn clean install -DskipTests```
* Once successfully built, you can run the service by one of these two methods:
```
        java -jar target/cooccurrence-0.1.0.jar
or
        mvn spring-boot:run
```