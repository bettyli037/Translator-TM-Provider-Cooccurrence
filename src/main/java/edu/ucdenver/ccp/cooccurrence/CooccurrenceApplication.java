package edu.ucdenver.ccp.cooccurrence;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;

@SpringBootApplication
@EnableCaching
public class CooccurrenceApplication {

	public static void main(String[] args) {
		SpringApplication.run(CooccurrenceApplication.class, args);
	}

}
