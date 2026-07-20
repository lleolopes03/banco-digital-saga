package com.bancodigital.antifraud;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class AntifraudApplication {

	public static void main(String[] args) {
		SpringApplication.run(AntifraudApplication.class, args);
	}

}
