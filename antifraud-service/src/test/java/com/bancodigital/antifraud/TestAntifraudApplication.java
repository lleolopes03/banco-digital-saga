package com.bancodigital.antifraud;

import org.springframework.boot.SpringApplication;

public class TestAntifraudApplication {

	public static void main(String[] args) {
		SpringApplication.from(AntifraudApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
