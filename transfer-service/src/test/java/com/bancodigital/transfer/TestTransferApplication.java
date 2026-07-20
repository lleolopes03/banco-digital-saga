package com.bancodigital.transfer;

import org.springframework.boot.SpringApplication;

public class TestTransferApplication {

	public static void main(String[] args) {
		SpringApplication.from(TransferApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
