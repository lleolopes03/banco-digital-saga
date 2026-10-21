package com.bancodigital.account;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

// Este contexto não tem Kafka: sem desligar a criação de tópicos, o KafkaAdmin tenta conectar em
// localhost:9092 e fica num laço de reconexão que consome CPU (e some em máquinas com muitos núcleos).
@Import(TestcontainersConfiguration.class)
@SpringBootTest(properties = "spring.kafka.admin.auto-create=false")
class AccountApplicationTests {

	@Test
	void contextLoads() {
	}

}
