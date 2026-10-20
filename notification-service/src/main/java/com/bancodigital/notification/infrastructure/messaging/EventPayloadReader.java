package com.bancodigital.notification.infrastructure.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Desserializa o payload JSON de um evento Kafka. Único dono do ObjectMapper dos consumers; falha de
 * desserialização vira IllegalStateException com o payload na mensagem, e o tratamento de erro do
 * listener (retry + DLQ) cuida do resto. Exceções do processamento em si NÃO são embrulhadas.
 */
final class EventPayloadReader {

    private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    private EventPayloadReader() {
    }

    static <T> T ler(String payload, Class<T> tipo) {
        try {
            return MAPPER.readValue(payload, tipo);
        } catch (Exception e) {
            throw new IllegalStateException("Falha ao desserializar " + tipo.getSimpleName() + ": " + payload, e);
        }
    }
}
