package com.bancodigital.transfer.infrastructure.messaging;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Encaminha o payload de um tópico compartilhado (com vários tipos de evento) ao tratador do seu tipo.
 * O tipo é o nome simples da classe do evento, o mesmo critério com que o produtor escreve o header
 * {@code eventType}: não há texto de tipo a manter, e tratar um tipo novo é registrar mais um
 * {@link #quando}, sem editar um {@code switch}.
 */
final class EventRouter {

    private final Map<String, BiConsumer<String, String>> tratadores = new HashMap<>();
    private final Consumer<String> tipoDesconhecido;

    private EventRouter(Consumer<String> tipoDesconhecido) {
        this.tipoDesconhecido = tipoDesconhecido;
    }

    /** Tipos sem tratador registrado são ignorados. */
    static EventRouter ignorandoDesconhecidos() {
        return new EventRouter(tipo -> { });
    }

    /** Tipos sem tratador registrado são um erro (vão para retry e DLQ). */
    static EventRouter rejeitandoDesconhecidos(String topico) {
        return new EventRouter(tipo -> {
            throw new IllegalStateException("eventType desconhecido em " + topico + ": " + tipo);
        });
    }

    /** Registra o tratador de um tipo; ele recebe o evento já desserializado e o traceparent recebido. */
    <T> EventRouter quando(Class<T> tipo, BiConsumer<T, String> tratador) {
        tratadores.put(tipo.getSimpleName(), (payload, traceparent) ->
                tratador.accept(EventPayloadReader.ler(payload, tipo), traceparent));
        return this;
    }

    void encaminhar(String eventType, String payload, String traceparent) {
        BiConsumer<String, String> tratador = tratadores.get(eventType);
        if (tratador == null) {
            tipoDesconhecido.accept(eventType);
            return;
        }
        tratador.accept(payload, traceparent);
    }
}
