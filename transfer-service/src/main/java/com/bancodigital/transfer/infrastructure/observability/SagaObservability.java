package com.bancodigital.transfer.infrastructure.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.stereotype.Component;

/*
 * Propagação de trace através da fronteira assíncrona do Kafka (padrão Outbox) + spans/métricas
 * customizados por etapa da saga. Duplicada por serviço (mesma convenção já usada neste repo
 * para OutboxEvent/ProcessedEvent/IdempotentEventHandler) em vez de extraída para um módulo
 * compartilhado, já que não existe um módulo "common" no projeto.
 *
 * transfer-service é o orquestrador da saga, então esta cópia também concentra os contadores de
 * ciclo de vida (sagas concluídas/falhas, compensações por motivo) — os outros dois serviços não
 * têm essa noção de "saga inteira", só processam uma etapa isolada.
 */
@Component
public class SagaObservability {

    private static final String TRACEPARENT_HEADER = "traceparent";
    private static final String METRIC_STEP_DURATION = "saga_step_duration_seconds";
    private static final String METRIC_SAGA_TOTAL = "saga_transfers_total";
    private static final String METRIC_COMPENSATIONS_TOTAL = "saga_compensations_total";

    private final OpenTelemetry openTelemetry;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;

    public SagaObservability(OpenTelemetry openTelemetry, Tracer tracer, MeterRegistry meterRegistry) {
        this.openTelemetry = openTelemetry;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
    }

    /** Captura o traceparent (W3C) do contexto de trace ativo — chamar ao criar um OutboxEvent. */
    public String traceparentAtual() {
        Map<String, String> carrier = new HashMap<>();
        openTelemetry.getPropagators().getTextMapPropagator().inject(Context.current(), carrier, Map::put);
        return carrier.get(TRACEPARENT_HEADER);
    }

    /** Anexa o traceparent já capturado (armazenado no OutboxEvent) como header Kafka literal. */
    public void injetarHeader(ProducerRecord<String, String> record, String traceContext) {
        if (traceContext == null || traceContext.isBlank()) {
            return;
        }
        record.headers().add(new RecordHeader(TRACEPARENT_HEADER, traceContext.getBytes(StandardCharsets.UTF_8)));
    }

    /*
     * Executa a etapa de negócio de um consumer dentro de um span "saga.<etapa>" cujo pai é o
     * trace extraído do traceparent recebido no header Kafka (ou um trace novo, se o header
     * vier nulo/ausente — nunca um erro), e registra a duração num Timer
     * saga_step_duration_seconds{step=<etapa>} para os dashboards de latência por etapa.
     */
    public void executarEtapa(String etapa, String traceparentRecebido, Map<String, String> atributos,
                               Runnable acao) {
        Context parentContext = extrairContexto(traceparentRecebido);
        Span span = tracer.spanBuilder("saga." + etapa).setParent(parentContext).startSpan();
        atributos.forEach(span::setAttribute);

        Timer.Sample sample = Timer.start(meterRegistry);
        try (Scope scope = span.makeCurrent()) {
            acao.run();
        } catch (RuntimeException ex) {
            span.recordException(ex);
            span.setStatus(StatusCode.ERROR, ex.getMessage());
            throw ex;
        } finally {
            // publishPercentileHistogram(): sem isso o Prometheus não recebe os buckets
            // (_bucket) necessários para histogram_quantile(0.95, ...) no dashboard Grafana.
            sample.stop(Timer.builder(METRIC_STEP_DURATION).tag("step", etapa).publishPercentileHistogram()
                    .register(meterRegistry));
            span.end();
        }
    }

    /** Contador de sagas que chegaram a um estado terminal, tagueado por status (COMPLETED/FAILED). */
    public void registrarSagaFinalizada(String status) {
        Counter.builder(METRIC_SAGA_TOTAL).tag("status", status).register(meterRegistry).increment();
    }

    /** Contador de compensações disparadas, tagueado pelo motivo (LIMITE_EXCEDIDO, SAGA_TIMEOUT, etc). */
    public void registrarCompensacao(String motivo) {
        Counter.builder(METRIC_COMPENSATIONS_TOTAL).tag("reason", motivo).register(meterRegistry).increment();
    }

    private Context extrairContexto(String traceparent) {
        if (traceparent == null || traceparent.isBlank()) {
            return Context.root();
        }
        Map<String, String> carrier = Map.of(TRACEPARENT_HEADER, traceparent);
        return openTelemetry.getPropagators().getTextMapPropagator().extract(Context.root(), carrier, MAP_GETTER);
    }

    private static final TextMapGetter<Map<String, String>> MAP_GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Map<String, String> carrier) {
            return carrier.keySet();
        }

        @Override
        public String get(Map<String, String> carrier, String key) {
            return carrier == null ? null : carrier.get(key);
        }
    };
}
