package com.bancodigital.notification.infrastructure.observability;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import java.util.Map;
import org.springframework.stereotype.Component;

/*
 * Propagação de trace através da fronteira assíncrona do Kafka + spans/métricas customizados por
 * etapa da saga. Duplicada por serviço (mesma convenção já usada neste repo para
 * OutboxEvent/ProcessedEvent/IdempotentEventHandler) em vez de extraída para um módulo
 * compartilhado, já que não existe um módulo "common" no projeto. notification-service só
 * consome eventos (não publica nenhum via Outbox), então esta versão não precisa de
 * traceparentAtual()/injetarHeader() — só extrai o contexto recebido para abrir o span.
 */
@Component
public class SagaObservability {

    private static final String TRACEPARENT_HEADER = "traceparent";
    private static final String METRIC_STEP_DURATION = "saga_step_duration_seconds";

    private final OpenTelemetry openTelemetry;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;

    public SagaObservability(OpenTelemetry openTelemetry, Tracer tracer, MeterRegistry meterRegistry) {
        this.openTelemetry = openTelemetry;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
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
