package com.bancodigital.notification.web;

import static org.assertj.core.api.Assertions.assertThat;

import com.bancodigital.notification.AbstractIntegrationTest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;

/*
 * A spec OpenAPI é um contrato: este teste a compara com o comportamento real da API. Falha se aparecer um
 * endpoint sem documentação, se o código de sucesso divergir do que o controller devolve ou se um erro
 * conhecido deixar de estar documentado.
 */
class OpenApiContractTest extends AbstractIntegrationTest {

    private record Esperado(String sucesso, Set<String> erros) {
    }

    private static final Map<String, Esperado> CONTRATO = Map.ofEntries(
            Map.entry("GET /api/notifications/{accountId}", new Esperado("200", Set.of("401", "403"))));

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Autowired
    private TestRestTemplate rest;

    @Test
    void todaOperacaoEstaDocumentadaComOCodigoDeSucessoEOsErrosReais() throws Exception {
        JsonNode spec = MAPPER.readTree(rest.getForObject("/v3/api-docs", String.class));
        Map<String, JsonNode> operacoes = new TreeMap<>();
        Iterator<Map.Entry<String, JsonNode>> caminhos = spec.get("paths").fields();
        while (caminhos.hasNext()) {
            Map.Entry<String, JsonNode> caminho = caminhos.next();
            Iterator<Map.Entry<String, JsonNode>> metodos = caminho.getValue().fields();
            while (metodos.hasNext()) {
                Map.Entry<String, JsonNode> metodo = metodos.next();
                operacoes.put(metodo.getKey().toUpperCase() + " " + caminho.getKey(), metodo.getValue());
            }
        }

        assertThat(operacoes.keySet()).as("operações expostas")
                .containsExactlyInAnyOrderElementsOf(CONTRATO.keySet());

        operacoes.forEach((nome, operacao) -> {
            Esperado esperado = CONTRATO.get(nome);
            assertThat(operacao.path("summary").asText()).as(nome + " summary").isNotBlank();
            assertThat(operacao.path("description").asText()).as(nome + " description").isNotBlank();
            assertThat(operacao.path("tags").get(0).asText()).as(nome + " tag").doesNotEndWith("-controller");

            Set<String> codigos = new TreeSet<>();
            operacao.path("responses").fieldNames().forEachRemaining(codigos::add);
            assertThat(codigos).as(nome + " respostas").contains(esperado.sucesso()).containsAll(esperado.erros());
            assertThat(codigos.stream().filter(c -> c.startsWith("2")))
                    .as(nome + " só o código de sucesso real").containsExactly(esperado.sucesso());
            assertThat(operacao.path("responses").path(esperado.sucesso()).has("content"))
                    .as(nome + " resposta de sucesso com corpo documentado").isTrue();
        });
    }
}
