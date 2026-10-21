# Solução de problemas

[← Voltar ao README](../README.md)

Problemas reais encontrados ao desenvolver e testar o projeto, com a causa e o que fazer.

## Testes

| Sintoma | Causa | O que fazer |
|---|---|---|
| `Container startup failed for image apache/kafka-native` ou `Container exited with code 1` ao rodar os testes | Pouca memória no Docker. Cada contexto Spring de teste sobe seus próprios containers (Postgres, Kafka, Redis), e os contextos ficam em cache durante a execução. | Dê ao Docker Desktop pelo menos 6 a 8 GB, pare o `docker compose` local antes de rodar a suíte e execute de novo. É intermitente e não indica erro no código. |
| Milhares de linhas `Rebootstrapping with Cluster(... localhost:9092 ...)` e testes lentos | Um contexto de teste sem Kafka onde o `KafkaAdmin` tenta criar os tópicos. | Nos testes `*ApplicationTests` já vai `spring.kafka.admin.auto-create=false`. Em um teste novo sem Kafka, faça o mesmo. |
| `spotless:check` falha no CI | Código fora do formato. | `./mvnw spotless:apply` no serviço e commite. |
| O `e2e-tests` usa um comportamento antigo dos serviços | O módulo depende dos serviços como bibliotecas do repositório Maven local. | Rode `./mvnw install -DskipTests` em cada serviço antes do `mvn verify` do `e2e-tests`. |

## Ambiente local

| Sintoma | Causa | O que fazer |
|---|---|---|
| Postgres ou MongoDB não sobe, "porta em uso" | Instalação nativa na mesma porta. | As portas já foram remapeadas (5442, 5443, 5434, 27018). Ajuste em `docker-compose.yml`, `.env.example` e `application-dev.yml`. |
| Jaeger reinicia ou falha com "permission denied" em `/badger` | O volume novo nasce com dono root e a imagem roda sem privilégio. | O `docker-compose.yml` usa `user: "0"` para o Jaeger em desenvolvimento. Mantenha, ou ajuste o dono do volume. |
| Jaeger sem traces | Amostragem em 0, `otel-collector` parado ou endpoint errado. | Confira `TRACING_SAMPLING_PROBABILITY`, o container `bank-otel-collector` e `OTEL_EXPORTER_OTLP_ENDPOINT`. |
| Painéis do Grafana vazios | Alvo do Prometheus fora do ar. | Em `http://localhost:9090/targets` os quatro serviços devem estar `UP`. Os serviços rodam no host, e o Prometheus os alcança por `host.docker.internal`. |
| Aviso `Failed to publish metrics to OTLP receiver (.../v1/metrics)` nos logs | O Micrometer tenta exportar métricas por OTLP, mas o collector só recebe traces. | Inofensivo: as métricas saem por scrape do Prometheus (`/actuator/prometheus`). |

## API e saga

| Sintoma | Causa | O que fazer |
|---|---|---|
| `401` em tudo | Token ausente, expirado (15 min por padrão), revogado no logout, ou `JWT_SECRET` diferente entre os serviços. | Faça login de novo e use o mesmo `JWT_SECRET` nos serviços que validam o token. |
| `403` ao consultar uma conta | Cada usuário só acessa a própria conta. | Use o token do dono da conta. |
| `429` em `POST /api/transfers` | Limite de 10 transferências por conta de origem a cada 60 s. | Aguarde o tempo do header `Retry-After`. |
| Saga termina `FAILED` com `VELOCITY_EXCEDIDA` | Regra antifraude: mais de 5 transferências da mesma origem em 60 s. | Esperado em teste de carga com poucas contas. Ver [Antifraude](antifraude.md) e os [testes de carga](../load-tests/README.md). |
| Saga fica em `STARTED` e vira `FAILED` com `SAGA_TIMEOUT` depois de 2 minutos | O `account-service` não processou o pedido de reserva (serviço parado ou consumer sem partições). | Confira se o `account-service` está no ar e se os tópicos existem com 3 partições (`kafka-topics.sh --describe`). Os tópicos são criados pelos próprios serviços na subida. |
| Saga fica em `ANTIFRAUD_PENDING` | `antifraud-service` parado. | Suba o serviço; a saga termina por timeout se ele não voltar. |
| Mensagens em `*-dlt` | Falha permanente de processamento após 1 tentativa e 2 retries. | Veja o motivo nos headers da mensagem. Ver [Resiliência](resiliencia.md). |
