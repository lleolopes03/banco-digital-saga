# Autenticação

[← Voltar ao README](../README.md)

`account-service`, `transfer-service` e `notification-service` exigem um JWT válido em toda rota
que não seja `/api/auth/**` ou `/actuator/health` (`SecurityConfig` de cada serviço).
`antifraud-service` continua sem autenticação — não expõe nenhum endpoint HTTP consumido pelo
frontend, só participa da saga via Kafka.

## Login, logout e blacklist

- **`POST /api/auth/login`** (só existe no `account-service`, dono da tabela `users`): recebe
  `email`/`senha`, valida a senha com BCrypt e devolve um JWT HS256 de **15 minutos** com claims
  `sub` (userId), `accountId` e `jti` (id único do token).
- **`POST /api/auth/logout`**: extrai o `jti` e a expiração do token recebido e grava
  `jwt:blacklist:<jti>` no Redis com TTL igual ao tempo de validade **restante** do token — a
  entrada some sozinha do Redis exatamente quando o token expiraria de qualquer forma.
- **`JwtAuthenticationFilter`** (duplicado em `account-service`, `transfer-service` e
  `notification-service`, mesma convenção de duplicação entre serviços já usada para
  `OutboxEvent`/`ProcessedEvent`): valida a assinatura do token e, antes de autorizar, confere se
  o `jti` está na blacklist do Redis. Os três serviços apontam para a mesma instância Redis
  (`docker-compose.yml`) e compartilham o segredo de assinatura via `JWT_SECRET`, então um logout
  feito no `account-service` também invalida o token nos outros dois imediatamente.

## Autorização por conta

Todo endpoint escopado por conta valida que o `accountId` do token bate com o `accountId` da
requisição, devolvendo `403` caso contrário:

| Serviço | Endpoint | Conta verificada |
|---|---|---|
| account-service | `GET/POST /api/accounts/{id}...` (balance, deposits, statement, busca por id) | `{id}` da URL |
| transfer-service | `POST /api/transfers` | `accountOrigemId` do corpo |
| notification-service | `GET /api/notifications/{accountId}` | `{accountId}` da URL |

`GET /api/transfers/{id}` continua exigindo apenas um JWT válido (não verifica se o chamador é
dono da conta de origem/destino da saga) — ver "Próximos passos" abaixo.

## Próximos passos

- **Refresh token**: o sistema usa só um access token de 15 minutos, sem refresh — o usuário
  precisa logar de novo quando expirar. Ainda não implementado.
- **Ownership em `GET /api/transfers/{id}`**: hoje qualquer usuário autenticado pode consultar
  qualquer saga pelo id, sem checar se é a conta de origem/destino.
- **Signup**: não há endpoint de cadastro de usuário — `User` só é criado diretamente
  no banco (ver testes `*IT`). `POST /api/accounts` exige um JWT válido, mas não precisa ser de um
  usuário já vinculado àquela conta (abrir uma nova conta não tem `{accountId}` para validar
  contra).
