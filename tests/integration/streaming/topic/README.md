# Provisionamento do tópico

Da raiz do repositório, execute `sh tests/integration/streaming/topic/run.sh`.
O teste usa um projeto Compose único, fixa bootstrap/tópico locais e preserva
seu volume Kafka como evidência. Remove containers e rede ao encerrar.
O nome do volume é impresso no início; nenhum volume principal é utilizado.

`kafka-topic-init` cria o tópico configurado quando ausente e verifica uma
partição, replicação 1, `retention.ms=86400000` (24h, Issue #27) e
`cleanup.policy=delete`. Divergências existentes causam falha sem reparo.
O script limita a execução completa a 90 segundos, com mais 5 segundos
para encerramento forçado.

A aplicação aguarda o sucesso inicial deste serviço. Compose pode reutilizar
um serviço one-shot concluído em chamadas posteriores de `up`. Antes de cada
futura publicação, execute explicitamente:

```sh
docker compose run --rm kafka-topic-init
```

A integração futura do publicador deve manter essa revalidação por execução.
O teste PoC anterior continua criando seu próprio tópico explicitamente,
usando `--no-deps`; não depende da criação automática do broker.
