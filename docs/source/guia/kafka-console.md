# Explorar Kafka com Redpanda Console

Kafka é o broker (servidor de mensagens) do projeto. Redpanda Console é a interface no navegador que consulta
o broker; não é um segundo Kafka e não substitui o serviço `kafka`.

Inicie o Console. O Compose aguarda o healthcheck do Kafka antes de iniciar a interface:

```bash
docker compose up -d --wait redpanda-console
```

Abra http://localhost:8080 e selecione **Topics**. A porta fica publicada em `127.0.0.1`, então a interface
destina-se ao próprio computador.

## Criar um tópico e ler uma mensagem de demonstração

Na raiz do projeto, crie o tópico usando o cliente que já vem no container Kafka:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic pixguard-console-demo --partitions 1 --replication-factor 1
```

Publique uma mensagem que está explicitamente marcada como sintética:

```bash
printf '%s\n' '{"schema_version":"payment_event_v1","event_id":"demo-0001","label_is_synthetic":true,"message":"evento fictício de demonstração"}' | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic pixguard-console-demo
```

No Console, atualize **Topics**, abra `pixguard-console-demo` e a aba **Messages**. A mensagem deverá mostrar
`event_id` igual a `demo-0001` e `label_is_synthetic` igual a `true`.

## Inspecionar o lote da FASE 2

A **FASE 2 está implementada**. O comando `pixguard generate` grava eventos em JSONL no volume local e não
publica sozinho no Kafka. O envio e a leitura do lote usam comandos explícitos:

* `pixguard publish` lê o JSONL e publica os eventos no tópico configurado.
* `pixguard capture` persiste os registros de uma faixa finita de offsets.
* `pixguard export-capture` valida a captura e exporta um snapshot JSONL.

Para preparar o tópico e executar os comandos com os parâmetros e verificações apropriados, siga o
{doc}`guia da FASE 5 <integracao-fase-5>`. Use o Console no mesmo projeto Compose e, após a publicação, abra o
tópico `pixguard.payment_event_v1` em **Topics** e selecione **Messages** para inspecionar a partição 0.

O tópico `pixguard-console-demo` acima continua sendo uma demonstração manual de inspeção. Sua mensagem é
simplificada e não contém todos os campos de `payment_event_v1`; ela não substitui os eventos do lote.
Na integração da FASE 5, o Spark lê o snapshot JSONL em lote. Esse percurso não estabelece processamento
contínuo no Spark, semântica *exactly-once* de ponta a ponta nem eficácia antifraude real.
