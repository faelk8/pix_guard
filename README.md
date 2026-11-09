<h1 align="center">PixGuard AI</h1>

<div align="center">
  <img alt="Apache Kafka" height="72" width="80" src="https://cdn.simpleicons.org/apachekafka/231F20">
  <img alt="Docker" height="72" width="80" src="https://cdn.simpleicons.org/docker/2496ED">
  <img alt="Python" height="72" width="80" src="https://cdn.simpleicons.org/python/3776AB">
</div>

O PixGuard AI é um laboratório educacional para estudar como dados de pagamentos podem ser gerados, validados,
processados e apresentados. Ele **não processa pagamentos, não usa transações reais e não é um sistema antifraude**.
Os eventos e rótulos são sintéticos; resultados descrevem apenas o simulador.

## Como o projeto deve funcionar

1. Um gerador cria registros fictícios de pagamentos.
2. Kafka, sistema de mensageria, será o caminho de transporte entre serviços nas fases de streaming.
3. Um pipeline, sequência de etapas de processamento, validará e organizará os dados.
4. Os resultados ficarão em camadas: Bronze (originais), Silver (validados) e Gold (resumidos).
5. Um dashboard mostrará o progresso e os indicadores do laboratório.

Docker executa os serviços em containers isolados. Docker Compose descreve como iniciá-los juntos; um volume
Docker local mantém os arquivos disponíveis para os containers. O armazenamento da primeira versão usa esse
volume, sem MinIO ou serviço S3 separado. A integração de streaming e o processamento com Spark serão entregues
em fases próprias.

## Visualizar o Kafka

O Redpanda Console é uma interface web para explorar um cluster Kafka. Neste projeto, ela roda em container e
se conecta ao broker pela rede interna do Compose. Com o Docker ativo, inicie a interface:

```bash
docker compose up -d --wait redpanda-console
```

Abra [http://localhost:8080](http://localhost:8080). Em **Topics**, você pode consultar os tópicos e abrir as
mensagens de cada partição. A interface é publicada somente no endereço local da máquina.

Na FASE 1, o gerador grava o lote de 100 mil eventos em JSONL no volume local; ele ainda não publica esses
eventos no Kafka. Para experimentar a visualização, crie um tópico e envie uma mensagem sintética de demonstração
por comandos executados dentro do container Kafka:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic pixguard-console-demo --partitions 1 --replication-factor 1
printf '%s\n' '{"schema_version":"payment_event_v1","event_id":"demo-0001","label_is_synthetic":true,"message":"evento fictício de demonstração"}' | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic pixguard-console-demo
```

Atualize o tópico no Console e abra **Messages** para conferir o registro. A publicação automática dos eventos
gerados no Kafka pertence à fase de streaming.

## Fases

- **FASE 0 — Arquitetura:** objetivos, limites, decisões e critérios de validação.
- **FASE 1 — Fluxo inicial:** serviços Docker, contrato de eventos, gerador sintético, painel de progresso e Redpanda Console para inspecionar Kafka.
- **FASE 2 — Streaming:** integração com Kafka, repetição, atrasos e falhas.
- **FASE 3 — Processamento:** prova com Spark e camadas Bronze, Silver e Gold.
- **FASE 4 — Análise:** regras explicáveis, métricas e eventual avaliação de aprendizado de máquina.
- **FASE 5 — Operação:** integração de ponta a ponta, validações finais e guias de execução.

O planejamento e o histórico de validação estão em [docs/plan.md](docs/plan.md). As decisões de arquitetura ficam
em [docs/adr/](docs/adr/), e as notas de estudo em [docs/learning-notes.md](docs/learning-notes.md). O código é
publicado por entregas vinculadas a Issues e Pull Requests; consulte o estado de cada fase antes de usar os
serviços descritos no plano.

## Portal de documentação

A documentação didática da arquitetura, dos comandos e dos limites de cada fase está em `docs/source/`. Construa
o portal Sphinx dentro de um container com `make docs-build`; os arquivos HTML são gerados em `docs/_build/html/`.
