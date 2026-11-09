# Notas de aprendizado — PixGuard AI

## FASE 0 — Conceitos iniciais

### Structured Streaming e SDP

Em Structured Streaming, descrevemos uma consulta sobre um DataFrame de streaming e também configuramos sua
execução, trigger, checkpoint e sink. O motor processa os dados incrementalmente; event time, watermark,
reprocessamento e semântica do sink continuam sendo decisões do projeto.

Com Spark Declarative Pipelines (SDP), declaramos datasets e dependências. O framework monta e executa o grafo;
as funções de dataset descrevem transformações e retornam DataFrames. `spark-pipelines dry-run` analisa
definições e o grafo sem ler nem escrever dados; `spark-pipelines run` executa a pipeline de verdade.
Declarativo muda a forma de organizar o pipeline, mas não remove as decisões sobre qualidade, event time,
checkpoint ou comportamento do destino.

Referências oficiais: [Structured Streaming](https://spark.apache.org/docs/latest/streaming/index.html) e
[Spark Declarative Pipelines](https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html).

### Exercício mínimo de quadro branco

Considere dois registros para a mesma conta fictícia: um evento com `event_time=10:00` recebido às 10:00:01 e
outro com `event_time=09:59` recebido às 10:00:05. Antes de escrever código, responda:

1. Qual campo define a janela de negócio: hora do evento ou hora de ingestão?
2. Como identificar replay do mesmo `event_id` sem remover dois eventos legítimos?
3. Até que atraso a janela continuará aceitando eventos tardios?
4. O que o pipeline grava se a saída falha depois da leitura?
5. Que evidência de `dry-run` é insuficiente para provar que a Gold foi materializada?

### Evidência histórica da FASE 0

Na FASE 0, nenhum pacote foi instalado e nenhum container foi iniciado. A disponibilidade e as licenças de
fontes externas ficaram para verificação antes de qualquer integração. A primeira versão do laboratório foi
planejada para operar com dados sintéticos e offline.

### O que aprendi / trade-offs

- Rótulo de fraude no simulador descreve uma regra inventada do gerador; não é ground truth bancário.
- A FASE 0 pode avaliar disponibilidade e riscos sem puxar dados nem iniciar serviços.
- A prova de SDP exige execução real com fixture; um dry-run isolado não é evidência de dados persistidos.

### Evidências da FASE 1

A geração padrão produziu 100.000 eventos dentro do Docker; duas execuções com a mesma seed produziram o mesmo
SHA-256. Os 100 testes da suíte passaram, o volume local persistiu os arquivos e o dashboard mostrou o manifesto
real de progresso. Kafka está disponível como serviço, mas ainda não recebe os eventos nesta fase.

### Próximo experimento

Na FASE 2, publicar eventos no Kafka e estudar entrega repetida, atraso, replay e idempotência. Depois, medir como
essas condições afetam a gravação e a visualização, sem presumir garantias que ainda não foram testadas.
