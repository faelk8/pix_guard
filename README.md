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
2. Kafka, sistema de mensageria, poderá transportar esses registros entre serviços.
3. Um pipeline, sequência de etapas de processamento, validará e organizará os dados.
4. Os resultados ficarão em camadas: Bronze (originais), Silver (validados) e Gold (resumidos).
5. Um dashboard mostrará o progresso e os indicadores do laboratório.

Docker executa os serviços em containers isolados. Docker Compose descreve como iniciá-los juntos; um volume
Docker local mantém os arquivos disponíveis para os containers. O armazenamento da primeira versão usa esse
volume, sem MinIO ou serviço S3 separado. A integração de streaming e o processamento com Spark serão entregues
em fases próprias.

## Fases

- **FASE 0 — Arquitetura:** objetivos, limites, decisões e critérios de validação.
- **FASE 1 — Fluxo inicial:** serviços Docker, contrato de eventos, gerador sintético e painel de progresso.
- **FASE 2 — Streaming:** integração com Kafka, repetição, atrasos e falhas.
- **FASE 3 — Processamento:** prova com Spark e camadas Bronze, Silver e Gold.
- **FASE 4 — Análise:** regras explicáveis, métricas e eventual avaliação de aprendizado de máquina.
- **FASE 5 — Operação:** integração de ponta a ponta, validações finais e guias de execução.

O planejamento e o histórico de validação estão em [docs/plan.md](docs/plan.md). As decisões de arquitetura ficam
em [docs/adr/](docs/adr/), e as notas de estudo em [docs/learning-notes.md](docs/learning-notes.md). O código é
publicado por entregas vinculadas a Issues e Pull Requests; consulte o estado de cada fase antes de usar os
serviços descritos no plano.
