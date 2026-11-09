# Plano do PixGuard AI

**Estado:** FASE 0 concluída: objetivos, arquitetura, decisões e roteiro de implementação definidos.

## Objetivo e escopo

Construir um laboratório educacional reproduzível para explorar eventos de pagamento Pix fictícios, Kafka,
Spark Declarative Pipelines (SDP), qualidade de dados e detecção experimental por regras/modelo. O MVP não usa
transações reais, não processa pagamentos e não representa um antifraude de produção.

MVP: gerador sintético de **100.000 eventos** com seed 42; contrato versionado; caminho offline por fixture;
Kafka no `docker-compose.yml`; camadas Bronze/Silver/Gold em diretórios locais num volume Docker compartilhado;
regras explicáveis; dashboard web local com marcação de dados simulados. Toda execução de código, Spark, testes e interface ocorre dentro de
containers. ML é opcional e só vem depois da qualidade/reprodutibilidade. Estatísticas agregadas do Banco Central
podem contextualizar o projeto em gráficos separados.

## O que existe / o que falta / como comprovar

| Existe | Falta | Como comprovar |
|---|---|---|
| Repositório público `faelk8/pix_guard` | desenvolver as fases do roteiro | entregas rastreadas por Issues e Pull Requests |
| Arquitetura e requisitos da FASE 0 documentados; Issues #1–#4 organizam o primeiro ciclo | implementação da infraestrutura e do gerador | plano, ADRs e Issue #4 |
| Python, Kafka e Spark como tecnologias planejadas | fixar e validar as versões dentro dos containers | testes de compatibilidade durante as fases de implementação |
| Estatísticas agregadas do Banco Central como fonte opcional futura | verificar utilidade, fonte, licença e formato antes de qualquer uso | não são requisito para o fluxo sintético offline |
| Volume Docker local como armazenamento inicial | validar compartilhamento e persistência entre serviços | critério da Issue #1 |
| Spark Declarative Pipelines como alternativa para processamento | executar uma prova com fixture antes de adotá-lo | critério da fase de processamento |

## Arquitetura proposta

```mermaid
flowchart LR
  GEN[Gerador sintético] --> K[Kafka local]
  K --> SDP[Ingestão SDP/adaptador comprovado]
  SDP --> B[Bronze em volume local]
  B --> S[Silver: contrato, dedupe, quarentena]
  S --> G[Gold: janelas e features]
  G --> RULE[Regras explicáveis]
  G -. opcional .-> ML[Modelo offline]
  RULE --> DASH[Dashboard web em container]
  ML --> DASH
  BCB[BCB agregado] --> CTX[Contexto separado]
```

O `docker-compose.yml` deverá reunir `kafka`, `app`, `dashboard` e um serviço de pipeline. O serviço de
processamento começa reservado para uma fase posterior. Um volume Docker nomeado compartilhado em
`/data/pixguard` guardará os eventos e resultados; não será necessário adicionar MinIO nesta versão. A primeira
implementação deverá gerar 100 mil eventos no volume sem manter o conjunto inteiro em memória. A integração com
Kafka e o processamento Spark serão validados em etapas próprias, sem presumir garantia de entrega de ponta a
ponta.

Dashboard progressivo: a primeira tela mostrará execução e contagem do gerador; depois poderá apresentar Kafka,
mensagens atrasadas ou repetidas, qualidade Bronze/Silver/Gold, regras e métricas. O painel e o processamento
rodarão em containers; o navegador no host será apenas o cliente visual.

### Esboço da primeira tela

```text
┌ PixGuard AI · DEMONSTRAÇÃO · DADOS 100% SINTÉTICOS ─────────────────┐
│ Gerador: aguardando │ Volume: local │ Dados: sintéticos │ Atualizar │
├────────────────────┬───────────────────┬────────────────────────────┤
│ Eventos produzidos │ Eventos solicitados │ Estado                     │
│ 0                  │ 100.000             │ aguardando                 │
├────────────────────────────────────────────────────────────────────┤
│ Progresso: 0% │ última atualização: — │ duração: — │ erro: —      │
└────────────────────────────────────────────────────────────────────┘
```

O dashboard deverá ler o estado persistido pelo gerador e distinguir uma execução concluída de estados vazios ou
com erro. Gráficos de streaming, camadas e alertas só entram depois que as respectivas fontes existirem e forem
validadas.

## Contrato e segurança de dados

Contrato proposto `payment_event_v1`: `event_id` UUID; hashes fictícios de conta/dispositivo; merchant; valor decimal BRL;
`event_time`/`ingested_at` UTC; região fictícia; canal, status, schema_version, `label_is_synthetic` e cenário.
Proibidos CPF, nome, conta e chave Pix reais. `ground_truth` não entra nas features de predição. Bronze preserva
envelope e metadados; Silver valida, normaliza, deduplica e põe inválidos em quarentena; Gold agrega por conta e
janela e explica cada alerta.

## Fontes e armazenamento

- Banco Central: estatísticas agregadas poderão ser consideradas como contexto visual separado. Antes do uso,
  verificar fonte, licença, formato e disponibilidade. Elas não são necessárias para o gerador ou testes offline.
- Eventos do projeto: 100% sintéticos, seed 42, sem dado pessoal; fixtures pequenas versionáveis.
- PaySim: opcional, fora do MVP; origem, licença e viés exigem avaliação antes de inclusão.
- Armazenamento das saídas: filesystem local no volume Docker nomeado. O caminho `/data/pixguard` deve ser
  compartilhado por app, pipeline e dashboard; separar subdiretórios de entrada, Bronze/Silver/Gold e checkpoints.

## Matriz preliminar de compatibilidade

| Componente | Estado/hipótese | Próxima prova |
|---|---|---|
| Python | versão a fixar na imagem da aplicação | escolher uma versão compatível com as ferramentas de processamento |
| Java e Spark SDP | candidatos para a fase de processamento | testar combinações compatíveis dentro do container |
| Kafka | serviço planejado no Compose | selecionar a imagem e testar entrega, replay e atraso |
| Filesystem Docker | volume nomeado em `/data/pixguard` | validar compartilhamento e persistência entre containers |
| Docker Compose | Kafka, aplicação, pipeline e dashboard | validar a configuração e os healthchecks |

## Orçamento de recursos

O lote de aceite é 100.000 eventos. O gerador e o pipeline devem processar em lotes ou fluxo, com fixtures
menores para ciclos rápidos. Cada serviço tem limites de recursos no Compose; as fases de processamento devem
medir o consumo e ajustar esses limites antes de aumentar o volume de dados.

## Experiência visual

A primeira tela deverá mostrar progresso real do gerador, totais, estado e um aviso permanente de dados
sintéticos. Telas seguintes poderão apresentar saúde do streaming, qualidade das camadas e alertas quando essas
fontes existirem. A interface não deverá inventar métricas e deverá tratar estados vazios ou erros de leitura.

## Riscos e decisões

- Compatibilidade Spark/SDP/Kafka/conector/Java ainda não comprovada. Mitigação: protótipo offline, depois PoC.
- Fontes externas podem mudar formato, licença ou disponibilidade. Mitigação: manter BCB opcional e verificar as
  condições antes de qualquer integração.
- Volume compartilhado é local ao Docker host e precisa de espaço, permissões e backup explícitos; não é storage
  distribuído nem ambiente de alta disponibilidade. Isso é aceitável para o laboratório local.
- Rótulos artificiais podem produzir métricas enganadoras. Mitigação: seed, split temporal, congelamento do teste
  e aviso explícito de que métricas descrevem o simulador.
- Garantias de entrega variam com source, checkpoint e sink. Mitigação: testar replay, explicar limites e evitar
  afirmação ampla de exactly-once.
- Como o repositório é público, revisar cada entrega para manter segredos, dados reais e artefatos locais fora
  do histórico. Documentação, código e configuração são publicados em entregas com escopo definido.

ADRs registrados: armazenamento em volume Docker local, execução em containers e uso de dados sintéticos
reproduzíveis. Decisões futuras incluem a abordagem de processamento Spark, checkpoints e critérios para avaliar
qualquer modelo, depois de estabelecer um baseline.

## Backlog por fases

### Rastreabilidade no GitHub

Cada mudança de código deve ter uma Issue que descreva o trabalho. Entregas de código, testes, configuração e
documentação são organizadas em Pull Requests vinculadas às Issues e validadas antes da integração.

1. FASE 0: auditoria e plano local (concluída).
2. Fundamental: consolidar requisitos/arquitetura (`PLAN-001`, Issue #4; concluída).
3. FASE 1: Compose base (`INFRA-001`, Issue #1), contrato/gerador de 100 mil eventos (`GEN-001`, Issue #2), dashboard (`UI-001`, Issue #3) e Redpanda Console para inspeção visual do broker (`INFRA-002`, Issue #6).
4. FASE 2: Kafka, filesystem compartilhado, replay, idempotência, atraso/quarentena e painel de streaming.
5. FASE 3: prova SDP no container com Bronze/Silver/Gold no volume local, consulta visual e execução reproduzível.
6. FASE 4: baseline, métricas, painel visual completo e comparação ML apenas se justificada.
7. FASE 5: hardening, execução E2E totalmente em Docker, README/runbook e série editorial final.

Cada fase terá Issue, critérios, validação executada em containers, evidências e um artigo próprio. Os artigos
conectam as etapas e incluem somente resultados já verificados.

## Critérios de aceite

- Seed 42 reproduz exatamente 100.000 eventos e IDs conforme a configuração declarada.
- Fixture injeta duplicatas, atrasos, eventos fora de ordem e inválidos, com tratamento observado e medido.
- Interrupção/retomada não duplica fatos Silver no sink escolhido, ou a limitação observada é documentada.
- SDP executa dry-run e run real com fixture; camadas são consultáveis e reexecução é medida.
- Regras e eventual modelo são avaliados em dados sintéticos congelados sem leakage, com falso positivo explícito.
- Painel, docs e posts sempre rotulam dados simulados e não prometem desempenho em fraude real.
- Testes offline rodam sem acesso ao BCB e posts não são publicados automaticamente.
- `docker compose up` inicia serviços; comandos de aplicação/testes rodam dentro dos containers, sem Python/Spark no host.
- A cada fase, testes passam antes de avançar e o dashboard exibe a capacidade visual concluída.
- Um artigo revisado por fase mantém links de série e contém apenas fatos comprovados daquela fase.

## Roteiro de estudo

1. Contratos, relógios de evento/processamento, UTC, dedupe e reprodutibilidade.
2. Kafka: chave, partição, offset, retenção, replay e consumer lag.
3. Structured Streaming: DataFrames, checkpoints, watermark e semântica do sink.
4. SDP: declarar datasets/fluxos, grafo, tabelas e diferença entre dry-run e execução real.
5. Qualidade, quarentena, métricas e risco de leakage em ML.

## Resultado da FASE 0

**PASSOU:** auditoria Git/ambiente, pesquisa de arquitetura e planejamento para Docker Compose, volume local,
100 mil eventos sintéticos, execução em containers, interface progressiva e validação incremental como critérios
para as fases de implementação. A FASE 0 não executou código da aplicação.
