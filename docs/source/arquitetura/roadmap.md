# Fases e limites do projeto

Cada fase é validada antes que a próxima seja considerada pronta. A arquitetura evolui aos poucos para que o
leitor consiga relacionar uma capacidade com seus testes e evidências.

| Fase | Objetivo | Estado nesta entrega |
|---|---|---|
| 0 — Arquitetura | Definir propósito, limites, arquitetura e critérios. | Concluída e documentada. |
| 1 — Fluxo inicial | Docker, contrato de eventos, geração de 100 mil registros, painel e Console Kafka. | Implementada; testes em container passaram. |
| 2 — Streaming | Publicar e consumir eventos em Kafka; estudar replay, atraso, duplicatas e falhas. | Planejada; gerador ainda não publica no broker, e a pipeline não lê Kafka. |
| 3 — Processamento offline | Processar o JSONL da FASE 1 em Bronze, Silver, quarentena e Gold com Spark SDP. | Implementada e validada em containers, em entrega própria. |
| 4 — Análise | Comparar regras explicáveis e, se justificado, modelos experimentais. | Entrega separada; não faz parte do processamento da FASE 3. |
| 5 — Operação | Integrar o fluxo, testar ponta a ponta e consolidar guias. | Planejada. |

## Por que separar as fases?

Primeiro, o projeto prova que consegue gerar registros válidos e acompanhá-los. Em seguida, uma fase liga o
gerador ao Kafka. Depois disso, o processamento transforma eventos em conjuntos de dados organizados. Somente
com dados e processamento conferidos faz sentido avaliar regras ou modelos.

Todo resultado usa dados inventados e só descreve o comportamento do simulador. As métricas não podem ser
interpretadas como desempenho contra fraude real. A FASE 3 lê `events.jsonl` diretamente: ter um serviço Kafka
no Compose e um Console para mensagens de demonstração não significa que o gerador esteja conectado ao broker.

## O que o processamento faz

O comando `pixguard-pipeline dry-run` valida o grafo declarado no projeto Spark; `pixguard-pipeline process`
executa o `dry-run` e depois a materialização SDP. Bronze preserva as linhas JSONL e sua proveniência. Silver
valida o contrato, representa horário em UTC e BRL como `Decimal`, mantém uma linha para cópias semanticamente
iguais do mesmo `event_id` e quarentena toda ocorrência inválida ou membro de um grupo de ID com payload
divergente. Gold soma e conta por dia UTC, comerciante, canal e status.

Cada execução congela a entrada em `input-snapshot.jsonl` e escreve sob `runs/<run_id>/`, com warehouse próprio,
Parquet, resumo de até vinte grupos, checksum,
contagens e manifesto atômico. O painel escolhe o manifesto concluído válido mais recente. Se uma execução
posterior falhar, ela gera um aviso, mas não troca os indicadores do último sucesso. Não há alias `latest` nem
política de retenção; cuidado com espaço no volume.

Na validação do lote padrão, Bronze/Silver tiveram 100.000 linhas, quarentena e duplicatas removidas foram zero,
Gold teve 3.269 grupos e dois `run_id` independentes produziram resultados idênticos. A fixture também testou
deduplicação, divergência de payload, JSON inválido e evento atrasado. Consulte o [guia para executar e validar
a FASE 3](../guia/processamento-fase-3.md) para comandos, saídas e limites.
