# Fases e limites do projeto

Cada fase é validada antes que a próxima seja considerada pronta. A arquitetura evolui aos poucos para que o
leitor consiga relacionar uma capacidade com seus testes e evidências.

| Fase | Objetivo | Estado nesta entrega |
|---|---|---|
| 0 — Arquitetura | Definir propósito, limites, arquitetura e critérios. | Concluída e documentada. |
| 1 — Fluxo inicial | Docker, contrato de eventos, geração de 100 mil registros, painel e Console Kafka. | Implementada; testes em container passaram. |
| 2 — Streaming | Publicar e consumir eventos em Kafka; estudar replay, atraso, duplicatas e falhas. | Integrada e validada: publicação do lote, captura de faixa finita e exportação JSONL; Spark não lê Kafka. |
| 3 — Processamento offline | Processar o JSONL da FASE 1 em Bronze, Silver, quarentena e Gold com Spark SDP. | Integrada e validada em containers; não consome Kafka. |
| 4 — Análise | Avaliar uma regra explicável fixa e mostrar métricas sintéticas; ML exige hipótese à parte. | Implementada e validada em containers como etapa separada; não comprova detecção real. |
| 5 — Operação | Integrar o fluxo, testar ponta a ponta e consolidar guias. | Integrada e validada: snapshot Kafka comparado ao caminho offline, replay, reexecução e recuperação controlada. |

## Por que separar as fases?

Primeiro, o projeto prova que consegue gerar registros válidos e acompanhá-los. Em seguida, uma fase liga o
gerador ao Kafka. Depois disso, o processamento transforma eventos em conjuntos de dados organizados. Somente
com dados e processamento conferidos faz sentido avaliar regras ou modelos.

Todo resultado usa dados inventados e só descreve o comportamento do simulador. As métricas não podem ser
interpretadas como desempenho contra fraude real. A FASE 2 transporta o lote pelo Kafka e exporta um snapshot finito de offsets. A FASE 3 lê JSONL em lote,
seja o original ou o snapshot exportado; ela não consome Kafka diretamente. A FASE 4 também é offline e consome
a Silver da FASE 3. A FASE 5 compara os dois caminhos e apresenta os resultados no dashboard.

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

## Evidência da integração final

O ensaio com seed 42 publicou 100.000 eventos e capturou offsets `[0, 100000)` da partição 0. Original, snapshot
Kafka e reexecução Spark produziram Bronze/Silver 100.000, zero duplicatas/quarentena, 3.269 grupos Gold e
BRL `245449133.57`. A comparação dos Parquet completos não encontrou diferenças Gold nos dois sentidos.
Duas avaliações da mesma Silver retornaram `equivalent=true`; os rótulos e métricas permanecem sintéticos.

O replay releu a faixa sem republicar e preservou o hash do JSONL exportado. O aceite separado passou em
8 testes, incluindo retomada de falhas controladas de armazenamento/commit. O broker tem uma réplica e
retenção de 24 h; esses ensaios não estabelecem alta disponibilidade, streaming contínuo, recuperação de
qualquer corrupção ou *exactly-once* de ponta a ponta. Consulte o [guia da FASE 5](../guia/integracao-fase-5.md)
para comandos, hashes, comparação, recuperação e limpeza.
