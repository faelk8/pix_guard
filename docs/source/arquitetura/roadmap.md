# Fases e limites do projeto

Cada fase é validada antes que a próxima seja considerada pronta. A arquitetura evolui aos poucos para que o
leitor consiga relacionar uma capacidade com seus testes e evidências.

| Fase | Objetivo | Estado nesta entrega |
|---|---|---|
| 0 — Arquitetura | Definir propósito, limites, arquitetura e critérios. | Concluída e documentada. |
| 1 — Fluxo inicial | Docker, contrato de eventos, geração de 100 mil registros, painel e Console Kafka. | Implementada; testes em container passaram. |
| 2 — Streaming | Publicar e consumir eventos em Kafka; estudar replay, atraso, duplicatas e falhas. | Planejada; gerador ainda não publica no broker. |
| 3 — Processamento | Provar Spark e camadas Bronze, Silver e Gold no volume. | Planejada; Spark não faz parte desta entrega. |
| 4 — Análise | Comparar regras explicáveis e, se justificado, modelos experimentais. | Planejada; nenhuma previsão antifraude foi construída. |
| 5 — Operação | Integrar o fluxo, testar ponta a ponta e consolidar guias. | Planejada. |

## Por que separar as fases?

Primeiro, o projeto prova que consegue gerar registros válidos e acompanhá-los. Em seguida, uma fase liga o
gerador ao Kafka. Depois disso, o processamento transforma eventos em conjuntos de dados organizados. Somente
com dados e processamento conferidos faz sentido avaliar regras ou modelos.

Todo resultado usa dados inventados e só descreve o comportamento do simulador. As métricas não podem ser
interpretadas como desempenho contra fraude real.
