# Analisar a Silver com uma regra explicável — FASE 4

Este guia mostra como gerar dados fictícios, processá-los na FASE 3, avaliar uma regra fixa e interpretar o painel.
Não é preciso conhecer Spark ou métricas de classificação para acompanhar os conceitos. A FASE 4 é offline: ela
lê uma tabela Silver já produzida e não consome mensagens Kafka.

> **Limite do laboratório:** todos os dados e rótulos são sintéticos. A regra usa a mesma separação de valores
> construída pelo gerador; resultados perfeitos aqui não demonstram que o sistema encontra fraude real.

## O que a regra faz

Um evento é um registro individual, como uma transferência fictícia, com identificador e valor. A regra
`amount_threshold_v1`, versão `1.0.0`, marca um valor quando ele é igual ou superior a **R$ 10.000,00**.
O `score_cents` é o valor convertido em centavos inteiros (R$ 10.000,00 vira 1.000.000 centavos). Esse score
ordena valores; ele **não** é chance ou probabilidade de fraude.

Cada previsão também recebe uma explicação estável:

| Condição | Resultado | Motivo |
|---|---|---|
| valor menor que R$ 10.000,00 | não sinalizar | `AMOUNT_BELOW_THRESHOLD` |
| valor igual ou maior que R$ 10.000,00 | sinalizar | `AMOUNT_AT_OR_ABOVE_THRESHOLD` |

O limite coincide com o começo da faixa `high_amount` do gerador. Esse cenário é criado artificialmente e já
produz o rótulo de referência `ground_truth.is_suspicious`. A avaliação mede se a regra reproduz esse simulador,
não uma política bancária.

## Por que separar previsão e resposta?

O rótulo, também chamado de resposta ou alvo, informa como o simulador classificou o evento. Se esse campo entrar
na regra, o sistema poderia simplesmente copiar a resposta; a métrica pareceria excelente sem que a regra
aprendesse ou calculasse nada. Esse erro é chamado de **vazamento do alvo**.

Por isso, o scorer recebe somente `amount_decimal` como atributo de entrada. `event_id` serve para localizar a
linha depois, e não participa da decisão. O sistema grava primeiro o arquivo de predições sem o rótulo. Depois
junta as predições ao alvo usando `event_id`, exigindo um evento único de cada lado. O processo falha em vez de
ocultar linhas faltantes, IDs duplicados ou valores inválidos.

```{mermaid}
flowchart LR
  S[Silver · evento e valor] --> F[Projeção: event_id + amount_decimal]
  F --> R[Regra fixa · sem alvo]
  R --> P[Predições persistidas]
  P --> J[Junção 1:1 por event_id]
  G[ground_truth.is_suspicious] --> J
  J --> M[Matriz e métricas]
```

## Entender a matriz de confusão

A matriz compara a decisão da regra com a resposta sintética. “Positivo” significa que o rótulo do simulador diz
`is_suspicious=true`; “alerta” significa que a regra sinalizou o valor.

| | Simulador positivo | Simulador negativo |
|---|---:|---:|
| Regra sinalizou | verdadeiro positivo (TP) | falso positivo (FP) |
| Regra não sinalizou | falso negativo (FN) | verdadeiro negativo (TN) |

Um falso positivo é um alerta extra segundo o simulador. Um falso negativo é um caso positivo que a regra deixou
passar. Os nomes descrevem a comparação com o rótulo fictício; não descrevem pagamentos ou perdas reais.

## Métricas em linguagem simples

* **Precision (precisão):** entre os eventos sinalizados, qual fração corresponde a positivos do simulador?
  Fórmula: `TP / (TP + FP)`.
* **Recall (sensibilidade):** entre os positivos do simulador, qual fração foi sinalizada?
  Fórmula: `TP / (TP + FN)`.
* **F1:** resumo que exige equilíbrio entre precision e recall: `2 × TP / (2 × TP + FP + FN)`.
* **FPR:** entre os negativos do simulador, qual fração foi sinalizada por engano? Fórmula: `FP / (FP + TN)`.
* **PR-AUC:** área sob a curva precision-recall, calculada aqui a partir do score contínuo em centavos. Ela
  avalia a ordenação dos casos ao percorrer diferentes cortes; não é a mesma coisa que average precision e o
  score não vira probabilidade.
* **Prevalência:** fração de positivos no conjunto, `(TP + FN) / N`.
* **Accuracy:** fração total de acertos. É secundária: com poucos positivos, prever sempre negativo pode dar
  accuracy alta e ainda perder todos os positivos.

A comparação inclui a baseline **sempre negativo**, uma regra trivial que nunca alerta. Ela ajuda a perceber o
quanto a accuracy isolada pode esconder. Se uma fórmula não tiver denominador, o resultado aparece como
`indefinido`, `null` e uma razão — por exemplo, precision fica indefinida se não houve nenhum alerta. Isso não
deve ser interpretado como zero nem como sucesso.

Exemplo pequeno: em 100 eventos, imagine TP=8, FP=2, FN=2 e TN=88. A regra sinalizou 10 eventos e acertou 8,
então precision é 8/10 = 80%. Havia 10 positivos e 8 foram encontrados, então recall também é 80%. FPR é
2/(2+88), aproximadamente 2,22%. Se a regra não sinalizar nenhum dos 100 eventos, precision será indefinida
porque `TP + FP = 0`; recall será zero se existirem positivos.

## Reproduzir em containers

Execute os comandos na raiz do repositório. A primeira vez, o lote é gerado e a imagem de processamento é
compilada. Escolha um identificador de run ainda não usado; runs são imutáveis e não podem ser sobrescritos.

```bash
docker compose --profile pipeline build pipeline
docker compose run --rm --no-deps app pixguard generate --seed 42 --count 100000 --output /data/pixguard/events-fase4-guia-novo.jsonl
docker compose --profile pipeline run --rm pipeline pixguard-pipeline process --input /data/pixguard/events-fase4-guia-novo.jsonl --run-id fase4-guia-silver-novo
docker compose --profile pipeline run --rm pipeline pixguard-analysis evaluate --source-run-id fase4-guia-silver-novo --analysis-run-id fase4-guia-eval-novo-001
docker compose --profile pipeline run --rm pipeline pixguard-analysis evaluate --source-run-id fase4-guia-silver-novo --analysis-run-id fase4-guia-eval-novo-002
docker compose --profile pipeline run --rm pipeline pixguard-analysis compare --left fase4-guia-eval-novo-001 --right fase4-guia-eval-novo-002
docker compose up -d --wait dashboard
```

Os exemplos usam um novo nome de arquivo e IDs próprios para não sobrescrever o JSONL nem os runs imutáveis já
existentes. Se algum nome já existir no seu volume, substitua-o por outro livre. O gerador grava um manifesto
junto ao arquivo; a pipeline e a análise recusam IDs de execução já usados. O Compose compartilha
`/data/pixguard` entre os containers.

Para comparar, execute `evaluate` novamente com um novo `analysis_run_id` e a mesma Silver, depois rode `compare`.
Um resultado `equivalent: true` significa que os Parquet completos e os JSON lógicos das duas execuções são
iguais. Na validação registrada, foram comparados `eval-003` e `eval-004`, no mesmo lote de origem
`batch-100k-001`; os IDs são exemplos históricos e não devem ser reutilizados.

## Resultado medido e como conferir

No lote validado de 100.000 registros, o gerador, com seed 42, produziu 5.008 positivos e 94.992 negativos.
Silver, predictions e evaluation tiveram 100.000 linhas cada.

| Medida observada | Valor |
|---|---:|
| TP / FP / FN / TN | 5.008 / 0 / 0 / 94.992 |
| Precision / Recall / F1 / FPR | 1,0 / 1,0 / 1,0 / 0,0 |
| PR-AUC | 1,0 |
| Prevalência e taxa de alertas | 5,008% / 5,008% |
| Accuracy da baseline sempre-negativo | 94,992% |
| Positivos perdidos pela baseline | 5.008 |

Duas execuções com a mesma imagem e protocolo levaram 9,608 s e 9,917 s; os picos de memória observados
foram aproximadamente 0,86 GiB e 0,99 GiB sob limite Compose de 4 GiB e 2 CPUs. Esses tempos dependem do
equipamento. Os artefatos ficam no volume local em `/data/pixguard/analysis-runs/<analysis_run_id>/`; a execução
guarda a regra/configuração, cópia e hashes da origem, Parquet de predições e avaliação, métricas, resumo com
até vinte exemplos e manifesto de conclusão.

Uma limitação da FASE 3 é que o manifesto de origem não contém checksum histórico do Parquet Silver. A FASE 4
congela o SHA-256 dos arquivos vistos no começo e verifica que continuam iguais ao final da avaliação; isso
protege contra mudanças durante a execução, mas não prova retroativamente que a Silver nunca mudou antes dela.

Suba ou atualize a tela:

```bash
docker compose up -d --wait dashboard
```

Abra [http://localhost:8501](http://localhost:8501) e procure **Avaliação da regra — FASE 4**. A tela lê o run
concluído válido mais recente, mostra a matriz, métricas, baseline e razões de resultados indefinidos. Se não
houver análise válida, não inventa números. A imagem do dashboard não inclui PySpark e não executa a análise ao
renderizar; a CLI Spark é um processo separado do profile `pipeline`.

## O que estes resultados não dizem

* O cenário suspeito recebe por construção valores de R$ 10.000,00 a R$ 50.000,00; os demais ficam entre R$ 1,00
  e R$ 2.000,00. A regra observa justamente esse valor. A separação artificial explica as métricas perfeitas.
* Não houve teste cego, treinamento, ajuste de threshold nem validação com transações reais. Repetir o mesmo
  simulador comprova reprodução, não generalização.
* O pipeline continua offline e lê JSONL; esta fase não liga o gerador ao Kafka. Kafka/streaming é trabalho
  futuro da FASE 2.
* Nenhum modelo de ML foi criado. ML só fará sentido após hipótese e dados adequados, decisão de arquitetura e
  avaliação separada sem reaproveitar o conjunto de teste para ajustar a regra.
* A ADR 0005 permanece `proposed`. Os valores acima descrevem a configuração executada, não uma aprovação
  formal de política por Rafael.

Para revisão: por que precision pode ficar indefinida enquanto accuracy é alta? O que mudaria se os valores
positivos e negativos se sobrepusessem? Por que repetir a mesma seed não cria evidência de desempenho real?
