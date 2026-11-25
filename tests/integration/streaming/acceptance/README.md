<h1 align="center">Aceite do transporte Kafka</h1>

<div align="center">
  <img alt="Apache Kafka" height="72" width="80" src="https://cdn.simpleicons.org/apachekafka/231F20">
</div>

Execute na raiz, em um projeto Compose novo com volumes vazios. Os comandos usam a seed 42
e o tópico padrão com uma partição. Deixe espaço em disco para guardar os recibos, os segmentos
e os snapshots de todas as execuções. Se já tiver usado o nome abaixo, escolha outro.

```bash
dc() { docker compose -p pixguard-acceptance -f docker-compose.yml -f tests/integration/streaming/acceptance/compose.yaml "$@"; }
app() { dc run --rm --no-deps app "$@"; }

dc build app
dc up -d --wait kafka
dc run --rm kafka-topic-init
app pixguard generate --count 100000 --seed 42
app pixguard publish --batch-id clean-001 --timeout 900
app pixguard capture --capture-id clean-001 --start 0 --end 100000
app pixguard export-capture --capture-id clean-001 --snapshot-id clean-001

app pixguard publish --batch-id repeat-001 --timeout 900
app pixguard capture --capture-id cumulative-001 --start 0 --end 200000
app pixguard export-capture --capture-id cumulative-001 --snapshot-id cumulative-001

app pixguard capture --capture-id replay-001 --start 0 --end 100000
app pixguard export-capture --capture-id replay-001 --snapshot-id replay-001
```

Confira nos dois manifestos de publicação 100.000 confirmações, zero incertos e zero não enviados.
Valide também o conteúdo, os hashes, a soma em BRL e as contagens reconciliadas de cada faixa.

```bash
for id in clean-001 cumulative-001 replay-001; do
  physical=100000
  if [ "$id" = cumulative-001 ]; then physical=200000; fi
  app python tests/integration/streaming/acceptance/verify_batch.py \
    --source /data/pixguard/events.jsonl \
    --capture "/data/pixguard/captures/$id" \
    --snapshot "/data/pixguard/captures/$id/snapshots/$id" \
    --expected-count 100000 --expected-physical "$physical"
done
```

O verificador espera 100.000 eventos válidos nos três snapshots e 100.000 duplicatas
na captura cumulativa. As somas usam Decimal. O hash do multiconjunto canônico
permite comparar o conteúdo mesmo quando a origem e o snapshot usam serializações diferentes.

O roteiro abaixo usa outro projeto Compose para exercitar quarentena, retomada depois
de falhas injetadas, expiração de offsets e indisponibilidade inicial do broker.

```bash
bash tests/integration/streaming/acceptance/run.sh
docker compose -p pixguard-acceptance-executor down
```

Interromper uma captura em andamento permite retomar com o mesmo ID, tópico e faixa.
Preserve o volume e o grupo do Kafka. Uma publicação interrompida exige conferir seu manifesto
e recibos antes de republicar. Para encerrar o ambiente do lote e conservar as evidências,
execute `dc down`.

A suíte de aceite exige `PIXGUARD_STREAM007_ACCEPTANCE=1`, definido pelo runner.
Para testar o código sem broker, execute `app pytest -q tests/unit tests/contracts`.
As integrações Kafka anteriores mantêm seus próprios pré-requisitos e comandos.
O runner usa o código e os testes do checkout montados somente para leitura.
As falhas de disco são injetadas no adaptador; não é necessário esgotar o disco real.
O commit do broker só ocorre após fsync do lote (até 100 registros), e sua resposta
precisa confirmar tópico, partição e próximo offset antes de atualizar o manifesto.
O exportador usa ordenação externa em disco; reserve espaço temporário também em `/tmp`.
