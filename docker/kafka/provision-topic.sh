#!/bin/bash
# Existing topics are inspected, never repaired or recreated implicitly.
set -euo pipefail
# Bound the whole operation, including Java metadata calls on an unavailable broker.
if [[ ${1:-} != --bounded ]]; then
    exec timeout --signal=TERM --kill-after=5s 90s /bin/bash "$0" --bounded
fi
# Issue #27 fixes the laboratory policy at 24h. Revalidate for each future
# publication with: docker compose run --rm kafka-topic-init
# Compose may reuse a previously successful one-shot container on later up calls.
bootstrap=${PIXGUARD_KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}
topic=${PIXGUARD_KAFKA_TOPIC:-pixguard.payment_event_v1}
if [[ ! "$topic" =~ ^[a-zA-Z0-9._-]+$ || "$topic" == '.' || "$topic" == '..' || ${#topic} -gt 249 ]]; then
    echo 'Invalid Kafka topic name' >&2
    exit 1
fi
bin=/opt/kafka/bin
"$bin/kafka-topics.sh" --bootstrap-server "$bootstrap" --create --if-not-exists \
    --topic "$topic" --partitions 1 --replication-factor 1 \
    --config retention.ms=86400000 --config cleanup.policy=delete
metadata=$("$bin/kafka-topics.sh" --bootstrap-server "$bootstrap" --describe --topic "$topic")
# Restrict the summary to the exact topic; --topic accepts regular expressions.
summary=$(printf '%s\n' "$metadata" | awk -v topic="$topic" '$1 == "Topic:" && $2 == topic && /PartitionCount:/ {print}')
if [[ $(printf '%s\n' "$summary" | awk '{for(i=1;i<=NF;i++) if($i=="PartitionCount:") print $(i+1)}') != 1 || \
      $(printf '%s\n' "$summary" | awk '{for(i=1;i<=NF;i++) if($i=="ReplicationFactor:") print $(i+1)}') != 1 ]]; then
    echo 'Kafka topic topology mismatch: expected partitions=1 replication=1' >&2
    exit 1
fi
configs=$("$bin/kafka-configs.sh" --bootstrap-server "$bootstrap" --entity-type topics \
    --entity-name "$topic" --describe --all)
for expected in retention.ms=86400000 cleanup.policy=delete; do
    if ! printf '%s\n' "$configs" | awk -v expected="$expected" '$1 == expected {found=1} END {exit !found}'; then
        echo "Kafka topic configuration mismatch: expected $expected" >&2
        exit 1
    fi
done
printf 'Verified topic=%s partitions=1 replication=1 retention.ms=86400000 cleanup.policy=delete\n' "$topic"
