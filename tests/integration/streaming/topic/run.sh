#!/bin/sh
# Dedicated project per run: preserve its evidence volume, never use main volumes.
set -eu
# Do not inherit a developer endpoint or business topic into destructive fixtures.
export PIXGUARD_KAFKA_BOOTSTRAP_SERVERS=kafka:9092
export PIXGUARD_KAFKA_TOPIC=pixguard.payment_event_v1
project="pixguard-stream002-$(date +%s)-$$"
echo "Isolated evidence volume: ${project}_kafka_data"
compose() {
    docker compose -p "$project" -f docker-compose.yml "$@"
}
trap 'compose down' EXIT
compose config --quiet
compose up -d --wait kafka
compose run --rm kafka-topic-init
compose run --rm kafka-topic-init
# Each divergence must fail and remain untouched by the provisioner.
for config in retention.ms=3600000 cleanup.policy=compact; do
    compose exec -T kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka:9092 \
        --entity-type topics --entity-name pixguard.payment_event_v1 --alter --add-config "$config"
    if compose run --rm kafka-topic-init; then
        echo "Unexpected success for divergent $config" >&2
        exit 1
    fi
    compose exec -T kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka:9092 \
        --entity-type topics --entity-name pixguard.payment_event_v1 --describe --all | \
        awk -v expected="$config" '$1 == expected {found=1} END {exit !found}'
    compose exec -T kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka:9092 \
        --entity-type topics --entity-name pixguard.payment_event_v1 --alter \
        --add-config retention.ms=86400000,cleanup.policy=delete
done
compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
    --alter --topic pixguard.payment_event_v1 --partitions 2
if compose run --rm kafka-topic-init; then
    echo 'Unexpected success for divergent partition count' >&2
    exit 1
fi
compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
    --describe --topic pixguard.payment_event_v1 | awk '/PartitionCount: 2/ {found=1} END {exit !found}'
echo 'PASS: creation, rerun, retention, policy and partition mismatch without repair'
