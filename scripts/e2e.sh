#!/usr/bin/env bash
set -euo pipefail

docker compose up -d
until docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do
  sleep 2
done

docker exec kafka /opt/kafka/bin/kafka-features.sh --bootstrap-server localhost:9092 describe | tee /tmp/kafka-share-features.txt
if ! grep -q 'share.version.*1' /tmp/kafka-share-features.txt; then
  docker exec kafka /opt/kafka/bin/kafka-features.sh --bootstrap-server localhost:9092 upgrade --feature share.version=1
fi
docker exec kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter \
  --entity-type groups --entity-name demo-share \
  --add-config share.auto.offset.reset=earliest,share.record.lock.duration.ms=5000
echo 'Broker is ready. Use the CLI to create orders, produce records, and start workers.'
