$ErrorActionPreference = 'Stop'
docker compose up -d
Start-Sleep -Seconds 8
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker exec kafka /opt/kafka/bin/kafka-features.sh --bootstrap-server localhost:9092 describe
docker exec kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter --entity-type groups --entity-name demo-share --add-config share.auto.offset.reset=earliest,share.record.lock.duration.ms=5000
Write-Host 'Run the CLI, select 1, 2, 3 and 4 to exercise the live demo.'
