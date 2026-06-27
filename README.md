# message-broker

Небольшой gRPC-брокер очередей на Rust.

## Запуск

```bash
cargo run
```

По умолчанию слушает `127.0.0.1:50051`.

Переменные окружения:

```bash
BROKER_ADDR=127.0.0.1:50051
BROKER_DATA_DIR=./data
```

Если `BROKER_DATA_DIR` не задан, брокер работает в памяти.

## Проверка

```bash
cargo test
cargo bench
```

## Бенчмарки

`cargo bench`, 2026-06-27:

| benchmark | время | throughput |
| --- | ---: | ---: |
| publish | 950.85 ns | 1.0517 Melem/s |
| publish_pull_ack | 2.3691 us | 422.10 Kelem/s |
| batch_publish_pull_ack_100 | 206.66 us | 483.89 Kelem/s |
| pull_nack_requeue | 1.1705 us | 854.31 Kelem/s |

Описание gRPC API лежит в `proto/broker.proto`.
