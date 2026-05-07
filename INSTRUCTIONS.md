Требуется

- Docker и Docker Compose
- (Опционально) `pgcli` или `psql` для проверки Postgres

Запуск (полный цикл)

1) Сброс ранее созданных контейнеров и томов (чтобы инициализация БД сработала чисто):

```bash
docker-compose down -v
```

2) Собрать и запустить стек сервисов:

```bash
docker-compose up -d --build
```

3) Подождать и убедиться, что `producer` завершил отправку всех данных в Kafka.
   - Посмотрите логи сервиса `producer` (в том же каталоге, где `docker-compose.yml`):

```bash
docker-compose logs -f producer
```

   - Ждём строку вида: `✓ Finished sending all X rows to Kafka!`.
   - Если вы не используете `docker-compose` для логов, можно смотреть контейнер напрямую:

```bash
docker ps
# найти container id/name для producer
docker logs -f <producer-container-name>
```

4) Когда `producer` отправил все данные, запустить Flink-джобу:

```bash
docker exec -it flink_jobmanager flink run -py /opt/flink_app/flink_job.py
```

Примечание: если вы запускаете эту команду слишком рано (producer ещё не отправил данные), таблицы могут оказаться пустыми. Подождите завершения producer и запустите джобу снова.

Проверка содержимого таблиц в PostgreSQL

Postgres доступен на хосте `localhost:5434` (в нашем окружении). Подключение с помощью `pgcli`:

```bash
pgcli postgresql://flinkuser:flinkpassword@localhost:5434/flinkdb
```

Либо добавьте новое подключение postgres в dbeaver (порт: 5434б, пользователь: flinkuser, пароль: flinkpassword, база: flinkdb)