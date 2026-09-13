# Waseet Logistics - starter folder

The brief is the project document your instructor handed out. Read that first. This file is only about getting
the stack running and knowing where things are.

## Before you start

Docker Desktop, running. Then, from this folder:

```
docker compose up -d --build
```

The first build pulls the Airflow image and takes a few minutes. Watch it come up:

```
docker compose logs -f airflow
```

Wait for the line that says Airflow is ready, then `Ctrl-C` out of the logs - that stops watching,
not the container.

Check both halves:

```
docker compose exec -T airflow airflow version
docker compose exec -T postgres psql -U de -d waseet -c "\dt"
```

The first prints `2.10.5`. The second prints four tables - `hubs`, `couriers`, `service_levels`,
`customers` - because the fact table and the run log are yours to write.

Airflow's UI is at http://localhost:8081, username `admin`, password `admin`.

The warehouse is at `localhost:5442` from your laptop, database `waseet`, user `de`, password `de`.

## Two addresses for one database

Inside the containers the warehouse is `postgres:5432`. From your laptop it is `localhost:5442`. Both
are correct; they are correct from different places, and mixing them up is the first thing to check
when a connection refuses.

`pipeline.py` reads `WASEET_DB_URL` from the environment, and `docker-compose.yml` sets it to the
in-container form. Run the same file from your terminal and it falls back to the laptop form. That is
the whole trick.

## Ports

5442 and 8081, so that Lecture 14's stack can stay up on 5432 and 8080. If either is busy, change the
left-hand side of the mapping in `docker-compose.yml`.

## Layout

```
data/           the scan files, the lookups, the history file
api/            the customer API and its records
init/           SQL that runs once, on first start, on an empty volume
pipeline/       pipeline.py, quality.py, ingest_customers.py - stubs, yours to finish
dags/           waseet_daily.py - mounted into the container, edit it from here
spark/          history_job.py - runs on your laptop, not in the container
```

`dags/`, `pipeline/` and `data/` are bind-mounted into the Airflow container. Edit them here in your
editor; there is no copying step and no rebuild.

## Changing the schema after the first start

`init/` only runs on an empty volume. Once Postgres has started once, editing the SQL does nothing.
To apply a change:

```
docker compose down -v
docker compose up -d
```

`down -v` deletes the volume and everything in the warehouse. You will do this several times while
you settle the schema, which is a good argument for getting your loads reproducible early.

## If something is wrong

**Port already allocated** - something else owns 5442 or 8081. Change the mapping.

**The airflow container restarts in a loop** - `docker compose logs airflow`. It is almost always
Postgres not being ready on the first attempt; the healthcheck usually prevents it, and a
`docker compose restart airflow` fixes it when it does not.

**Permission errors on `airflow_logs/`, on macOS or Linux** - the container runs as uid 50000.
`echo "AIRFLOW_UID=$(id -u)" > .env`, then `docker compose down && docker compose up -d`.

**A DAG does not appear** - `docker compose exec -T airflow airflow dags list-import-errors`. A DAG
file with a syntax error is not a DAG that failed; it is a DAG that does not exist.

## Tidying up at the end

```
docker compose d