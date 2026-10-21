# AirCheck: Streaming Ingestion

Live air-quality ingestion from the sensor.community network into a governed bronze layer in Unity Catalog, through Azure Event Hubs.

> Team project, forked from [dan-vader/aircheck](https://github.com/dan-vader/aircheck). This README covers only the streaming part, which is my work.

---

## 1. Topic

[sensor.community](https://sensor.community/en/) (formerly Luftdaten) is a global citizen-science network. Volunteers build DIY sensors and publish the readings as open data. A live API returns the most recent measurements (roughly the last 5 minutes) from thousands of sensors.

This part of the project turns that live feed into a continuously updated bronze table:

```
sensor.community API -> Producer (local) -> Azure Event Hub -> Consumer (Databricks) -> bronze.sensor_live_raw
```

The scope is deliberately limited to ingestion. Nothing is cleaned or aggregated here. Records are stored as they arrive, with technical metadata and a safety net for fields nobody expected.

---

## 2. Analytical Questions

The stream exists to answer questions that need fresh data rather than a daily archive:

- How many sensors are reporting right now, and in which countries?
- What are the current PM10 and PM2.5 levels, and how did they change over the last minutes and hours?
- Where are the most polluted locations at this moment?
- How do Ukraine, Poland and Germany compare right now?
- Which readings exceed the WHO daily guideline values (PM10 above 50 µg/m³, PM2.5 above 25 µg/m³)?
- When did a new type of measurement (temperature, humidity, pressure) first appear in the feed, and did the pipeline absorb it without breaking?

The last question is the reason the schema-evolution scenario exists (section 4.5).

---

## 3. Data

**Source:** `https://data.sensor.community/airrohr/v1/filter/type=<types>&country=<countries>`

**Scope:** sensor types `SDS011` (Phase A) and `SDS011,BME280` (Phase B), countries `UA, PL, DE`. A single poll returns several thousand records (around 9,000 in practice).

**Grain:** one record is one measurement event from one sensor at one timestamp.

### Raw record shape

```json
{
  "id": 6489703325,
  "timestamp": "2021-08-14 16:09:32",
  "sensor": { "id": 489, "sensor_type": { "id": 14, "name": "SDS011" } },
  "location": { "id": 231, "latitude": "48.12", "longitude": "11.576", "country": "DE" },
  "sensordatavalues": [
    { "value_type": "P1", "value": "11.53" },
    { "value_type": "P2", "value": "8.70" }
  ]
}
```

`sensordatavalues` is an array of `{value_type, value}` pairs. Its shape is identical for every sensor type; only the set of `value_type` values differs.

### Bronze table: `sensor_live_raw`

| Column | Type | Description |
|---|---|---|
| `id` | string | Measurement ID from the API |
| `timestamp` | string | Measurement time (UTC) as reported by the sensor |
| `sensor_id` | string | Sensor identifier |
| `sensor_type` | string | Sensor model, for example `SDS011`, `BME280` |
| `location_id` | string | Location identifier |
| `latitude`, `longitude` | string | Sensor coordinates |
| `country` | string | ISO country code |
| `P1` | string | PM10, µg/m³ |
| `P2` | string | PM2.5, µg/m³ |
| `temperature` | string | °C (appears in Phase B) |
| `humidity` | string | % relative humidity (appears in Phase B) |
| `pressure` | string | Pa (appears in Phase B) |
| `_rescued_data` | string | JSON of fields not covered by the active schema |
| `_source` | string | Always `sensor_live_eh` |
| `_source_partition`, `_source_offset` | string | Event Hub partition and offset of the source event |
| `_ingested_at` | timestamp | Time the row was written to bronze |

Values stay as strings in bronze on purpose. Typing and cleaning belong to silver.

### Characteristics of the real data

These were observed during development and shaped the design:

- **Sensor types are not clean.** Some stations labelled `SDS011` send only climate readings (temperature, humidity, pressure) and no `P1`/`P2`. The `sensor_type` label does not reliably predict which fields a record contains.
- **Extra fields appear.** `PMS7003` sensors send an additional `P0` value, and `BME280` sensors send `pressure_at_sealevel`. Neither is in the schema, so both land in `_rescued_data` instead of being lost.
- **Volume is bursty.** One poll can return thousands of records at once, so the consumer caps how much it reads per micro-batch.
- **Freshness is limited by the source.** The API only exposes the last few minutes, so the poll interval trades completeness against load on a volunteer-run service.

---

## 4. My Work: Streaming Ingestion

### 4.1 Data flow and arrival rate

| Hop | Model | Frequency |
|---|---|---|
| API -> Producer | **Pull.** The API is request/response only, with no push or webhook, so the producer polls it. | Every 90 seconds (`poll_interval_sec`) |
| Producer -> Event Hub | **Push.** The producer sends events to the hub in batches. | After every poll |
| Event Hub -> Consumer | **Pull.** Structured Streaming polls the hub through the Kafka-compatible endpoint. | Micro-batch every 30 seconds, up to 5,000 events each |

- One poll returns about 9,000 records, so the average load is roughly 100 events per second. It arrives in bursts, one burst per poll, not as a smooth flow.
- The consumer can read up to 5,000 events per 30-second trigger, which is about 15,000 per poll interval. That is above the roughly 9,000 that arrive, so it keeps up and drains each burst within a minute or so.
- The stream is therefore near real time with a latency of about a minute or two: up to 90 seconds of polling plus up to 30 seconds of micro-batching.
- The API exposes roughly the last 5 minutes while the producer polls every 90 seconds, so consecutive responses can overlap. The producer does not deduplicate, so the same measurement `id` can be written to bronze more than once. Delivery is at-least-once, and removing duplicates is left to silver.

### 4.2 Producer

`ingestion/streaming/eventhub_producer.py` is the single entry point. Everything under `ingestion/streaming/producer/` is imported by it and is not run separately.

| Module | Responsibility |
|---|---|
| `_01_config.py` | Loads the chosen environment from the shared config file |
| `_02_secrets.py` | Reads secrets from Key Vault in parallel (`asyncio.gather`) |
| `_03_sensor_api.py` | Builds the filter URL and fetches data with retry and backoff |
| `_04_transform.py` | Pivots `sensordatavalues` into named top-level fields |
| `_05_eventhub_sender.py` | Batches events and sends them to Event Hub |
| `_06_shutdown.py` | Graceful shutdown on SIGINT / SIGTERM |

**Where it runs.** The producer was developed and runs locally on a laptop. In a real deployment it would run as a long-lived process on a VM, since it is a continuous poller and a live feed should not depend on one personal machine. The code is already suited for that:

- Authentication goes through `DefaultAzureCredential`, which uses the Azure CLI login locally and Managed Identity on a VM, with no change to the code.
- Secrets come from Key Vault, and the config holds only secret names.
- It handles SIGTERM, so it can run under a service manager that stops and restarts it cleanly.
- Environment, config path and sensor filter are passed as command-line arguments.

### 4.3 Event Hub

| Setting | Value | Reason |
|---|---|---|
| Entity | `aircheck-sensor-readings` | In the shared namespace |
| Partitions | 2 | See below |
| Retention | 48 hours | Lets a stopped consumer catch up after a break |
| Cleanup policy | Delete | Every reading matters; compaction would keep only the latest value per key |
| Auth | SAS policy (Send + Listen) | Connection string stored in Key Vault |

**Partitioning.** Two partitions are enough here:

- The load of around 100 events per second is far below what one partition can ingest, so partitions are not needed for throughput. Two give some headroom, a second unit of parallelism for the consumer, and the ability to keep working across a partition issue.
- Events are sent without a partition key, so the SDK spreads batches across partitions. Without a key there is no hot partition, and data skew is not expected.
- Per-sensor ordering is not required: the sensor timestamp is in the event, and ordering is not needed for bronze.

### 4.4 Consumer

`ingestion/streaming/load_bronze_streaming.py` is a Databricks notebook (Source format), intended to run as a continuous Databricks Job.

- Reads Event Hub through its Kafka-compatible endpoint with Structured Streaming. Auto Loader is not used because the data does not land as files.
- Parses the JSON payload with `from_json` and `rescuedDataColumn`, so unexpected fields end up in `_rescued_data` instead of being dropped.
- Writes to `sensor_live_raw` in append mode with `mergeSchema`, a 30-second trigger, and `maxOffsetsPerTrigger` to bound each micro-batch.
- Uses `startingOffsets: earliest`, so a first run or a lost checkpoint replays whatever Event Hub still retains (48 hours).
- Ends with `query.awaitTermination()`. Without it, a notebook run as a Job finishes immediately and takes the stream down with it.
- Keeps its checkpoint in the bronze volume under `_checkpoints/sensor_live`.

### 4.5 Schema Evolution

The schema is assembled from the config, so no field names are hardcoded in the notebook:

- **Phase A:** common fields plus `base_measurement_fields` (`P1`, `P2`).
- **Phase B:** Phase A plus `phase_b_extra_fields` (`temperature`, `humidity`, `pressure`).

Demo flow:

1. Phase A runs with the producer filter `SDS011`. The table has `P1` and `P2` only.
2. Stop the consumer. Set its `sensor_filter` widget to `SDS011,BME280` and restart the producer with `--sensor-filter "SDS011,BME280"`.
3. The consumer starts with the wider schema. `mergeSchema` adds `temperature`, `humidity` and `pressure` as real columns, and existing rows get `NULL` in them.
4. `DESCRIBE TABLE` shows the new columns, and `DESCRIBE HISTORY` shows the point where the stream restarted and the schema changed.

A streaming query fixes its schema when it starts, so a restart is required to change it. This comes from Structured Streaming itself and is not a workaround. Fields outside both schemas (such as `P0` or `pressure_at_sealevel`) keep flowing into `_rescued_data` in both phases.

A generic `map<string,string>` column would avoid listing any field names, but it would never show a new column in `DESCRIBE TABLE`. For that reason the project uses explicit, config-driven columns.

### 4.6 Key Technical Decisions

| Decision | Why |
|---|---|
| **Poll the API, push to Event Hub** | The source only supports request/response. Event Hub decouples polling from consumption, so the consumer can stop and catch up from retained events. |
| **Pivot, not flatten** | `sensordatavalues` has the same shape for every sensor type. Forwarding it as is, or splitting it into one row per value, would never add a column when a new sensor type appears. Pivoting each `value_type` into its own top-level field changes the event shape when a new sensor type appears, which is what schema evolution reacts to. |
| **Retry only when it can help** | `tenacity` with exponential backoff, jitter and a cap, applied only to network errors, HTTP 429 and 5xx. Other 4xx errors are logged immediately, so a broken request is visible instead of being hidden behind retries. |
| **Two partitions, no partition key** | Load is far below partition capacity and per-sensor ordering is not needed. Round-robin distribution avoids hot partitions and skew. |
| **48-hour retention, delete policy** | Gives the consumer two days to recover. Compaction would keep only the latest value per key and lose readings. |
| **Kafka endpoint instead of Auto Loader** | The data is a stream of events, not files. |
| **Rescued data column** | Unexpected fields are kept, not dropped, and can be promoted to real columns later. |
| **Strings in bronze** | Bronze stores what arrived. Typing and cleaning belong to silver. |
| **Config-driven schema** | A new sensor field is added through config, and it becomes a real column visible in `DESCRIBE TABLE`. |
| **`maxOffsetsPerTrigger` and a 30-second trigger** | Bounds micro-batch size for bursty polls while keeping latency low. |
| **At-least-once delivery, no dedup in bronze** | Bronze stays raw and append-only. Retries and overlapping polls can create duplicates, which are handled downstream by `id`. |
| **Secrets in Key Vault, never in code or config** | The same code runs locally and on a VM through `DefaultAzureCredential`. |
| **Graceful shutdown** | The current poll cycle finishes before the process exits, so no batch is cut in half. On Windows only Ctrl+C (SIGINT) is available. |
| **Accurate counters** | `events_sent` counts only batches that were actually sent successfully. |

### 4.7 Configuration

All settings live in the shared config file, under `streaming`:

```yaml
streaming:
  poll_interval_sec: 90
  max_retries: 3
  retry_backoff_base: 2
  request_timeout_sec: 30
  sensor_type_filter: "SDS011"
  country_filter: "UA,PL,DE"
  checkpoint_path: "/Volumes/<catalog>/<bronze_schema>/raw/_checkpoints/sensor_live"
  target_table: "sensor_live_raw"
  max_offsets_per_trigger: 5000
  base_measurement_fields: ["P1", "P2"]
  phase_b_extra_fields: ["temperature", "humidity", "pressure"]
```

Related top-level keys: `key_vault_name`, `secret_scope`, `secrets.contact_email`, `secrets.eventhub_conn_str`, `eventhub_namespace`, `eventhub_name`.

### 4.8 How to Run

**Producer (local)**

```
az login
pip install httpx pyyaml azure-eventhub azure-identity azure-keyvault-secrets tenacity
cd ingestion/streaming
python eventhub_producer.py --env dev
```

Phase B: `python eventhub_producer.py --env dev --sensor-filter "SDS011,BME280"`

Optional: `--config-path` overrides the default `../../config/aircheck.yaml`.

**Consumer (Databricks)**

Run `load_bronze_streaming` as a notebook or through the continuous Job. Widgets: `env` (default `dev`), `config_path`, and `sensor_filter` (empty for Phase A, `SDS011,BME280` for Phase B).

**Check the result**

```sql
SELECT sensor_type, P1, P2, temperature, humidity, pressure, _rescued_data, _ingested_at
FROM <catalog>.<bronze_schema>.sensor_live_raw
ORDER BY _ingested_at DESC
LIMIT 20;

DESCRIBE TABLE <catalog>.<bronze_schema>.sensor_live_raw;
DESCRIBE HISTORY <catalog>.<bronze_schema>.sensor_live_raw;
```

After switching phases, let the consumer run for a few minutes before checking. It may still be working through backlog, and stopping the cell early makes it look as if nothing was written.

### 4.9 Known Limitations

- **Duplicates in bronze.** Overlapping polls and producer retries can write the same `id` more than once.
- **No streaming observability yet.** There is no `StreamingQueryListener` or alerting on failed micro-batches. This is the next step.
- **`earliest` with short retention.** If the checkpoint is lost, only what Event Hub still holds (48 hours) can be replayed.
- **Schema changes need a restart.** Phase B field names are defined in config, so a new sensor type means editing the config and restarting the consumer.
- **Local producer.** The producer runs on a laptop, so a live demo depends on that machine being online. A VM with Managed Identity would remove that dependency.
- **No partition key.** Ordering of events from one sensor is not guaranteed inside Event Hub.
- **Sensor labels are unreliable.** Queries should not assume that `sensor_type` determines which measurement columns are populated.
