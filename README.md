# Vehicle IoT Telemetry Pipeline: Real-Time Analytics on Azure

End-to-end real-time analytics platform for a commercial fleet (Fleetage Mobility use case). Simulated vehicle telemetry is streamed through **Confluent Cloud Kafka** and processed with **Apache Flink** to detect overspeeding, abnormal fuel drops and temperature deviations. A **hot path** sends SMS/phone alerts to drivers through **Azure Functions and Twilio**, and a **cold path** archives events to **Azure Data Lake Gen2**, transforms them with **Azure Data Factory**, and models them in **Azure Synapse** for BI dashboards.

**Tech stack:** Confluent Cloud Kafka · Apache Flink · Azure Functions · Twilio API · Azure Data Lake Gen2 · Azure Data Factory · Azure Synapse Analytics · Parquet · Delta · Python

## Architecture

### Phase 1: Hot path (real-time alerting)

![Hot path architecture](images/hot_path.png)

`Vehicle telemetry → Kafka (Telemetry topic) → Flink → Alert Topics → HTTP Sink → Azure Function → Twilio → Driver`

### Phase 2: Cold path (historical analytics)

![Cold path architecture](images/cold_path.png)

`Flink alert topics → Blob Sink → ADLS Gen2 (Bronze → Silver → Gold) → Azure Data Factory → Azure Synapse → BI dashboards`

## Anomaly detection rules (Flink)

| Rule | Condition | Meaning |
|------|-----------|---------|
| Overspeeding | `speed > 80` | Vehicle exceeding speed limit |
| Low fuel / abnormal fuel drop | `fuel < 10%` | Fuel level critically low |
| Temperature deviation | `temp > 200` | Engine temperature too high |

Flagged events are routed to alert topics, which drive both the real-time alerts and the archival path.

## Phase 1: Hot path

- Telemetry is produced by a Python simulator and published to a Kafka topic on Confluent Cloud.
- A Flink job applies the three rules above to the stream and writes flagged events to alert topics.
- An HTTP sink connector triggers an **Azure Function**, which sends SMS/phone alerts to the driver via **Twilio**.
- Target: alerts delivered **within 5 seconds** of detection.

## Phase 2: Cold path

- A Blob sink archives alert events from Kafka to **ADLS Gen2**.
- **Bronze:** raw events as archived.
- **Silver:** transformed and stored as Parquet.
- **Gold:** aggregated Delta tables.
- **Azure Data Factory** pipelines handle the ETL and prepare curated datasets.
- **Azure Synapse Analytics** holds the SQL fact and dimension models used by BI dashboards.
- Synapse can also trigger a real-time action downstream.

## Deliverables

| Phase | Deliverable |
|-------|-------------|
| 1 | Kafka ingestion setup (topics and producer) |
| 1 | Flink real-time processing job (anomaly detection) |
| 1 | Azure Functions + Twilio alerting |
| 1 | Working POC with simulated live telemetry |
| 2 | ADLS Gen2 storage layout (Bronze / Silver / Gold) |
| 2 | Azure Data Factory ETL pipelines |
| 2 | Synapse fact/dimension models |
| 2 | BI dashboards (operational and historical) |

## Acceptance criteria

- **Phase 1:** real-time ingestion works, anomalies are detected correctly, and alerts arrive within 5 seconds.
- **Phase 2:** data is stored reliably, ETL pipelines run, Synapse models are validated, and dashboards show accurate KPIs.

## Repository structure

<!-- Update this tree to match your repo -->
```
.
├── images/            # architecture diagrams
├── producer/          # telemetry simulator
├── flink/             # Flink job and anomaly rules
├── azure_function/    # alert function (Twilio)
└── README.md
```

## How to run

1. Create a Confluent Cloud cluster and the `Telemetry` and alert topics.
2. Run the telemetry producer to stream simulated vehicle data.
3. Deploy the Flink job to apply the anomaly rules.
4. Deploy the Azure Function and add your Twilio credentials as application settings (never commit secrets).
5. Configure the HTTP sink connector to call the Azure Function.
6. For the cold path, set up the Blob sink, ADLS Gen2 containers, ADF pipelines and Synapse models.

## Author

**Jaswanth Kumar Dowpati**: [LinkedIn](https://www.linkedin.com/in/jaswanth-dowpati-694336340) · [GitHub](https://github.com/jaswanthdowpati35-maker)
