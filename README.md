# COVID-19 Real-Time Processing System

**Project Name:** `covid-realtime-processing-system`

A complete end-to-end real-time data pipeline designed to ingest, process, and analyze COVID-19 data streams. This project utilizes **Apache Kafka**, **Apache NiFi**, **Spark Structured Streaming**, and **SQLite** to calculate spread trends and hotspot scores in real-time.

## 🏗️ Architecture & Data Flow

```text
[kafka_json_producer.py] 
        ↓ (produces JSON lines)
[Kafka Topic: covid] 
        ↓ 
[Apache NiFi] (Cleansing, Validation, Enrichment)
        ↓ (publishes cleaned records)
[Kafka Topic: covid_cleaned] 
        ↓ 
[Spark Structured Streaming] (Real-time Analytics)
        ↓ 
[SQLite Database] (covid_analysis.db)
````

## 🚀 Features & Real-Time Analytics

This pipeline computes the following metrics on the fly:

  * **Recovered Cases Calculation:** `cumulative_total_cases - active_cases - cumulative_total_deaths`
  * **7-Day Moving Average:** Tracks the spread trend based on daily new cases.
  * **Hotspot Score:** `daily_new_cases / 7-day_avg`. A score \>1 indicates accelerating spread.
  * **Next-Day Prediction:** Simple 3-day rolling average forecast for the next day's cases.
  * **Data Enrichment:** Adds `processed_date` timestamps via NiFi.
  * **Deduplication:** Ensures uniqueness based on `(date, country)`.

## 📂 Repository Structure

```text
covid-realtime-processing-system/
│
├── kafka_json_producer.py        # Python Producer: Sends covid_data.json → Kafka "covid"
├── covid_data.json               # Raw source data (one JSON per line)
├── covid19_real_time_analysis.py # Spark Streaming consumer & analytics engine
├── covid_analysis.db             # Output SQLite DB (auto-generated)
├── nifi_flow_screenshot.png      # Visual reference of the NiFi flow
└── README.md                     # Project Documentation
```

## 🛠️ Tech Stack

  * **Apache Kafka:** Messaging backbone for decoupling data producers and consumers.
  * **Apache NiFi (v1.28.0):** Data routing, cleansing, validation, and enrichment gateway.
  * **Apache Spark Structured Streaming:** High-latency, high-throughput real-time analytics engine.
  * **SQLite:** Lightweight analytical store for final metrics.
  * **Python 3.9+:** Used for the data producer and Spark scripting.

-----

## 🌊 NiFi Flow Strategy

**Goal:** Ensure only valid, schema-compliant data reaches the analytics engine.

### Processor Breakdown

| Processor | Purpose | Key Configuration Details |
| :--- | :--- | :--- |
| **ConsumeKafkaRecord\_2.0** | Ingests raw JSON records. | Topic: `covid`, Reader: `JsonTreeReader`, Offset: `earliest` |
| **JoltTransformRecord** | Cleanses missing numeric fields. | Transforms nulls in `active_cases`, `daily_new_cases`, etc., to "0". |
| **UpdateRecord** | Adds ingestion timestamp. | Adds `/processed_date` → `${now():format('yyyy-MM-dd HH:mm:ss')}` |
| **ValidateRecord** | Schema validation. | **Strict Type Checking = true**. Ensures data conforms to schema. |
| **UpdateAttribute** | Routing flags. | Adds attribute `flag = success` for valid records. |
| **LogAttribute** | Debugging. | Logs payload + attributes (Info level). |
| **PublishKafkaRecord\_2.0** | Publishes clean data. | Topic: `covid_cleaned`. |

**Routing Logic:**

  * **Success:** Clean records are routed to `UpdateAttribute` → `PublishKafkaRecord`.
  * **Failure/Invalid:** Routed to dead-end queues for inspection.

-----

## ⚡ Spark Streaming Logic (`covid19_real_time_analysis.py`)

  * **Source:** Consumes from Kafka topic `covid_cleaned`.
  * **Processing:**
      * Processes micro-batches every **10 seconds**.
      * Merges incoming stream with existing SQLite historical data.
      * Computes derived metrics (Recovered, Hotspot Score, Moving Averages).
  * **Sink:** Overwrites the `covid_metrics` table in `covid_analysis.db` with the full dataset.
  * **Fault Tolerance:** Checkpointing is enabled.

## 📊 Database Schema (`covid_metrics`)

| Column | Description |
| :--- | :--- |
| `date` | Record date (YYYY-MM-DD) |
| `country` | Country name |
| `cumulative_total_cases` | Total confirmed cases |
| `daily_new_cases` | New cases reported that day |
| `active_cases` | Currently active cases |
| `cumulative_total_deaths` | Total deaths |
| `daily_new_deaths` | New deaths reported that day |
| `recovered_cases` | **Calculated:** Total - Active - Deaths |
| `spread_trend_7d_avg` | **Calculated:** 7-day moving average |
| `hotspot_score` | **Calculated:** Acceleration indicator (\>1 = hotspot) |
| `predicted_new_cases_next_day` | **Calculated:** Simple forecast |
| `processed_date` | NiFi ingestion timestamp |

-----

## ⚙️ How to Run

### 1\. Start Kafka & Create Topics

```bash
# Create input and output topics
kafka-topics.sh --create --topic covid --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics.sh --create --topic covid_cleaned --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 2\. Configure & Start NiFi

1.  Launch Apache NiFi.
2.  Import the flow (or recreate based on the screenshot/processor table).
3.  Start the **nifi-covid-group** process group.

### 3\. Run Data Producer

This script reads the raw JSON file and simulates a data stream into Kafka.

```bash
python kafka_json_producer.py
```

### 4\. Run Spark Analytics

Submit the Spark job with the required Kafka SQL package.
*(Note: Adjust the Spark version in the package name if necessary)*.

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 covid19_real_time_analysis.py
```

### 5\. View Results

Open `covid_analysis.db` using **DB Browser for SQLite** or a similar tool to view the real-time updates in the `covid_metrics` table.

-----

## 🔮 Future Improvements

  * [ ] Add Avro Schema Registry integration in NiFi.
  * [ ] Migrate from SQLite to PostgreSQL or ClickHouse for scalability.
  * [ ] Visualize data using Grafana + Prometheus.
  * [ ] Containerize the full stack using Docker Compose.
  * [ ] Implement advanced forecasting using Prophet or LSTMs.

## 📄 License

MIT License – Feel free to use, modify, and share.

```
```
