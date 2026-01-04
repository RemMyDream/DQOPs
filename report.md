***Abstract***

Financial markets generate large volumes of high-frequency and heterogeneous data from multiple sources, including real-time stock prices and global news events. This project presents a Big Data Storage and Processing architecture for integrating and analyzing financial data from sources such as Stooq, Finnhub, and GDELT. The proposed system supports both real-time stream processing and batch processing to enable low-latency analytics and large-scale historical analysis. A multi-layered data storage approach is employed to manage the data lifecycle from raw to processed data. The architecture is designed with a focus on scalability, performance, data quality, and fault tolerance, while also supporting data governance and reproducibility for financial analytics and forecasting.

# **1\.   PROBLEM DEFINITION**

## **1.1. Problem**

In the financial market, particularly in the stock market, data is generated in large volume, at high velocity, and high variety. Stock prices change in real time through APIs such as Stooq and Finnhub, while macroeconomic factors, news, and global events are reflected through GDELT data in semi-structured and unstructured formats. The integration and unified analysis of these heterogeneous data sources play a crucial role in market trend analysis, risk detection, and investment decision support.

However, storing and processing financial data at scale presents several significant challenges:

* Collecting both real-time and historical data from multiple heterogeneous sources with non-uniform data formats.  
* Ensuring data consistency and system scalability as data volume continuously grows over time.  
* Processing streaming and batch data in parallel to support both real-time analytics and in-depth offline analysis.  
* Managing the data lifecycle from raw data to cleaned, standardized, and aggregated datasets.  
* Supporting low-latency analytical queries for dashboards while also providing high-quality data for machine learning models in financial forecasting.  
* Tracking metadata, data lineage, and data quality to ensure governance, reproducibility, and reliability of analytical results.

Therefore, the problem addressed in this work is:

To design and implement a scalable Big Data Lakehouse architecture capable of ingesting, storing, processing, and analyzing large-scale financial data in both real-time and batch modes, while supporting data visualization and machine learning–based analytics in the context of the stock market.

## **1.2. Architecture Overview**

To address the challenges of processing high-velocity stock market data alongside massive historical news datasets, the system implements a **Lambda Architecture**. This hybrid approach decomposes the problem into three distinct layers—Batch, Speed, and Serving—allowing the system to balance data consistency (accuracy) with low-latency availability (real-time).

The architecture is organized into four logical stages:

* **Hybrid Data Ingestion:** The system employs a bifurcated strategy to handle heterogeneous data sources. High-volume historical data (GDELT, Stooq) is ingested directly into the Data Lake for bulk processing, while high-velocity real-time data (Finnhub) flows through a transactional buffer and Change Data Capture (CDC) pipeline to ensure immediate availability and data integrity.  
* **Batch Layer (Master Dataset):** Utilizing a **Lakehouse** pattern, this layer manages the immutable master dataset stored in an object store (MinIO). It processes large-scale historical data using distributed computing (Apache Spark) to create a refined "Medallion" structure (Bronze, Silver, Gold), ensuring a comprehensive and accurate view of historical trends.  
* **Speed Layer (Real-time Views):** This layer handles new data streams that have not yet been processed by the Batch Layer. It leverages a streaming backbone (Apache Kafka) to capture and process market ticks in near real-time, minimizing the latency gap between data generation and availability.  
* **Unified Serving Layer:** To provide a single access point for analytics, the architecture unifies outputs from both the Batch and Speed layers into a high-performance OLAP engine (ClickHouse). This allows downstream applications—such as dashboards (Superset, Grafana) and machine learning models—to query both historical depth and real-time freshness seamlessly.

# **2\. ARCHITECTURE and DESIGN**

## **2.1. Data Ingestion**

Data ingestion is the first and fundamental component of the proposed big data architecture. Its primary objective is to collect data from multiple heterogeneous external sources, ensure data reliability and consistency, and deliver the data to downstream batch and streaming processing pipelines.

### **2.1.1. Data Source**

The system ingests data from three main external APIs related to the financial domain:

* **Stooq API:** Provides **Daily OHLCV** (Open, High, Low, Close, Volume) data for historical analysis, serving as the "ground truth" for calculating returns and volatility. The scope for this project covers NVIDIA (NVDA) and Microsoft (MSFT) from January 1, 2020, to the present.  
* **GDELT Project:** Supplies the **GDELT 2.0 Global Knowledge Graph (GKG)**, a massive database for global news and event monitoring. To efficiently handle the raw, multi-terabyte dataset, the architecture leverages **Google BigQuery** for server-side SQL filtering, significantly reducing data retrieval time and storage costs.  
* **Finnhub API**: provides near real-time stock market data, which is periodically fetched every 5 minutes to support streaming and real-time analysis.

These data sources differ in terms of update frequency, data volume, and timeliness requirements, which motivates the adoption of a hybrid ingestion strategy.

### **2.1.2. Batch Data Ingestion**

Batch-oriented data sources, including GDELT and historical stock data from Stooq, are periodically extracted from source and processed using Apache Spark. This batch ingestion pipeline is designed to handle large volumes of historical data efficiently and is well-suited for analytical workloads such as aggregation, feature extraction, and trend analysis.

The processed data is then stored in the data lake following the Medallion Architecture (Bronze, Silver, Gold), which enables structured data refinement and long-term storage.

### **2.1.3. Streaming Data Ingestion** 

**PostgreSQL as a Staging and Buffering Layer**

All raw data collected from the external APIs is first ingested into PostgreSQL, which serves as a staging database and a single source of truth for the system. The use of PostgreSQL in the ingestion layer provides several advantages:

* Ensures data durability and fault tolerance in case of API failures or network issues.  
* Allows schema normalization and validation before data enters the big data ecosystem.  
* Supports data replay and reprocessing, which is essential for batch analytics.  
* Enables a unified data source for both batch and streaming pipelines.

In particular, data from the Finnhub API is ingested into PostgreSQL at a fixed interval of every 5 minutes, creating a near real-time data stream while maintaining transactional consistency.

**CDC**

To support near real-time processing, the system employs Change Data Capture (CDC) using Debezium. Specifically, changes in Finnhub-related tables within PostgreSQL are captured at the transaction log level and published to Apache Kafka topics.

* This CDC-based ingestion approach offers several benefits:  
* Guarantees exactly-once data delivery semantics at the database level.  
* Enables event-driven streaming without directly coupling external APIs to Kafka.  
* Ensures consistency between batch and streaming data by using the same data source.

Once ingested into Kafka, streaming data can be consumed by real-time analytics systems such as ClickHouse, dashboards, or feature stores for downstream machine learning applications.

## **2.2. Batch Processing**

Batch processing is responsible for handling large volumes of historical and periodically collected data within the proposed big data architecture. This processing mode is mainly applied to data sources such as GDELT news events and historical stock price data from Stooq, which do not require real-time processing but demand high scalability and analytical capability.

### **2.2.1. Batch Data Extraction**

Batch data is extracted from PostgreSQL, which acts as the staging and ingestion layer of the system. At scheduled intervals, Apache Spark jobs read raw data from PostgreSQL tables and prepare it for large-scale processing. This approach allows the system to maintain a clear separation between data ingestion and data processing, improving reliability and reusability of data.

### **2.2.2. Batch Processing with Apache Spark**

Apache Spark is used as the core batch processing engine due to its distributed computing capabilities and efficient handling of large datasets. Spark performs multiple data transformation tasks, including:

* Data cleaning and validation  
* Schema standardization  
* Data enrichment by joining stock price data with external event and news data  
* Aggregation and statistical analysis over time windows

These transformations enable the system to convert raw financial data into structured and analytical-ready datasets.

### **2.2.3. Medallion Architecture: Bronze, Silver, and Gold Layers**

The batch processing pipeline follows the medallion architecture, which organizes data into three progressive layers:

* *Bronze Layer*: Stores raw data extracted from PostgreSQL with minimal transformation. This layer preserves original records for auditing and reprocessing purposes.  
* *Silver Layer*: Contains cleaned, standardized, and enriched data. Common transformations such as data deduplication, normalization, and joining across datasets are applied at this stage.  
* *Gold Layer*: Provides aggregated and business-ready datasets optimized for analytics, reporting, and machine learning tasks.

This layered approach improves data quality, traceability, and maintainability throughout the data lifecycle.

### **2.2.4. Data Storage and Table  Management**

Processed batch data is stored in MinIO, an object storage system that serves as the data lake for the architecture. To manage large analytical tables efficiently, Apache Iceberg is used as the table format, providing:

* Schema evolution support  
* Time travel and versioning  
* Efficient partitioning and metadata management

This combination enables scalable and reliable batch data storage while supporting future reprocessing and backfilling scenarios.

### **2.2.5. Metadata Management and Orchestration**

To ensure effective data governance and pipeline management, the batch processing layer integrates with OpenMetadata to track dataset schemas, lineage, and data quality metrics. In addition, Apache Airflow is used to orchestrate batch jobs, manage execution dependencies, and schedule periodic Spark workflows.

### **2.2.6. Batch Processing Outputs**

The output of the batch processing pipeline is consumed by multiple downstream components, including:

* Offline feature generation for machine learning workflows  
* Historical analysis and reporting tasks

## **2.3. Stream Processing**

Streaming processing is designed to handle near real-time data and enable low-latency analytics within the proposed architecture. This processing mode is primarily applied to frequently updated stock market data collected from the Finnhub API, where timely insights are required.

### **2.3.1. Near Real-Time Data Characteristics**

Unlike batch-oriented data sources, stock price data from Finnhub is ingested at a fixed interval of every 5 minutes. Although this data is not strictly real-time, it is classified as near real-time streaming data, which requires continuous processing and immediate availability for downstream systems such as dashboards and online machine learning services.

### **2.3.2. Change Data Capture (CDC) with Debezium**

To support streaming ingestion, the system employs Change Data Capture (CDC) using Debezium. Instead of directly pushing data from the Finnhub API to Apache Kafka, all incoming data is first stored in PostgreSQL. Debezium monitors changes in relevant PostgreSQL tables by reading the database’s Write-Ahead Log (WAL) and converts each insert or update operation into a streaming event.

* This CDC-based approach provides several advantages:  
*  Ensures consistency between batch and streaming data pipelines  
* Guarantees reliable event delivery based on committed database transactions  
* Eliminates the need for custom Kafka producers  
* Enables event replay and fault-tolerant streaming ingestion

### **2.3.3. Apache Kafka as the Streaming Backbone**

Apache Kafka acts as the central message broker for the streaming pipeline. Events produced by Debezium are published to Kafka topics, where they are persisted and made available to multiple downstream consumers.

Kafka provides:

* High-throughput and low-latency message delivery  
* Horizontal scalability and fault tolerance  
* Loose coupling between data producers and consumers

This design allows multiple real-time applications to independently consume the same data stream without interfering with each other.

### **2.3.4. Stream Consumers and Real-Time Analytics**

Kafka consumers subscribe to relevant topics and process streaming events in real time. In the proposed architecture, streaming data is consumed by:

* ClickHouse, which stores near real-time data for fast analytical queries and aggregations  
* Real-time dashboards that display current market trends  
* Online feature pipelines that provide up-to-date features for machine learning inference

ClickHouse is particularly suitable for this workload due to its high-performance OLAP capabilities and efficient handling of time-series data.

### **2.3.5. Monitoring and Observability**

To ensure stable operation of the streaming pipeline, system metrics such as Kafka throughput, consumer lag, and CDC latency are continuously monitored. **Prometheus** is used to collect metrics from Kafka, Debezium, and other streaming components, while Grafana provides real-time dashboards for system observability and performance monitoring.

### **2.3.6. Streaming Processing Outputs**

The streaming processing layer enables several time-sensitive use cases, including:

* Real-time market monitoring dashboards  
* Rapid detection of price movements and anomalies  
* Online feature delivery for machine learning models  
* Low-latency analytical queries for decision support

## **2.4. Serving Layer**

### **2.4.1. Speed Layer \+ Batch Layer Integration with Clickhouse**

OLAP

### **2.4.2. Serving Real-time Metrics (Stream to Clickhouse)**

### **2.4.3. Serving Historical Analytics (Batch to ClickHouse)**

### **2.4.4. Dashboards**

# **3\. IMPLEMENTATION DETAILS**

This section describes the practical implementation of the proposed system, focusing on the technologies used, deployment environment, and the realization of data ingestion, batch processing, streaming processing, and system monitoring.

## **3.1. Technology Stack and Deployment Environment**

The system is implemented using a modern cloud-native technology stack designed to support large-scale data processing, real-time analytics, and machine learning workflows. All components are deployed as containerized microservices, ensuring modularity, scalability, and ease of management.

The core technology stack includes:

*  **PostgreSQL** as the staging and transactional database for ingested data.  
* **Apache Spark** for distributed batch processing.  
* **Apache Kafka** and Debezium for streaming data processing using Change Data Capture (CDC).  
* **MinIO** as the object storage layer for the data lake.  
* **Apache Iceberg** for table management, schema evolution, and data versioning.  
* **ClickHouse** for low-latency analytical queries on near real-time data.  
* **Apache Airflow** for batch workflow orchestration.  
* **Apache Superset** for analytical visualization and reporting.  
* **MLflow** for managing the machine learning lifecycle, including experiment tracking and model versioning for next-day stock return prediction.  
* **Prometheus** and **Grafana** for system monitoring and observability.

The deployment environment is based on Docker for containerization and Kubernetes for orchestration. Kubernetes provides automated deployment, scaling, and fault tolerance for all system components. Helm charts are used to manage application configuration and enable reproducible, version-controlled deployments across different environments. This design allows the system to remain cloud-agnostic while fully leveraging cloud-native principles.

## **3.2. Data Ingestion Implementation**

Data ingestion is implemented through dedicated ingestion services responsible for collecting data from external financial data sources, including GDELT, Stooq, and Finnhub APIs. These services periodically request data, validate API responses, and transform raw JSON payloads into structured formats compatible with the database schema.

All ingested data is initially stored in PostgreSQL, which acts as a staging and buffering layer. This design improves system reliability by ensuring data durability and allowing data replay in case of downstream failures. For Finnhub data, the ingestion service retrieves stock market data at a fixed interval of every 5 minutes, providing near real-time updates while maintaining transactional consistency.

## **3.3. Batch Processing Implementation**

Batch processing is implemented using Apache Spark, which is responsible for handling large volumes of historical and periodically collected data, such as GDELT events and historical stock prices from Stooq.

Spark batch jobs read data from PostgreSQL and perform a series of transformations, including data cleaning, normalization, aggregation, and enrichment. The processed data is stored in the data lake following the Medallion Architecture, consisting of Bronze, Silver, and Gold layers.

* The *Bronze layer* stores raw data with minimal transformation.  
* The *Silver layer* contains cleaned and standardized datasets.  
* The *Gold layer* provides aggregated and analytics-ready data optimized for reporting and machine learning.

Processed data is stored in MinIO, while Apache Iceberg manages table metadata, enabling schema evolution, version control, and time travel. Batch workflows are scheduled and orchestrated using Apache Airflow, ensuring reliable execution and dependency management.

## **3.4. Streaming Processing Implementation**

Streaming processing is implemented to support near real-time analytics on frequently updated stock market data from the Finnhub API. Instead of directly publishing API data to Kafka, the system employs a CDC-based streaming pipeline.

Debezium monitors changes in Finnhub-related tables within PostgreSQL by reading the database’s Write-Ahead Log (WAL). Each committed change is converted into an event and published to Apache Kafka topics. Kafka serves as the central streaming backbone, providing high throughput, low latency, and fault tolerance.

Streaming consumers subscribe to Kafka topics and process data in real time. In this system, streaming data is ingested into ClickHouse, which enables fast analytical queries and supports real-time dashboards and online feature generation for machine learning applications.

## **3.5. Serving & Visualization Implementation**

### **3.5.1. ClickHouse Table Engines**

To meet the requirement of low-latency queries for the visualization layer, ClickHouse is utilized as the central OLAP (Online Analytical Processing) engine. The implementation leverages distinct table engines to handle the specific characteristics of both the Batch and Speed layers efficiently.

**1\. Batch Layer Integration (Zero-Copy Architecture)** For the Batch Layer, the system avoids duplicating data from the Data Lake to the Data Warehouse. Instead, we utilize the **Apache Iceberg Table Engine** (or S3 Table Engine) to query data directly from MinIO.

* **Engine:** Iceberg (on top of MinIO/S3).  
* **Mechanism:** ClickHouse acts purely as a compute engine, reading the Parquet/Iceberg files generated by Apache Spark in the Gold Layer.  
* **Benefits:** This approach ensures strong consistency between the Lake and the Warehouse, reduces storage costs by avoiding redundancy, and allows the serving layer to access aggregated reports immediately after the Spark job completes.

**2\. Speed Layer Integration (Stream Ingestion Pipeline)** For real-time stock data flowing from Kafka, ClickHouse employs a three-stage ingestion pipeline to ensure high throughput and data persistence:

* **Stage 1: Kafka Engine (The Consumer)**  
  * A table defined with the Kafka engine acts as a direct consumer interface. It connects to the Kafka broker (e.g., topic finnhub\_stock\_prices), handles offset management, and deserializes the incoming Avro/JSON messages. This table does not store data persistently.  
* **Stage 2: Materialized View (The Trigger)**  
  * A Materialized View functions as an internal ETL trigger. It automatically reads data blocks from the Kafka Engine table as soon as they arrive, performs necessary type casting (e.g., converting UNIX timestamps to DateTime), and inserts the transformed data into the target storage table.  
* **Stage 3: MergeTree Engine (The Storage)**  
  * The final destination is a table using the MergeTree engine.  
  * **Optimization:** The table is configured with a Primary Key and Ordering Key (e.g., ORDER BY (symbol, timestamp)). This structure optimizes time-series queries, allowing Grafana to retrieve candlestick data (OHLC) for specific stock symbols with millisecond-level latency.

### **3.5.2. Building Dashboards with Superset (For Historical Deep Analysis)** 

**Objective:** To visualize historical model outputs and validate trading strategies using the Batch Layer.

* **Live Sentinel:** Big Number charts display the current Sentiment Score and News Volume. This provides an immediate numeric assessment of the active market regime.  
* **Regime Detection:** A dual-axis time-series chart overlays Sentiment Intensity (bars) against Stock Price (line). This correlates news volume spikes with subsequent price breakouts.  
* **Signal Validation:** Bar charts aggregate "Next-Day Returns" categorized by "Sentiment Bins." This validates model behavior, distinguishing between trend-following (positive sentiment) and mean-reversion (panic buy) setups.  
* **Risk Radar:** Stacked area charts track historical volatility across assets. This metric is used to adjust position sizing based on relative risk exposure.

### **3.5.3. Building Dashboards with Grafana (For Real-time Monitoring)**

**Objective:** To provide a low-latency interface for real-time market monitoring using the Speed Layer.

* **Real-time Candlestick Chart:**  
  * **Mechanism:** Connects directly to ClickHouse to aggregate raw tick data streams into 1-minute OHLC (Open, High, Low, Close) bars using argMin and argMax functions.  
  * **Function:** Visualizes immediate price discovery and market microstructure with sub-second latency.  
  * **Dynamic Filtering:** Implements Grafana Variables to switch assets instantly without query modification, enabling rapid multi-asset execution monitoring in a single view.

# **4\. LESSONS LEARNED**

 **Lesson 1: Hybrid Data Ingestion Strategies (Batch vs. Stream)**

#### **Problem Description**

* **Context:** The system handles two distinct data types: high-volume historical batch data (GDELT, Stooq) and high-velocity real-time market ticks (Finnhub).  
* **Challenges:** Using a "one-size-fits-all" ingestion approach created performance bottlenecks. Routing massive batch files through a relational database caused unnecessary I/O overhead, while sending raw API streams directly to the Data Lake risked data duplication and integrity issues.  
* **System Impact:** High latency in batch loading and potential data inconsistencies in the streaming layer.

#### **Approaches Tried**

* **Approach 1: Unified Database Ingestion.** Attempted to route all data through PostgreSQL before archiving.  
  * *Trade-off:* Severely degraded performance for batch ingestion; resource wastage on non-transactional data.  
* **Approach 2: Direct-to-Lake for All.** Attempted to write API streams directly to MinIO.  
  * *Trade-off:* Resulted in duplicate records and lack of immediate consistency for the operational view.

#### **Final Solution**

* **Batch Layer (GDELT/Stooq):** **Direct-to-Lake**. Files are fetched via Web/API and written directly to MinIO (Bronze Layer) as Parquet. This bypasses the transactional overhead entirely.  
* **Speed Layer (Finnhub):** **Transactional Buffering**. Implemented the pipeline: API \-\> Postgres \-\> Debezium \-\> Kafka \-\> MinIO.  
  * **Reasoning:** PostgreSQL acts as a transactional buffer to enforce Primary Keys (deduplication) and ensure data integrity during network instability. Debezium captures these changes (CDC) for the Data Lake.

#### **Key Takeaways**

* **"Don't put a database in the middle unless you have a transactional reason."**  
* Use raw storage (Parquet/MinIO) for bulk throughput.  
* Use relational databases (PostgreSQL) only when data integrity, deduplication, or immediate operational serving is required.

### **Lesson 2: Optimizing the Serving Layer for Low-Latency Analytics**

#### **Problem Description**

* **Context:** The project required both interactive historical analysis (Superset) and sub-second real-time monitoring (Grafana).  
* **Challenges:** The initial architecture failed to meet latency requirements for the presentation layer. Dashboards were sluggish, leading to poor User Experience (UX).  
* **System Impact:** Dashboard timeout errors and inability to visualize real-time market movements.

#### **Approaches Tried**

* **Approach 1: SparkSQL (Direct Query).** Connected Superset directly to MinIO via Spark.  
  * *Trade-off:* High latency. Spark is designed for throughput, not interactive queries. Spinning up executors for every dashboard filter caused multi-second delays.  
* **Approach 2: PostgreSQL (Serving Layer).** Loaded aggregated data into Postgres.  
  * *Trade-off:* Poor scalability for analytics. Being row-oriented, Postgres struggled to aggregate millions of records (e.g., `SUM(price)`) efficiently compared to column-oriented stores.

#### **Final Solution**

* **ClickHouse as the Unified Serving Layer.**  
  * **For Batch (Superset):** Aggregated "Gold" data is ingested into ClickHouse. Its column-based architecture allows for sub-second query responses on large historical datasets using SIMD vectorization.  
  * **For Real-Time (Grafana):** utilized ClickHouse's `Kafka Table Engine` to ingest streams directly (consuming millions of rows/sec). Grafana queries this directly for live dashboards.

#### **Key Takeaways**

* **OLTP vs. OLAP:** Never use a transactional DB (Postgres) for heavy analytical workloads; use a columnar store (ClickHouse).  
* **Latency vs. Throughput:** Spark is for processing (ETL); ClickHouse is for serving.  
* ClickHouse effectively bridges the gap in the **Lambda Architecture**, serving both Batch and Speed views.

### **Lesson 3: Contract-First Development for Parallel Execution**

#### **Problem Description**

* **Context:** The Data Engineering (DE) team and the Data Visualization (Viz) team needed to work simultaneously to meet the Capstone deadline.  
* **Challenges:** The visualization team was blocked because the data pipeline was not yet fully operational. Waiting for the full backend implementation effectively forced a "Waterfall" process in an Agile timeline.  
* **System Impact:** Delayed feedback loops on dashboard design and potential integration hell at the end of the project.

#### **Approaches Tried**

* **Approach 1: Sequential Development.** Waited for the DE team to finish the Bronze/Silver/Gold layers before starting dashboard work.  
  * *Trade-off:* Inefficient time management; risk of missing the final presentation deadline.  
* **Approach 2: Ad-hoc Mocking.** Used random hardcoded values in the visualization tools.  
  * *Trade-off:* Integration failed when real data arrived because data types and formats did not match.

#### **Final Solution**

* **Schema-Based Contract Development.**  
  * The teams agreed on a strict CSV schema (Contract) defining column names, data types, and expected values upfront.  
  * The Viz team built dashboards using these "Contract CSVs" (Mock Data) while the DE team built pipelines to produce that exact output.  
  * **Integration:** Swapping the mock source for the real ClickHouse/MinIO source required zero changes to the dashboard logic.

#### **Key Takeaways**

* Decouple dependencies between backend and frontend teams early.  
* **Contract-First:** Defining the output schema *before* writing the processing logic allows parallel development and reduces friction during the integration phase.

Lesson 4: Multi-arch Build, Sử dụng biến ARG của Docker?

# **5\. REFERENCE**
