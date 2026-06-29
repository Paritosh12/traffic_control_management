# Streaming Data Management System (SDMS)

## 1. Overview of the Project

The SDMS is a advanced simulation platform for real-time streaming data, particularly focused on intelligent traffic control. It is composed of a continuous stream of events (like sensors, general events, and commands) that run concurrently. The project supports various streaming windows (Sliding, Tumbling, and **Landmark**) to process analytical queries.

Key features include:

- **Landmark Aggregation**: The system supports "Beginning-to-Now" data analysis where metrics like Max Speed, Min Occupancy, and Total Vehicle Counts are calculated globally from the start of the simulation.
- **Incremental Summary Updates**: A background materialization process syncs the granular stream results into summary tables every 20 logical system ticks.
- **Advanced Graphing**: A per-second granular graph builder that allows users to visualize global trends by selecting specific attributes (Speed, Occupancy, Vehicles, Events) and aggregators (AVG, MIN, MAX, SUM, COUNT).
- **Logical Clock Sync**: The system tick is synchronized with the logical timestamps in the data stream, ensuring precise query execution.

## 2. Setup and Run Guide

### A. Environment Setup

1. **Clone the repository**:

   ```bash
   git clone <repository_url>
   cd traffic_control_management
   ```

2. **Create and Activate a Virtual Environment**:

   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

3. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

### B. Database Setup & Configurations

The project uses MySQL as its central operational datastore.

1. Create a MySQL database named `trafficdb`.
2. Update your credentials in `config.json`:
   ```json
   {
     "database": {
       "user": "your_user",
       "host": "localhost",
       "database": "trafficdb",
       "password": "your_password"
     }
   }
   ```
3. Initialize the schema:
   ```bash
   mysql -u your_user -p trafficdb < database/setup_sdms.sql
   mysql -u your_user -p trafficdb < database/static.sql
   mysql -u your_user -p trafficdb < database/temp.sql
   mysql -u your_user -p trafficdb < database/summary_tables.sql
   ```

### C. Running the Application

```bash
python3 app.py
```

Visit `http://127.0.0.1:5000` to start the simulation.

## 3. Project Architecture

- **`app.py`**: The Flask master process that orchestrates the entire simulation lifecycle.
- **`input_generator.py`**: Simulates traffic hardware by dumping XML bursts into the `data/` folder.
- **`input_monitor.py`**: Parses incoming XML, validates against XSD schemas, and injects data into the stream tables while synchronizing the system clock.
- **`query_processor.py`**: The engine that executes high-frequency SQL queries from the repository and stores results in the `output_buffer`.
- **`query_builder.py`**: Converts structured JSON query configurations from `query_repository.query_config` into parameterized SQL at runtime.
- **`summary_updater.py`**: A background service that materializes granular buffer results into permanent summary tables every 20 ticks.
- **`database/summary_tables.sql`**: Defines the schema for long-term incremental aggregation.
- **`frontend/`**: Interactive dashboard for real-time visualization, graph building, and system control.

## 4. Configuration-Based Queries

Continuous queries are stored as JSON configs instead of raw SQL. A query config declares the source stream, tick filter, group-by fields, and aggregate aliases. `query_processor.py` passes the config to `query_builder.py`, executes the generated SQL for the current tick, merges those rows with the latest prior result for the same query, and writes cumulative seen-so-far JSON results to `output_buffer`.

For example, an emergency count query stores the total emergency count seen so far. If the previous output count is `2` and the current tick has one new emergency event, the next `output_buffer` row stores `3`. The same cumulative behavior applies to `sum`, `count`, `max`, `min`, and `avg`.

Example:

```json
{
  "query_id": "IS-OCC-PKT",
  "source": "sensor_stream",
  "filter": { "field": "ts", "operator": "=", "value": "{CURRENT_TICK}" },
  "group_by": ["intersection_id"],
  "aggregations": [
    { "name": "s_occ", "operation": "sum", "field": "occupancy" },
    { "name": "c_pkt", "operation": "count", "field": "*" }
  ]
}
```

The formal schema is in `docs/query_config_schema.json`, and converted examples are in `docs/example_queries.json`.
