-- Finalized Summary tables for incremental aggregation

CREATE TABLE IF NOT EXISTS summary_metric_values (
    stream_name VARCHAR(30),
    metric_field VARCHAR(50),
    aggregate_name VARCHAR(20),
    dimensions VARCHAR(255),
    value DOUBLE DEFAULT 0,
    helper_count DOUBLE DEFAULT 0,
    last_updated_tick INT,
    PRIMARY KEY (stream_name, metric_field, aggregate_name, dimensions)
);
