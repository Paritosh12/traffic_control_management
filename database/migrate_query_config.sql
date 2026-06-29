SET @has_query_config := (
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'query_repository'
      AND COLUMN_NAME = 'query_config'
);

SET @alter_query_config := IF(
    @has_query_config = 0,
    'ALTER TABLE query_repository ADD COLUMN query_config JSON AFTER description',
    'SELECT "query_config already exists"'
);

PREPARE stmt FROM @alter_query_config;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

CREATE TABLE IF NOT EXISTS summary_updater_state (
    id INT PRIMARY KEY,
    last_processed_buffer_id INT DEFAULT 0
);

INSERT INTO summary_updater_state (id, last_processed_buffer_id)
VALUES (1, 0)
ON DUPLICATE KEY UPDATE last_processed_buffer_id = last_processed_buffer_id;

INSERT INTO query_repository (query_id, description, query_config, frequency_sec, last_run) VALUES
('RD-EVT-PKT', 'Cumulative event counts by road and event type', '{"query_id":"RD-EVT-PKT","source":"event_stream","filter":{"field":"ts","operator":"=","value":"{CURRENT_TICK}"},"group_by":["road_id","intersection_id","event_type"],"aggregations":[{"name":"c_evt","operation":"count","field":"*"}]}', 1, 0),
('IS-EVT-PKT', 'Cumulative event counts by intersection and event type', '{"query_id":"IS-EVT-PKT","source":"event_stream","filter":{"field":"ts","operator":"=","value":"{CURRENT_TICK}"},"group_by":["intersection_id","event_type"],"aggregations":[{"name":"c_evt","operation":"count","field":"*"}]}', 1, 0)
ON DUPLICATE KEY UPDATE query_id = query_id;
