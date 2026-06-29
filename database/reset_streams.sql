TRUNCATE TABLE sensor_stream;
TRUNCATE TABLE event_stream;
TRUNCATE TABLE command_stream;

TRUNCATE TABLE output_buffer;
TRUNCATE TABLE summary_metric_values;

UPDATE system_tick SET current_tick = 0 WHERE id = 1;

UPDATE query_repository SET last_run = 0;
