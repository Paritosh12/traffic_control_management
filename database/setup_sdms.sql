DROP TABLE IF EXISTS system_tick;
CREATE TABLE system_tick (
    id INT PRIMARY KEY,
    current_tick INT
);
INSERT INTO system_tick (id, current_tick) VALUES (1, 0);

DROP TABLE IF EXISTS query_repository;
CREATE TABLE query_repository (
    query_id VARCHAR(50) PRIMARY KEY,
    description VARCHAR(255),
    query_config JSON,
    frequency_sec INT,
    last_run INT
);

INSERT INTO query_repository (query_id, description, query_config, frequency_sec, last_run) VALUES
('RD-SPEED-PKT', 'Per-tick speed aggregates by road', '{"query_id":"RD-SPEED-PKT","source":"sensor_stream","filter":{"field":"ts","operator":"=","value":"{CURRENT_TICK}"},"group_by":["road_id","intersection_id"],"aggregations":[{"name":"s_spd","operation":"sum","field":"avg_speed"},{"name":"x_spd","operation":"max","field":"avg_speed"},{"name":"n_spd","operation":"min","field":"avg_speed"},{"name":"c_pkt","operation":"count","field":"*"}],"summary_updates":[{"target":"pkt_count","source":"c_pkt","operation":"increment"},{"target":"sum_speed","source":"s_spd","operation":"increment"},{"target":"max_speed","source":"x_spd","operation":"max"},{"target":"min_speed","source":"n_spd","operation":"min"}],"derived_metrics":[{"name":"avg_speed","formula":"sum_speed / pkt_count"}]}', 1, 0),
('IS-SPEED-PKT', 'Per-tick speed aggregates by intersection', '{"query_id":"IS-SPEED-PKT","source":"sensor_stream","filter":{"field":"ts","operator":"=","value":"{CURRENT_TICK}"},"group_by":["intersection_id"],"aggregations":[{"name":"s_spd","operation":"sum","field":"avg_speed"},{"name":"x_spd","operation":"max","field":"avg_speed"},{"name":"n_spd","operation":"min","field":"avg_speed"},{"name":"c_pkt","operation":"count","field":"*"}],"derived_metrics":[{"name":"avg_speed","formula":"s_spd / c_pkt"}]}', 1, 0),
('RD-OCC-PKT', 'Per-tick occupancy aggregates by road', '{"query_id":"RD-OCC-PKT","source":"sensor_stream","filter":{"field":"ts","operator":"=","value":"{CURRENT_TICK}"},"group_by":["road_id","intersection_id"],"aggregations":[{"name":"s_occ","operation":"sum","field":"occupancy"},{"name":"x_occ","operation":"max","field":"occupancy"},{"name":"n_occ","operation":"min","field":"occupancy"},{"name":"c_pkt","operation":"count","field":"*"}],"summary_updates":[{"target":"sum_occupancy","source":"s_occ","operation":"increment"},{"target":"max_occupancy","source":"x_occ","operation":"max"},{"target":"min_occupancy","source":"n_occ","operation":"min"}],"derived_metrics":[{"name":"avg_occupancy","formula":"sum_occupancy / pkt_count"}]}', 1, 0),
('IS-OCC-PKT', 'Per-tick occupancy aggregates by intersection', '{"query_id":"IS-OCC-PKT","source":"sensor_stream","filter":{"field":"ts","operator":"=","value":"{CURRENT_TICK}"},"group_by":["intersection_id"],"aggregations":[{"name":"s_occ","operation":"sum","field":"occupancy"},{"name":"x_occ","operation":"max","field":"occupancy"},{"name":"n_occ","operation":"min","field":"occupancy"},{"name":"c_pkt","operation":"count","field":"*"}],"derived_metrics":[{"name":"avg_occupancy","formula":"s_occ / c_pkt"}]}', 1, 0),
('RD-VEH-PKT', 'Per-tick vehicle count totals by road', '{"query_id":"RD-VEH-PKT","source":"sensor_stream","filter":{"field":"ts","operator":"=","value":"{CURRENT_TICK}"},"group_by":["road_id","intersection_id"],"aggregations":[{"name":"s_veh","operation":"sum","field":"vehicle_count"}],"summary_updates":[{"target":"total_vehicles","source":"s_veh","operation":"increment"}]}', 1, 0),
('IS-VEH-PKT', 'Per-tick vehicle count totals by intersection', '{"query_id":"IS-VEH-PKT","source":"sensor_stream","filter":{"field":"ts","operator":"=","value":"{CURRENT_TICK}"},"group_by":["intersection_id"],"aggregations":[{"name":"s_veh","operation":"sum","field":"vehicle_count"}]}', 1, 0),
('RD-EVT-PKT', 'Per-tick event counts by road and event type', '{"query_id":"RD-EVT-PKT","source":"event_stream","filter":{"field":"ts","operator":"=","value":"{CURRENT_TICK}"},"group_by":["road_id","intersection_id","event_type"],"aggregations":[{"name":"c_evt","operation":"count","field":"*"}],"summary_updates":[{"target":"total_count","source":"c_evt","operation":"increment"}]}', 1, 0),
('IS-EVT-PKT', 'Per-tick event counts by intersection and event type', '{"query_id":"IS-EVT-PKT","source":"event_stream","filter":{"field":"ts","operator":"=","value":"{CURRENT_TICK}"},"group_by":["intersection_id","event_type"],"aggregations":[{"name":"c_evt","operation":"count","field":"*"}]}', 1, 0);


DROP TABLE IF EXISTS output_buffer;
CREATE TABLE output_buffer (
    id INT AUTO_INCREMENT PRIMARY KEY,
    query_id VARCHAR(50),
    ts INT,
    result TEXT
);
