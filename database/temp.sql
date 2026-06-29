SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS command_stream;
DROP TABLE IF EXISTS event_stream;
DROP TABLE IF EXISTS sensor_stream;

SET FOREIGN_KEY_CHECKS = 1;


CREATE TABLE sensor_stream (
    pkt_id VARCHAR(50) PRIMARY KEY,           -- PKT_000012
    intersection_id VARCHAR(20),
    road_id VARCHAR(20),
    signal_id VARCHAR(30),
    vehicle_count INT,
    avg_speed FLOAT,
    occupancy FLOAT,                          -- % lane usage
    ts INT
);

CREATE TABLE event_stream (
    event_id VARCHAR(50) PRIMARY KEY,         
    event_type VARCHAR(20),                   -- Emergency, Pedestrian, Accident, VIP_Convoy
    intersection_id VARCHAR(20),
    road_id VARCHAR(20),
    priority INT,
    ts INT
);

CREATE TABLE command_stream (
    cmd_id VARCHAR(50) PRIMARY KEY,           -- CMD-001343
    signal_id VARCHAR(30),
    action VARCHAR(10),                       -- GREEN/RED/YELLOW
    duration INT,
    reason VARCHAR(20),                       -- NORMAL / EMERGENCY
    ts INT
);

