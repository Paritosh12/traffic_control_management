
SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS signal_group;
DROP TABLE IF EXISTS traffic_signal;
DROP TABLE IF EXISTS road;
DROP TABLE IF EXISTS intersection;


SET FOREIGN_KEY_CHECKS = 1;

CREATE TABLE intersection (
    intersection_id VARCHAR(20) PRIMARY KEY,
    location VARCHAR(100),
    num_roads INT
);

CREATE TABLE road (
    road_id VARCHAR(20),
    intersection_id VARCHAR(20),
    direction VARCHAR(10),     -- N, S, E, W
    num_lanes INT,
    PRIMARY KEY (road_id, intersection_id),
    FOREIGN KEY (intersection_id) REFERENCES intersection(intersection_id)
);

CREATE TABLE traffic_signal (
    signal_id VARCHAR(30) PRIMARY KEY,
    intersection_id VARCHAR(20),
    road_id VARCHAR(20),
    current_state VARCHAR(10),   -- RED/GREEN/YELLOW
    last_updated TIMESTAMP,
    FOREIGN KEY (intersection_id) REFERENCES intersection(intersection_id),
    FOREIGN KEY (road_id, intersection_id) REFERENCES road(road_id, intersection_id)
);

CREATE TABLE signal_group (
    group_id INT,
    signal_id VARCHAR(30),
    PRIMARY KEY (group_id, signal_id),
    FOREIGN KEY (signal_id) REFERENCES traffic_signal(signal_id)
);