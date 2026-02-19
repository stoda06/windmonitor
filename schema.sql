CREATE DATABASE IF NOT EXISTS windmonitor;
USE windmonitor;

CREATE TABLE IF NOT EXISTS wind_readings (
    id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    recorded_at DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    wind_speed  DECIMAL(6, 2)   NOT NULL COMMENT 'Wind speed in m/s',
    wind_dir    DECIMAL(5, 1)   NOT NULL COMMENT 'Wind direction in degrees (0-360)',
    INDEX idx_recorded_at (recorded_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
