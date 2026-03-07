# Database schema

The harvester creates and uses one SQLite database (default: `data/metrics.db`).

## Tables

### `nodes`
Tracks known nodes over time.

- `id` INTEGER PK
- `device_id` TEXT
- `name` TEXT
- `ip` TEXT
- `domain` TEXT
- `first_seen_utc` TEXT (ISO 8601)
- `last_seen_utc` TEXT (ISO 8601)
- UNIQUE(`device_id`, `ip`)

### `runs`
One row per script execution run.

- `run_id` TEXT PK
- `started_at_utc` TEXT
- `completed_at_utc` TEXT
- `status` TEXT (`running`, `completed`, `failed`)
- `source_files` TEXT (semicolon-separated source list)
- `total_nodes` INTEGER
- `reachable_nodes` INTEGER
- `collected_nodes` INTEGER
- `parsed_nodes` INTEGER
- `notes` TEXT

### `node_jobs`
Detailed per-node trigger and collect state tracking.

- `id` INTEGER PK
- `run_id` TEXT
- `device_id` TEXT
- `name` TEXT
- `ip` TEXT
- `domain` TEXT
- `status` TEXT (for example `triggered`, `trigger_failed`, `collected`, `collected_failed_result`, `collect_pending`)
- `triggered_at_utc` TEXT
- `collected_at_utc` TEXT
- `result_file` TEXT
- `error_file` TEXT
- `error_message` TEXT

### `measurements`
Final stored measurements with raw payload.

- `id` INTEGER PK
- `run_id` TEXT
- `device_id` TEXT
- `name` TEXT
- `ip` TEXT
- `domain` TEXT
- `nodeid` TEXT
- `target` TEXT
- `throughput_mbit` REAL
- `measurement_timestamp_ns` TEXT
- `measured_at_utc` TEXT
- `raw_output` TEXT
- `collected_at_utc` TEXT

## Indexes
The schema also creates operational indexes for the main query paths:

- `idx_nodes_last_seen_utc` on `nodes(last_seen_utc)`
- `idx_node_jobs_run_id` on `node_jobs(run_id)`
- `idx_node_jobs_run_status` on `node_jobs(run_id, status)`
- `idx_measurements_run_id` on `measurements(run_id)`
- `idx_measurements_device_id` on `measurements(device_id)`
- `idx_measurements_nodeid` on `measurements(nodeid)`
- `idx_measurements_run_device_id` on `measurements(run_id, device_id)`
- `idx_measurements_measured_at_utc` on `measurements(measured_at_utc)`

## Notes
- Raw files are also stored in `data/raw/<run_id>/`.
- Final failed speedtest results are also stored in `measurements`; they use `throughput_mbit = 0` and preserve the raw failure payload.
- Schema initialization runs automatically inside `collect-node-metrics.ps1`.
- WAL mode is enabled for the database.
