# Database schema

The harvester creates and uses one SQLite database (default: `data/metrics.db`).

## Table: nodes
Tracks known nodes over time.

- `id` INTEGER PK
- `device_id` TEXT
- `name` TEXT
- `ip` TEXT
- `domain` TEXT
- `first_seen_utc` TEXT (ISO 8601)
- `last_seen_utc` TEXT (ISO 8601)
- UNIQUE(`device_id`, `ip`)

## Table: runs
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

## Table: node_jobs
Detailed per-node trigger/collect state tracking.

- `id` INTEGER PK
- `run_id` TEXT
- `device_id` TEXT
- `name` TEXT
- `ip` TEXT
- `domain` TEXT
- `status` TEXT (for example `triggered`, `trigger_failed`, `collected`)
- `triggered_at_utc` TEXT
- `collected_at_utc` TEXT
- `result_file` TEXT
- `error_file` TEXT
- `error_message` TEXT

## Table: measurements
Final stored measurements with raw payload.

- `id` INTEGER PK
- `run_id` TEXT
- `device_id` TEXT
- `name` TEXT
- `ip` TEXT
- `domain` TEXT
- `nodeid` TEXT (from Gluon primary mac)
- `target` TEXT
- `throughput_mbit` REAL (`download_mbit`)
- `measurement_timestamp_ns` TEXT (node timestamp from payload)
- `measured_at_utc` TEXT (derived UTC timestamp)
- `raw_output` TEXT (full original line)
- `collected_at_utc` TEXT

## Notes
- Raw files are also kept in `data/raw/<run_id>/`.
- Schema is initialized automatically by `collect-node-metrics.ps1`.
