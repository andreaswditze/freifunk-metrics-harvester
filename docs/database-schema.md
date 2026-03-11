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
- `result_type` TEXT (`success`, `final_failed`)
- `failure_reason` TEXT
- `downloaded_bytes` INTEGER
- `expected_bytes` INTEGER
- `download_duration_seconds` REAL
- `timeout_seconds` INTEGER
- `wget_exit_code` INTEGER
- `wget_exit_reason` TEXT
- `wget_stderr` TEXT
- `raw_output` TEXT
- `collected_at_utc` TEXT

### `node_diagnostics`
Per-node early diagnostic snapshots captured near the scheduled download start for every node that produced a parsed diagnostic file.

- `id` INTEGER PK
- `run_id` TEXT
- `device_id` TEXT
- `name` TEXT
- `ip` TEXT
- `domain` TEXT
- `nodeid` TEXT
- `diagnostic_timestamp_ns` TEXT
- `diagnosed_at_utc` TEXT
- `speedtest_delay_seconds` INTEGER
- `diagnostic_delay_seconds` INTEGER
- `target_host` TEXT
- `gateway_probe` TEXT
- `gateway_probe_kind` TEXT
- `ping_gateway_loss_pct` REAL
- `ping_target_loss_pct` REAL
- `load1` REAL
- `load5` REAL
- `load15` REAL
- `target_ipv4` TEXT
- `target_ipv6` TEXT
- `route_get_ipv4` TEXT
- `route_get_ipv6` TEXT
- `wget_stderr` TEXT
- `tcp_gateway_probe_port` INTEGER
- `tcp_gateway_probe_result` TEXT
- `tcp_target_probe_port` INTEGER
- `tcp_target_probe_result` TEXT
- `target_resolution` TEXT
- `route_get` TEXT
- `tcp_gateway_probe` TEXT
- `tcp_target_probe` TEXT
- `ip_rule` TEXT
- `batctl_if` TEXT
- `batctl_n` TEXT
- `ubus_network_dump` TEXT
- `ubus_ifstatus_wan` TEXT
- `ubus_ifstatus_wan6` TEXT
- `local_path` TEXT
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
- `idx_node_diagnostics_run_id` on `node_diagnostics(run_id)`
- `idx_node_diagnostics_device_id` on `node_diagnostics(device_id)`
- `idx_node_diagnostics_run_device_id` on `node_diagnostics(run_id, device_id)`

## Notes
- Raw files are also stored in `data/raw/<run_id>/`.
- Final failed speedtest results are also stored in `measurements`; they use `throughput_mbit = 0` and preserve the raw failure payload plus parsed transfer metadata, including dedicated `wget_exit_reason` and untruncated `wget_stderr` fields.
- Early diagnostics are written to `data/raw/<run_id>/` for all triggered nodes and persisted in `node_diagnostics` whenever the collector parses a diagnostic payload for that node.
- BATMAN mesh quality snapshots are persisted via `batctl_if` and `batctl_n` to help judge whether a stable mesh path to an exit gateway exists.
- Default gateway TCP reachability is additionally persisted via `tcp_gateway_probe_port`, `tcp_gateway_probe_result`, and `tcp_gateway_probe` so WAN-less nodes can be distinguished from pure ICMP reachability issues.
- Target-host TCP reachability is additionally persisted via `tcp_target_probe_port`, `tcp_target_probe_result`, and `tcp_target_probe` to separate long `downloaded_bytes = 0` failures from DNS-only or ICMP-only symptoms.
- Schema initialization runs automatically inside `collect-node-metrics.ps1` and applies the current schema directly for fresh database setups without `ALTER TABLE` migrations.
- The collect phase waits for parseable result files using polling and a timeout based on `TriggerRandomDelayMaxSeconds + CollectWaitTimeoutSeconds`.
- WAL mode is enabled for the database.






