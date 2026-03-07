# freifunk-metrics-harvester

PowerShell 7 collector for distributed Freifunk node performance measurements.

## Purpose
The collector runs in three operational steps:

1. Trigger asynchronous download measurements on reachable Gluon nodes.
2. Wait for the configured maximum random start delay.
3. Reconnect to successfully triggered nodes, collect result files, parse them, and store data in SQLite.

## Repository rules
Allowed in git:
- `src/config.demo.ps1`
- `src/config.development.example.ps1`

Never commit:
- `src/config.production.ps1`
- `src/config.production.*`

## Requirements
- Linux host (`mars`) with PowerShell 7 (`pwsh`)
- `ssh`, `sqlite3`
- PowerShell modules:
  - `ImportExcel` for `.xlsx` input files
  - `Pester` for tests

## Quickstart on mars
```bash
ssh ffuser@mars
cd /home/ffuser/skripte

git clone https://github.com/andreaswditze/freifunk-metrics-harvester.git
cd freifunk-metrics-harvester
cp src/config.demo.ps1 src/config.production.ps1
pwsh ./src/collect-node-metrics.ps1
```

## Configuration
Use `src/config.production.ps1` locally on mars.

Required paths:
- `ScriptBaseDir`
- `DatabasePath`
- `RawResultBaseDir`
- `LogDir`
- `TempDir`
- `SshKeyPath`

Node source:
- `ExcelInputFiles`
- `ExcelInputDirectories`
- `ExcelSearchRecurse`
- or `UseTestNodeIPs` plus `TestNodeIPs`

Runtime tuning:
- `TriggerParallelism`
- `CollectParallelism`
- `TriggerRandomDelayMaxSeconds`
- `SpeedtestTargetUrl`
- `SpeedtestTargetBytes`
- `RemoteResultDir`
- `SshConnectTimeoutSeconds`
- `LogFilePrefix`

The collector validates these values at startup and aborts early on invalid configuration.

## Runtime behavior
- Triggering runs with up to `TriggerParallelism` concurrent SSH sessions.
- Each node waits a random delay between `0` and `TriggerRandomDelayMaxSeconds` before starting the download.
- After all trigger attempts finish, the collector waits `TriggerRandomDelayMaxSeconds` once globally.
- Collecting runs only against nodes that were successfully triggered.
- Collecting uses up to `CollectParallelism` concurrent SSH sessions.
- Per node, all `*.txt` files in `RemoteResultDir` are fetched in one SSH stream instead of one `cat` call per file.
- Parsed measurement files are deleted remotely after a successful download attempt. Files without a measurement line stay on the node and are reported as pending.

## SSH trust model
The collector intentionally accepts changing SSH host keys.

It uses `StrictHostKeyChecking=no` and does not persist known-host entries because Gluon nodes can legitimately regenerate or replace keys in the field. For this project the script is used as a backup and monitoring tool, so operational reachability is prioritized over strict host identity continuity. This is an explicit tradeoff, not an omission.

## Logging
- Console output is intentionally minimal: startup status, one current action line, and progress bars.
- Main run log: `log/<prefix>-YYYYMMDD-HHMMSS.log`
- Daily node action log: `log/daily/node-actions-YYYYMMDD.log`

Node action log includes events such as:
- `trigger_start`, `trigger_success`, `trigger_failed`
- `collect_start`, `collect_success`, `collect_failed`, `collect_pending`
- `parse_success`, `parse_failed`

## Tests
Run all Pester tests from repo root:
```bash
pwsh ./Invoke-Tests.ps1
```

Alternative direct runner:
```bash
pwsh ./tests/Invoke-Tests.ps1
```

## Node result files
- Each node writes measurement output to `RemoteResultDir/<timestamp>.txt`.
- The collector stores raw local copies in `data/raw/<run_id>/`.
- The parser uses the first matching `speedtest,nodeid=...` line found in each collected file.

## Documentation
- `docs/mars-runbook.md`
- `docs/database-schema.md`

## License
MIT License
