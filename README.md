# freifunk-metrics-harvester

Minimalistic PowerShell 7 harvester for Freifunk node performance measurements.

## Purpose
This project runs a two-phase measurement flow against Gluon nodes:

1. Trigger speed tests asynchronously on reachable nodes.
2. Reconnect, collect result files, parse output, and store data in SQLite.

The result data is intended for network bottleneck detection from end-user perspective.

## Architecture
Freifunk Node (Gluon)
        |
        | SSH trigger + result collection
        v
Central Collector (PowerShell 7)
        |
        +-- Local raw files
        +-- SQLite metrics database

## Repository rules
Allowed in git:
- `src/config.demo.ps1`
- `src/config.development.example.ps1`

Never commit:
- `src/config.production.ps1`
- `src/config.production.*`

## Requirements
- Linux host with PowerShell 7 (`pwsh`)
- `ssh`, `scp`, `sqlite3`
- Optional: PowerShell module `ImportExcel` for `.xlsx` input files

Install ImportExcel if needed:

```powershell
Install-Module ImportExcel -Scope CurrentUser
```

## Project structure
- `src/collect-node-metrics.ps1`: main application
- `src/config.demo.ps1`: documented production defaults for host `mars`
- `src/config.development.example.ps1`: development template
- `docs/database-schema.md`: schema documentation
- `data/raw/`: collected raw result files by run id
- `log/`: log files
- `temp/`: temporary files and optional input staging

## Configuration
For production on `mars`:

1. Copy `src/config.demo.ps1` to `src/config.production.ps1`
2. Set either `ExcelInputFiles` and/or `ExcelInputDirectories` (recursive search supported)
3. Keep `config.production.ps1` local only (ignored by git)

## Run

```powershell
pwsh ./src/collect-node-metrics.ps1
```

Optional:

```powershell
pwsh ./src/collect-node-metrics.ps1 -ConfigPath ./src/config.development.example.ps1 -RunId run-20260306-220000
```

## Runtime flow
- Startup, config summary, database initialization
- Excel import (`DeviceID`, `Name`, `IP`, `Domain`)
- Trigger phase (`ssh`):
  - `mkdir -p /tmp/harvester`
  - start exact wget/awk payload in background
  - write `<runid>.result` and `<runid>.error`
- Collect phase (`scp`, fallback `ssh cat`)
- Parse Influx line protocol output (`download_mbit`, `nodeid`, `target`, timestamp)
- Store raw + parsed values in SQLite (`nodes`, `runs`, `node_jobs`, `measurements`)
- Final run summary in log

## Logging format
All log lines follow:

`DD.MM.YYYY HH:mm:ss: [LEVEL] message`

## Cron/systemd readiness
The script is non-interactive and can be called directly from cron or a systemd timer.

## About
This project is developed for and used by Freifunk Nordhessen e.V.

https://www.freifunk-nordhessen.de

## License
MIT License
Copyright (c) 2026 Andreas W. Ditze



## Testing mode
For controlled operational testing without Excel import, set in `config.production.ps1`:

```powershell
UseTestNodeIPs = $true
TestNodeIPs    = @(
    '2a03:2260:3013:200:7a8a:20ff:fed0:747a'
    '2a03:2260:3013:200:1ae8:29ff:fe5c:1ff8'
)
```

In this mode, the script only targets the configured `TestNodeIPs`.

## Pester tests
Run unit tests:

```powershell
pwsh -NoProfile -Command "Invoke-Pester ./tests"
```
