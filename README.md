# freifunk-metrics-harvester

PowerShell 7 collector for distributed Freifunk node performance measurements.

## Purpose
The collector runs in two phases:

1. Trigger asynchronous speed measurements on reachable Gluon nodes.
2. Reconnect, collect result files, parse output, and store data in SQLite.

## Repository rules
Allowed in git:
- `src/config.demo.ps1`
- `src/config.development.example.ps1`

Never commit:
- `src/config.production.ps1`
- `src/config.production.*`

## Requirements
- Linux host (`mars`) with PowerShell 7 (`pwsh`)
- `ssh`, `scp`, `sqlite3`
- PowerShell modules:
  - `ImportExcel` (for `.xlsx` node files)
  - `Pester` (for tests)

## Quickstart on mars
```bash
ssh ffuser@mars
cd /home/ffuser/skripte

# first time
git clone https://github.com/andreaswditze/freifunk-metrics-harvester.git
cd freifunk-metrics-harvester

# updates
# git pull --ff-only origin main
```

Create local production config:
```bash
cp src/config.demo.ps1 src/config.production.ps1
```

Run collector:
```bash
pwsh ./src/collect-node-metrics.ps1
```

## Configuration
Use `src/config.production.ps1` locally on mars.

Excel mode (default):
- `ExcelInputFiles`
- `ExcelInputDirectories`
- `ExcelSearchRecurse`

Test-IP mode (bypass Excel import):
- `UseTestNodeIPs = $true`
- `TestNodeIPs = @(...)`

Default test IPs are preconfigured:
- `2a03:2260:3013:200:7a8a:20ff:fed0:747a`
- `2a03:2260:3013:200:1ae8:29ff:fe5c:1ff8`

## Logging
- Main run log: `log/<prefix>-YYYYMMDD-HHMMSS.log`
- Daily node action log: `log/daily/node-actions-YYYYMMDD.log`

Node action log includes timestamped events like:
- `trigger_start`, `trigger_success`, `trigger_failed`
- `collect_start`, `collect_success`, `collect_failed`
- `parse_success`, `parse_failed`

## Tests (Pester)
Run:
```bash
pwsh ./tests/Invoke-Tests.ps1
```

## Documentation
- `docs/mars-runbook.md` (operations, deploy, updates, tests)
- `docs/database-schema.md` (SQLite schema)

## License
MIT License

## Node result files
- On each node, the measurement output is written as `/tmp/harvester/<timestamp>.txt`.
- In collect phase, all files in `/tmp/harvester` are downloaded for the node.
- Parser extracts the first matching `speedtest,nodeid=...` line from each file.