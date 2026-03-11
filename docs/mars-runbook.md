# Mars Runbook

Operational guide for running `freifunk-metrics-harvester` on host `mars`.

## 1. Prerequisites
Install system packages:

```bash
sudo apt-get update
sudo apt-get install -y powershell sqlite3 openssh-client
```

Install PowerShell modules:

```bash
pwsh -NoProfile -Command "Install-Module ImportExcel -Scope CurrentUser -Force"
pwsh -NoProfile -Command "Install-Module Pester -Scope CurrentUser -Force"
```

If `ImportExcel` warns about autosize, install optional dependencies:

```bash
sudo apt-get install -y --no-install-recommends libgdiplus libc6-dev
```

## 2. Initial deployment
```bash
cd /home/ffuser/skripte
git clone https://github.com/andreaswditze/freifunk-metrics-harvester.git
cd freifunk-metrics-harvester
cp src/config.demo.ps1 src/config.production.ps1
```

Edit local config:
- `SshKeyPath` must point to a valid key on mars.
- `ExcelInputDirectories` should point to the FNDG tree.
- Keep `src/config.production.ps1` local only.

## 3. Update deployment
```bash
cd /home/ffuser/skripte/freifunk-metrics-harvester
git checkout main
git pull --ff-only origin main
```

## 4. Recommended config fields
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

Example operational values:

```powershell
TriggerParallelism = 10
CollectParallelism = 10
TriggerRandomDelayMaxSeconds = 600
SpeedtestTargetUrl = 'https://fsn1-speed.hetzner.com/100MB.bin'
SpeedtestTargetBytes = 104857600
RemoteResultDir = '/tmp/harvester'
```

The collector validates these settings at startup and stops immediately on invalid values.

## 5. Run modes
Excel mode:

```powershell
UseTestNodeIPs = $false
ExcelInputDirectories = @('/home/ffuser/skripte/fndg/twodrive')
ExcelSearchRecurse = $true
```

Test-IP mode:

```powershell
UseTestNodeIPs = $true
TestNodeIPs = @(
    '2a03:2260:3013:200:7a8a:20ff:fed0:747a'
    '2a03:2260:3013:200:1ae8:29ff:fe5c:1ff8'
)
```

## 6. Execute collector
```bash
cd /home/ffuser/skripte/freifunk-metrics-harvester
pwsh ./src/collect-node-metrics.ps1
```

Optional explicit config:

```bash
pwsh ./src/collect-node-metrics.ps1 -ConfigPath ./src/config.production.ps1
```

Operational flow:
- Trigger phase runs in parallel up to `TriggerParallelism`.
- Each node receives a controller-assigned download delay between `0` and `TriggerRandomDelayMaxSeconds` seconds.
- On odd ISO weekdays, delays follow ascending last-known throughput; on even ISO weekdays, they follow ascending node ID.
- The weekday alternation avoids the same mesh-cluster neighbors always starting together and gives recurring anomalies a chance to show up under different start offsets.
- After trigger completion, the collector waits and polls for up to `TriggerRandomDelayMaxSeconds + CollectWaitTimeoutSeconds`.
- Collect phase reconnects only to successfully triggered nodes and runs in parallel up to `CollectParallelism`.
- Result files are fetched per node in one SSH stream. Parsed measurement files are removed remotely. Pending files remain on the node.

## 7. SSH host keys
The script intentionally tolerates changed SSH host keys.

Gluon nodes can legitimately change keys in the field. For this project the collector acts as a backup and monitoring system, so reachability is prioritized over strict host identity continuity. This is why host-key persistence and strict checking are intentionally disabled.

## 8. Verify results
- DB: `/home/ffuser/skripte/freifunk-metrics-harvester/data/metrics.db`
- Raw result files: `/home/ffuser/skripte/freifunk-metrics-harvester/data/raw/<run_id>/`
- Main logs: `/home/ffuser/skripte/freifunk-metrics-harvester/log/`
- Daily node actions: `/home/ffuser/skripte/freifunk-metrics-harvester/log/daily/`

Console behavior:
- Startup banner plus startup status lines
- One current action line
- Progress bars for trigger, wait, and collect

## 9. Run tests on mars
```bash
cd /home/ffuser/skripte/freifunk-metrics-harvester
pwsh ./Invoke-Tests.ps1
```
This default run now executes the full suite, including the HTTP integration test and the SSH streaming test.

Alternative direct runner:

```bash
pwsh ./tests/Invoke-Tests.ps1
```

Optional JUnit/NUnit output:

```bash
pwsh ./tests/Invoke-Tests.ps1 -OutputFormat NUnitXml -OutputPath ./temp/test-results.xml
```

## 10. Troubleshooting
- `No column headers found on top row`:
  file is skipped by design.
- `vorlage_*.xlsx`:
  file is skipped by naming rule.
- Empty run:
  check `UseTestNodeIPs` versus Excel settings and inspect the startup config summary log.
- Repeated `collect_pending` entries:
  increase `TriggerRandomDelayMaxSeconds` only if you also accept the doubled wait before collect, or reduce it if nodes finish too late for the current workflow.

