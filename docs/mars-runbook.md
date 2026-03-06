# Mars Runbook

Operational guide for running `freifunk-metrics-harvester` on host `mars`.

## 1. Prerequisites

Install system packages (Debian/Ubuntu):

```bash
sudo apt-get update
sudo apt-get install -y powershell sqlite3 openssh-client
```

Install PowerShell modules:

```bash
pwsh -NoProfile -Command "Install-Module ImportExcel -Scope CurrentUser -Force"
pwsh -NoProfile -Command "Install-Module Pester -Scope CurrentUser -Force"
```

Note: If `ImportExcel` warns about autosize, install optional dependencies:

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

- `SshKeyPath` must point to valid key on mars.
- `ExcelInputDirectories` should point to FNDG tree.
- Keep `src/config.production.ps1` local only (gitignored).

## 3. Update deployment

```bash
cd /home/ffuser/skripte/freifunk-metrics-harvester
git checkout main
git pull --ff-only origin main
```

## 4. Run modes

### Excel mode

```powershell
UseTestNodeIPs       = $false
ExcelInputDirectories = @('/home/ffuser/skripte/fndg/twodrive')
ExcelSearchRecurse    = $true
```

### Test-IP mode
Bypasses Excel import and targets only configured test nodes.

```powershell
UseTestNodeIPs = $true
TestNodeIPs = @(
    '2a03:2260:3013:200:7a8a:20ff:fed0:747a'
    '2a03:2260:3013:200:1ae8:29ff:fe5c:1ff8'
)
```

## 5. Execute collector

```bash
cd /home/ffuser/skripte/freifunk-metrics-harvester
pwsh ./src/collect-node-metrics.ps1
```

Optional explicit config:

```bash
pwsh ./src/collect-node-metrics.ps1 -ConfigPath ./src/config.production.ps1
```

## 6. Verify results

- DB: `/home/ffuser/skripte/freifunk-metrics-harvester/data/metrics.db`
- Raw result files: `/home/ffuser/skripte/freifunk-metrics-harvester/data/raw/<run_id>/`
- Main logs: `/home/ffuser/skripte/freifunk-metrics-harvester/log/`
- Daily node actions: `/home/ffuser/skripte/freifunk-metrics-harvester/log/daily/`

## 7. Run tests on mars

```bash
cd /home/ffuser/skripte/freifunk-metrics-harvester
pwsh ./tests/Invoke-Tests.ps1
```

Optional JUnit/NUnit output:

```bash
pwsh ./tests/Invoke-Tests.ps1 -OutputFormat NUnitXml -OutputPath ./temp/test-results.xml
```

## 8. Troubleshooting

- `No column headers found on top row`:
  file is skipped by design.
- `vorlage_*.xlsx`:
  file is skipped by naming rule.
- SSH failures:
  validate key, node reachability, and `root` login policy on target nodes.
- Empty run:
  check `UseTestNodeIPs` vs Excel settings and inspect startup config summary log.
