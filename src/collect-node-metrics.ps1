<#
.SYNOPSIS
Collects distributed Freifunk node performance measurements in two phases.

.DESCRIPTION
Phase A triggers an asynchronous wget-based speed test on each reachable node via SSH.
Phase B reconnects, collects result files, parses Influx-style line protocol output,
and stores raw + parsed values in a local SQLite database.

.NOTES
Designed for PowerShell 7 on Linux and non-interactive execution via cron/systemd.
#>
[CmdletBinding()]
param(
    [string]$ConfigPath,
    [string]$RunId,
    [switch]$VerboseLogging,
    [switch]$NoRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$script:CurrentConfig = $null
$script:LogFilePath = $null
$script:DailyLogDir = $null
$script:DailyLogFilePath = $null

function Log {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$Message,
        [ValidateSet('INFO', 'WARN', 'ERROR', 'DEBUG')]
        [string]$Level = 'INFO'
    )

    $timestamp = Get-Date -Format 'dd.MM.yyyy HH:mm:ss'
    $line = '{0}: [{1}] {2}' -f $timestamp, $Level, $Message
    Write-Host $line

    if ($script:LogFilePath) {
        Add-Content -Path $script:LogFilePath -Value $line
    }

    if ($script:DailyLogFilePath) {
        Add-Content -Path $script:DailyLogFilePath -Value $line
    }
}


function Log-NodeAction {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [pscustomobject]$Node,
        [Parameter(Mandatory = $true)]
        [string]$Action,
        [string]$Detail = '',
        [ValidateSet('INFO', 'WARN', 'ERROR', 'DEBUG')]
        [string]$Level = 'INFO'
    )

    $safeDetail = $Detail.Replace('"', "'").Trim()
    $message = 'NODE action={0} device_id={1} name="{2}" ip={3} domain={4} detail="{5}"' -f $Action, $Node.DeviceID, $Node.Name, $Node.IP, $Node.Domain, $safeDetail
    Log -Level $Level -Message $message
}

function Get-EnvironmentConfig {
    [CmdletBinding()]
    param(
        [string]$RequestedPath
    )

    $scriptRoot = Split-Path -Parent $PSCommandPath
    $configCandidates = @()

    if ($RequestedPath) {
        $configCandidates += $RequestedPath
    }

    $configCandidates += @(
        (Join-Path $scriptRoot 'config.production.ps1'),
        (Join-Path $scriptRoot 'config.development.example.ps1'),
        (Join-Path $scriptRoot 'config.demo.ps1')
    )

    $chosenConfig = $null
    foreach ($candidate in $configCandidates) {
        if ($candidate -and (Test-Path -Path $candidate -PathType Leaf)) {
            $chosenConfig = (Resolve-Path -Path $candidate).Path
            break
        }
    }

    if (-not $chosenConfig) {
        throw 'No config file found. Provide -ConfigPath or create config.production.ps1 from config.demo.ps1.'
    }

    $configData = & $chosenConfig
    if (-not $configData -or $configData -isnot [hashtable]) {
        throw "Config file must return a hashtable: ${chosenConfig}"
    }

    $defaults = @{
        SshUser                   = 'root'
        SshBinary                 = 'ssh'
        ScpBinary                 = 'scp'
        SQLiteBinary              = 'sqlite3'
        RemoteResultDir           = '/tmp/harvester'
        SshConnectTimeoutSeconds  = 8
        CollectWaitSeconds        = 90
        LogFilePrefix             = 'collect-node-metrics'
        ExcelInputFiles           = @()
        ExcelInputDirectories     = @()
        ExcelSearchRecurse        = $true
        UseTestNodeIPs            = $false
        TestNodeIPs               = @('2a03:2260:3013:200:7a8a:20ff:fed0:747a','2a03:2260:3013:200:1ae8:29ff:fe5c:1ff8')
    }

    foreach ($key in $defaults.Keys) {
        if (-not $configData.ContainsKey($key) -or [string]::IsNullOrWhiteSpace([string]$configData[$key])) {
            $configData[$key] = $defaults[$key]
        }
    }

    $requiredKeys = @(
        'ScriptBaseDir',
        'DatabasePath',
        'RawResultBaseDir',
        'LogDir',
        'TempDir',
        'SshKeyPath'
    )

    foreach ($key in $requiredKeys) {
        if (-not $configData.ContainsKey($key) -or [string]::IsNullOrWhiteSpace([string]$configData[$key])) {
            throw "Missing required config key: ${key}"
        }
    }

    $configData.ConfigPath = $chosenConfig
    return $configData
}

function Test-ValidNodeRow {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [pscustomobject]$Row
    )

    if ([string]::IsNullOrWhiteSpace([string]$Row.DeviceID)) { return $false }
    if ([string]::IsNullOrWhiteSpace([string]$Row.Name)) { return $false }
    if ([string]::IsNullOrWhiteSpace([string]$Row.IP)) { return $false }
    if ([string]::IsNullOrWhiteSpace([string]$Row.Domain)) { return $false }

    return $true
}

function Get-NormalizedIP {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$IP
    )

    $trimmed = $IP.Trim()
    $sanitized = $trimmed.Trim('[', ']')
    return $sanitized
}

function Get-RowValue {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object]$Row,
        [Parameter(Mandatory = $true)]
        [string[]]$Candidates
    )

    $propertyMap = @{}
    foreach ($prop in $Row.PSObject.Properties) {
        $propertyMap[$prop.Name.ToLowerInvariant()] = $prop.Value
    }

    foreach ($candidate in $Candidates) {
        $lookup = $candidate.ToLowerInvariant()
        if ($propertyMap.ContainsKey($lookup)) {
            return [string]$propertyMap[$lookup]
        }
    }

    return ''
}

function Resolve-NodeSourceFiles {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Config
    )

    $resolved = New-Object System.Collections.Generic.List[string]

    foreach ($path in @($Config.ExcelInputFiles)) {
        if ([string]::IsNullOrWhiteSpace([string]$path)) { continue }
        if (Test-Path -Path $path -PathType Leaf) {
            $resolved.Add((Resolve-Path -Path $path).Path)
        }
        else {
            Log -Level WARN -Message "Excel file path missing: ${path}"
        }
    }
    $extensions = @('*.xlsx', '*.xlsm', '*.xls', '*.csv')
    foreach ($dir in @($Config.ExcelInputDirectories)) {
        if ([string]::IsNullOrWhiteSpace([string]$dir)) { continue }
        if (-not (Test-Path -Path $dir -PathType Container)) {
            Log -Level WARN -Message "Excel input directory missing: ${dir}"
            continue
        }

        foreach ($pattern in $extensions) {
            $items = Get-ChildItem -Path $dir -Filter $pattern -File -Recurse:$([bool]$Config.ExcelSearchRecurse) -ErrorAction SilentlyContinue
            foreach ($item in $items) {
                $resolved.Add($item.FullName)
            }
        }
    }

    return @($resolved | Sort-Object -Unique)
}

function Get-TestNodesFromConfig {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Config
    )

    $normalized = @(
        @($Config.TestNodeIPs) |
            ForEach-Object { Get-NormalizedIP -IP ([string]$_) } |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
            Select-Object -Unique
    )

    $nodes = New-Object System.Collections.Generic.List[object]
    $index = 1
    foreach ($ip in $normalized) {
        $nodes.Add([pscustomobject]@{
            DeviceID = 'test-{0:d3}' -f $index
            Name     = 'TestNode-{0:d3}' -f $index
            IP       = $ip
            Domain   = 'testing'
        })
        $index++
    }

    return [pscustomobject]@{
        Nodes       = $nodes.ToArray()
        SourceFiles = @('<test-node-ips>')
    }
}
function Import-NodeListFromExcel {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Config
    )

    $allNodes = New-Object System.Collections.Generic.List[object]
    $sourceFiles = @(Resolve-NodeSourceFiles -Config $Config)

    if (-not $sourceFiles -or $sourceFiles.Count -eq 0) {
        throw 'No node source files found. Use ExcelInputFiles and/or ExcelInputDirectories in config.'
    }

    $importExcelAvailable = $false
    if (Get-Module -ListAvailable -Name ImportExcel) {
        $importExcelAvailable = $true
        Import-Module ImportExcel -ErrorAction Stop | Out-Null
    }

    foreach ($filePath in $sourceFiles) {
        if (-not (Test-Path -Path $filePath -PathType Leaf)) {
            Log -Level WARN -Message "Excel source missing: ${filePath}"
            continue
        }

        $extension = [IO.Path]::GetExtension($filePath).ToLowerInvariant()
        $baseName = [IO.Path]::GetFileName($filePath)
        $rows = @()

        if ($baseName -match '(?i)^vorlage_.*\\.xlsx$') {
            Log -Level WARN -Message "Skipping template workbook by naming rule: ${filePath}"
            continue
        }

        if ($extension -eq '.csv') {
            $rows = Import-Csv -Path $filePath
        }
        elseif ($extension -in @('.xlsx', '.xlsm', '.xls')) {
            if (-not $importExcelAvailable) {
                throw 'Module ImportExcel is required for .xlsx imports. Install with: Install-Module ImportExcel -Scope CurrentUser'
            }

            try {
                $rows = Import-Excel -Path $filePath
            }
            catch {
                $message = $_.Exception.Message
                if ($message -match 'No column headers found on top row') {
                    Log -Level WARN -Message "Skipping workbook without header row: ${filePath}"
                    continue
                }

                throw
            }
        }
        else {
            Log -Level WARN -Message "Skipping unsupported file extension: ${filePath}"
            continue
        }

        $imported = 0
        $skipped = 0

        foreach ($row in $rows) {
            $node = [pscustomobject]@{
                DeviceID = (Get-RowValue -Row $row -Candidates @('DeviceID', 'DeviceId', 'ID', 'NodeID', 'NodeId'))
                Name     = (Get-RowValue -Row $row -Candidates @('Name', 'Hostname', 'NodeName'))
                IP       = (Get-RowValue -Row $row -Candidates @('IP', 'IPv4', 'Address', 'NodeIP'))
                Domain   = (Get-RowValue -Row $row -Candidates @('Domain', 'Segment', 'Community'))
            }

            if (-not (Test-ValidNodeRow -Row $node)) {
                $skipped++
                continue
            }

            $node.IP = Get-NormalizedIP -IP $node.IP
            $allNodes.Add($node)
            $imported++
        }

        Log -Message "Excel import done for ${filePath}: imported=${imported}, skipped=${skipped}"
    }

    $uniqueNodes = $allNodes |
        Group-Object -Property DeviceID, IP |
        ForEach-Object { $_.Group[0] }

    return [pscustomobject]@{
        Nodes       = @($uniqueNodes)
        SourceFiles = $sourceFiles
    }
}

function Invoke-Sqlite {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Config,
        [Parameter(Mandatory = $true)]
        [string]$Sql
    )

    $dbDir = Split-Path -Parent $Config.DatabasePath
    if (-not (Test-Path -Path $dbDir -PathType Container)) {
        New-Item -ItemType Directory -Path $dbDir -Force | Out-Null
    }

    $result = & $Config.SQLiteBinary $Config.DatabasePath $Sql 2>&1
    $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0) {
        throw "SQLite command failed (${exitCode}): $($result -join ' ')"
    }

    return $result
}

function Escape-SqlLiteral {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [string]$Value
    )

    if ($null -eq $Value) {
        return ''
    }

    return $Value.Replace("'", "''")
}

function Initialize-Database {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Config
    )

    $ddl = @"
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS nodes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id TEXT NOT NULL,
    name TEXT NOT NULL,
    ip TEXT NOT NULL,
    domain TEXT NOT NULL,
    first_seen_utc TEXT NOT NULL,
    last_seen_utc TEXT NOT NULL,
    UNIQUE(device_id, ip)
);

CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    started_at_utc TEXT NOT NULL,
    completed_at_utc TEXT,
    status TEXT NOT NULL,
    source_files TEXT,
    total_nodes INTEGER NOT NULL DEFAULT 0,
    reachable_nodes INTEGER NOT NULL DEFAULT 0,
    collected_nodes INTEGER NOT NULL DEFAULT 0,
    parsed_nodes INTEGER NOT NULL DEFAULT 0,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS node_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    device_id TEXT,
    name TEXT,
    ip TEXT NOT NULL,
    domain TEXT,
    status TEXT NOT NULL,
    triggered_at_utc TEXT,
    collected_at_utc TEXT,
    result_file TEXT,
    error_file TEXT,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS measurements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    device_id TEXT,
    name TEXT,
    ip TEXT,
    domain TEXT,
    nodeid TEXT,
    target TEXT,
    throughput_mbit REAL,
    measurement_timestamp_ns TEXT,
    measured_at_utc TEXT,
    raw_output TEXT NOT NULL,
    collected_at_utc TEXT NOT NULL
);
"@

    Invoke-Sqlite -Config $Config -Sql $ddl | Out-Null
    Log -Message "Database initialized: $($Config.DatabasePath)"
}

function Start-MeasurementRun {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Config,
        [Parameter(Mandatory = $true)]
        [string]$RunId,
        [Parameter(Mandatory = $true)]
        [string[]]$SourceFiles,
        [int]$TotalNodes = 0
    )

    $startedUtc = (Get-Date).ToUniversalTime().ToString('o')
    $sourceJoined = Escape-SqlLiteral -Value ($SourceFiles -join ';')
    $sql = "INSERT INTO runs (run_id, started_at_utc, status, source_files, total_nodes) VALUES ('$((Escape-SqlLiteral -Value $RunId))', '$startedUtc', 'running', '$sourceJoined', $TotalNodes);"

    Invoke-Sqlite -Config $Config -Sql $sql | Out-Null

    $runRawDir = Join-Path $Config.RawResultBaseDir $RunId
    New-Item -ItemType Directory -Path $runRawDir -Force | Out-Null

    Log -Message "Measurement run started: ${RunId}"

    return [pscustomobject]@{
        RunId        = $RunId
        StartedAtUtc = $startedUtc
        RawDir       = $runRawDir
    }
}

function New-SshArgs {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Config,
        [Parameter(Mandatory = $true)]
        [string]$NodeIp
    )

    return @(
        '-i', $Config.SshKeyPath,
        '-o', 'BatchMode=yes',
        '-o', "ConnectTimeout=$($Config.SshConnectTimeoutSeconds)",
        '-o', 'StrictHostKeyChecking=accept-new',
        "$($Config.SshUser)@${NodeIp}"
    )
}

function Invoke-NodeTriggerCommand {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Config,
        [Parameter(Mandatory = $true)]
        [pscustomobject]$Node,
        [Parameter(Mandatory = $true)]
        [string]$RunId
    )

    $remoteResultFile = "$($Config.RemoteResultDir)/${RunId}.result"
    $remoteErrorFile = "$($Config.RemoteResultDir)/${RunId}.error"

    $payload = "start=`$(date +%s%N); nodeid=`$(tr -d ':' </lib/gluon/core/sysconfig/primary_mac); t0=`$(date +%s.%N); wget -O /dev/null -q https://fsn1-speed.hetzner.com/100MB.bin; t1=`$(date +%s.%N); awk -v nodeid=`"`$nodeid`" -v start=`"`$start`" -v t0=`"`$t0`" -v t1=`"`$t1`" 'BEGIN{bytes=104857600; target=`"https://fsn1-speed.hetzner.com/100MB.bin`"; sec=t1-t0; printf `"speedtest,nodeid=%s download_mbit=%.2f,target=\\`"%s\\`" %s\\n`",nodeid,(bytes*8)/(sec*1000000),target,start}'"
    $triggerCmd = "mkdir -p '$($Config.RemoteResultDir)'; ( $payload ) > '$remoteResultFile' 2> '$remoteErrorFile' &"

    $sshArgs = New-SshArgs -Config $Config -NodeIp $Node.IP
    $output = & $Config.SshBinary @sshArgs $triggerCmd 2>&1
    $exitCode = $LASTEXITCODE

    if ($exitCode -ne 0) {
        return [pscustomobject]@{
            Reachable        = $false
            Triggered        = $false
            RemoteResultFile = $remoteResultFile
            RemoteErrorFile  = $remoteErrorFile
            Error            = ($output -join ' ')
        }
    }

    return [pscustomobject]@{
        Reachable        = $true
        Triggered        = $true
        RemoteResultFile = $remoteResultFile
        RemoteErrorFile  = $remoteErrorFile
        Error            = ''
    }
}

function Collect-NodeResults {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Config,
        [Parameter(Mandatory = $true)]
        [pscustomobject]$Node,
        [Parameter(Mandatory = $true)]
        [string]$RunId,
        [Parameter(Mandatory = $true)]
        [string]$RawDir
    )

    $resultFileName = "{0}_{1}.result" -f $Node.DeviceID, $Node.IP.Replace(':', '_')
    $errorFileName = "{0}_{1}.error" -f $Node.DeviceID, $Node.IP.Replace(':', '_')
    $localResultPath = Join-Path $RawDir $resultFileName
    $localErrorPath = Join-Path $RawDir $errorFileName

    $remoteResultPath = "$($Config.RemoteResultDir)/${RunId}.result"
    $remoteErrorPath = "$($Config.RemoteResultDir)/${RunId}.error"

    $scpCommon = @(
        '-i', $Config.SshKeyPath,
        '-o', 'BatchMode=yes',
        '-o', "ConnectTimeout=$($Config.SshConnectTimeoutSeconds)",
        '-o', 'StrictHostKeyChecking=accept-new'
    )

    $scpResult = & $Config.ScpBinary @scpCommon "$($Config.SshUser)@$($Node.IP):$remoteResultPath" $localResultPath 2>&1
    $resultExit = $LASTEXITCODE

    if ($resultExit -ne 0) {
        $sshArgs = New-SshArgs -Config $Config -NodeIp $Node.IP
        $catOutput = & $Config.SshBinary @sshArgs "cat '$remoteResultPath'" 2>&1
        if ($LASTEXITCODE -ne 0) {
            return [pscustomobject]@{
                Success         = $false
                LocalResultPath = $localResultPath
                LocalErrorPath  = $localErrorPath
                RawOutput       = ''
                ErrorOutput     = ($scpResult -join ' ') + ' | ' + ($catOutput -join ' ')
            }
        }

        Set-Content -Path $localResultPath -Value ($catOutput -join "`n") -NoNewline
    }

    $null = & $Config.ScpBinary @scpCommon "$($Config.SshUser)@$($Node.IP):$remoteErrorPath" $localErrorPath 2>$null
    if ($LASTEXITCODE -ne 0) {
        Set-Content -Path $localErrorPath -Value '' -NoNewline
    }

    $rawOutput = ''
    if (Test-Path -Path $localResultPath -PathType Leaf) {
        $rawOutput = (Get-Content -Path $localResultPath -Raw)
    }

    $errorOutput = ''
    if (Test-Path -Path $localErrorPath -PathType Leaf) {
        $errorOutput = (Get-Content -Path $localErrorPath -Raw)
    }

    return [pscustomobject]@{
        Success         = $true
        LocalResultPath = $localResultPath
        LocalErrorPath  = $localErrorPath
        RawOutput       = $rawOutput.Trim()
        ErrorOutput     = $errorOutput.Trim()
    }
}

function Parse-MeasurementOutput {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$RawOutput
    )

    $line = ($RawOutput -split "`n" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Select-Object -First 1)
    if (-not $line) {
        return $null
    }

    $regex = '^speedtest,nodeid=(?<nodeid>[^ ]+)\s+download_mbit=(?<download>[0-9]+(?:\.[0-9]+)?),target="(?<target>[^"]+)"\s+(?<timestamp>[0-9]+)$'
    $match = [regex]::Match($line.Trim(), $regex)
    if (-not $match.Success) {
        return $null
    }

    return [pscustomobject]@{
        NodeId         = $match.Groups['nodeid'].Value
        ThroughputMbit = [double]::Parse($match.Groups['download'].Value, [System.Globalization.CultureInfo]::InvariantCulture)
        Target         = $match.Groups['target'].Value
        TimestampNs    = $match.Groups['timestamp'].Value
    }
}

function Save-Measurement {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Config,
        [Parameter(Mandatory = $true)]
        [pscustomobject]$Node,
        [Parameter(Mandatory = $true)]
        [string]$RunId,
        [Parameter(Mandatory = $true)]
        [string]$RawOutput,
        [AllowNull()]
        [pscustomobject]$ParsedMeasurement
    )

    $nowUtc = (Get-Date).ToUniversalTime().ToString('o')

    $deviceId = Escape-SqlLiteral -Value $Node.DeviceID
    $name = Escape-SqlLiteral -Value $Node.Name
    $ip = Escape-SqlLiteral -Value $Node.IP
    $domain = Escape-SqlLiteral -Value $Node.Domain
    $rawEsc = Escape-SqlLiteral -Value $RawOutput

    $upsertNodeSql = "INSERT INTO nodes (device_id, name, ip, domain, first_seen_utc, last_seen_utc) VALUES ('$deviceId', '$name', '$ip', '$domain', '$nowUtc', '$nowUtc') ON CONFLICT(device_id, ip) DO UPDATE SET name = excluded.name, domain = excluded.domain, last_seen_utc = excluded.last_seen_utc;"
    Invoke-Sqlite -Config $Config -Sql $upsertNodeSql | Out-Null

    $nodeId = ''
    $target = ''
    $throughput = 'NULL'
    $tsNs = ''
    $measuredAtUtc = ''

    if ($ParsedMeasurement) {
        $nodeId = Escape-SqlLiteral -Value $ParsedMeasurement.NodeId
        $target = Escape-SqlLiteral -Value $ParsedMeasurement.Target
        $throughput = [string]::Format([System.Globalization.CultureInfo]::InvariantCulture, '{0:0.00}', $ParsedMeasurement.ThroughputMbit)
        $tsNs = Escape-SqlLiteral -Value $ParsedMeasurement.TimestampNs
        $measuredAtUtc = (Get-Date -Date '1970-01-01T00:00:00Z').AddMilliseconds([double]$ParsedMeasurement.TimestampNs / 1000000).ToUniversalTime().ToString('o')
    }

    $measuredAtEsc = Escape-SqlLiteral -Value $measuredAtUtc

    $insertMeasurement = "INSERT INTO measurements (run_id, device_id, name, ip, domain, nodeid, target, throughput_mbit, measurement_timestamp_ns, measured_at_utc, raw_output, collected_at_utc) VALUES ('$((Escape-SqlLiteral -Value $RunId))', '$deviceId', '$name', '$ip', '$domain', '$nodeId', '$target', $throughput, '$tsNs', '$measuredAtEsc', '$rawEsc', '$nowUtc');"

    Invoke-Sqlite -Config $Config -Sql $insertMeasurement | Out-Null
    Log -Message "DB insert complete for node ${ip}"
}

function Complete-MeasurementRun {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Config,
        [Parameter(Mandatory = $true)]
        [string]$RunId,
        [Parameter(Mandatory = $true)]
        [int]$ReachableNodes,
        [Parameter(Mandatory = $true)]
        [int]$CollectedNodes,
        [Parameter(Mandatory = $true)]
        [int]$ParsedNodes,
        [Parameter(Mandatory = $true)]
        [string]$Status,
        [string]$Notes = ''
    )

    $completedUtc = (Get-Date).ToUniversalTime().ToString('o')
    $notesEsc = Escape-SqlLiteral -Value $Notes

    $sql = "UPDATE runs SET completed_at_utc = '$completedUtc', status = '$((Escape-SqlLiteral -Value $Status))', reachable_nodes = $ReachableNodes, collected_nodes = $CollectedNodes, parsed_nodes = $ParsedNodes, notes = '$notesEsc' WHERE run_id = '$((Escape-SqlLiteral -Value $RunId))';"

    Invoke-Sqlite -Config $Config -Sql $sql | Out-Null
    Log -Message "Run completed: ${RunId}, status=${Status}, reachable=${ReachableNodes}, collected=${CollectedNodes}, parsed=${ParsedNodes}"
}

function Insert-NodeJobRecord {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Config,
        [Parameter(Mandatory = $true)]
        [string]$RunId,
        [Parameter(Mandatory = $true)]
        [pscustomobject]$Node,
        [Parameter(Mandatory = $true)]
        [string]$Status,
        [string]$TriggeredAtUtc = '',
        [string]$CollectedAtUtc = '',
        [string]$ResultFile = '',
        [string]$ErrorFile = '',
        [string]$ErrorMessage = ''
    )

    $sql = "INSERT INTO node_jobs (run_id, device_id, name, ip, domain, status, triggered_at_utc, collected_at_utc, result_file, error_file, error_message) VALUES ('$((Escape-SqlLiteral -Value $RunId))', '$((Escape-SqlLiteral -Value $Node.DeviceID))', '$((Escape-SqlLiteral -Value $Node.Name))', '$((Escape-SqlLiteral -Value $Node.IP))', '$((Escape-SqlLiteral -Value $Node.Domain))', '$((Escape-SqlLiteral -Value $Status))', '$((Escape-SqlLiteral -Value $TriggeredAtUtc))', '$((Escape-SqlLiteral -Value $CollectedAtUtc))', '$((Escape-SqlLiteral -Value $ResultFile))', '$((Escape-SqlLiteral -Value $ErrorFile))', '$((Escape-SqlLiteral -Value $ErrorMessage))');"

    Invoke-Sqlite -Config $Config -Sql $sql | Out-Null
}

if (-not $NoRun) {
try {
    $config = Get-EnvironmentConfig -RequestedPath $ConfigPath
    $script:CurrentConfig = $config

    foreach ($pathKey in @('ScriptBaseDir', 'RawResultBaseDir', 'LogDir', 'TempDir')) {
        if (-not (Test-Path -Path $config[$pathKey] -PathType Container)) {
            New-Item -ItemType Directory -Path $config[$pathKey] -Force | Out-Null
        }
    }

    $script:LogFilePath = Join-Path $config.LogDir ('{0}-{1}.log' -f $config.LogFilePrefix, (Get-Date -Format 'yyyyMMdd-HHmmss'))
    $script:DailyLogDir = Join-Path $config.LogDir 'daily'
    if (-not (Test-Path -Path $script:DailyLogDir -PathType Container)) {
        New-Item -ItemType Directory -Path $script:DailyLogDir -Force | Out-Null
    }
    $script:DailyLogFilePath = Join-Path $script:DailyLogDir ('node-actions-{0}.log' -f (Get-Date -Format 'yyyyMMdd'))

    Log -Message 'Startup'
    Log -Message "Using config: $($config.ConfigPath)"
    Log -Message "Config summary: db=$($config.DatabasePath), raw=$($config.RawResultBaseDir), temp=$($config.TempDir), files=$(@($config.ExcelInputFiles).Count), dirs=$(@($config.ExcelInputDirectories).Count), recurse=$($config.ExcelSearchRecurse), test_mode=$($config.UseTestNodeIPs), test_ips=$(@($config.TestNodeIPs).Count)"

    Initialize-Database -Config $config

    $importResult = $null
    if ($config.UseTestNodeIPs) {
        $importResult = Get-TestNodesFromConfig -Config $config
        Log -Message "Testing mode enabled: using TestNodeIPs only, count=$(@($importResult.Nodes).Count)"
    }
    else {
        $importResult = Import-NodeListFromExcel -Config $config
    }

    $nodes = @($importResult.Nodes)

    if ($nodes.Count -eq 0) {
        Log -Level WARN -Message 'No valid nodes found. Exiting.'
        exit 0
    }

    if (-not $RunId) {
        $RunId = 'run-{0}' -f (Get-Date -Format 'yyyyMMdd-HHmmss')
    }

    $runInfo = Start-MeasurementRun -Config $config -RunId $RunId -SourceFiles $importResult.SourceFiles -TotalNodes $nodes.Count

    $triggeredNodes = New-Object System.Collections.Generic.List[object]
    $reachableCount = 0

    Log -Message "Trigger phase start, nodes=$($nodes.Count)"
    foreach ($node in $nodes) {
        try {
            Log-NodeAction -Node $node -Action 'trigger_start' -Detail 'attempting ssh trigger'
            $triggerResult = Invoke-NodeTriggerCommand -Config $config -Node $node -RunId $RunId
            $triggeredAtUtc = (Get-Date).ToUniversalTime().ToString('o')

            if ($triggerResult.Triggered) {
                $reachableCount++
                $triggeredNodes.Add($node)
                Insert-NodeJobRecord -Config $config -RunId $RunId -Node $node -Status 'triggered' -TriggeredAtUtc $triggeredAtUtc -ResultFile $triggerResult.RemoteResultFile -ErrorFile $triggerResult.RemoteErrorFile
                Log -Message "Node trigger success: $($node.IP)"
                Log-NodeAction -Node $node -Action 'trigger_success' -Detail 'remote background job started'
            }
            else {
                Insert-NodeJobRecord -Config $config -RunId $RunId -Node $node -Status 'trigger_failed' -TriggeredAtUtc $triggeredAtUtc -ErrorMessage $triggerResult.Error
                Log -Level WARN -Message "Node trigger failed: $($node.IP) - $($triggerResult.Error)"
                Log-NodeAction -Node $node -Action 'trigger_failed' -Detail $triggerResult.Error -Level WARN
            }
        }
        catch {
            Insert-NodeJobRecord -Config $config -RunId $RunId -Node $node -Status 'trigger_exception' -TriggeredAtUtc ((Get-Date).ToUniversalTime().ToString('o')) -ErrorMessage $_.Exception.Message
            Log -Level ERROR -Message "Node trigger exception for $($node.IP): $($_.Exception.Message)"
            Log-NodeAction -Node $node -Action 'trigger_exception' -Detail $_.Exception.Message -Level ERROR
        }
    }

    Log -Message "Trigger phase done, reachable=$reachableCount"

    if ($config.CollectWaitSeconds -gt 0) {
        Log -Message "Waiting $($config.CollectWaitSeconds)s before collect phase"
        Start-Sleep -Seconds $config.CollectWaitSeconds
    }

    $collectedCount = 0
    $parsedCount = 0

    Log -Message "Collect phase start, nodes=$($triggeredNodes.Count)"
    foreach ($node in $triggeredNodes) {
        try {
            Log-NodeAction -Node $node -Action 'collect_start' -Detail 'attempting result collection'
            $collect = Collect-NodeResults -Config $config -Node $node -RunId $RunId -RawDir $runInfo.RawDir
            $collectedAtUtc = (Get-Date).ToUniversalTime().ToString('o')

            if (-not $collect.Success) {
                Insert-NodeJobRecord -Config $config -RunId $RunId -Node $node -Status 'collect_failed' -CollectedAtUtc $collectedAtUtc -ErrorMessage $collect.ErrorOutput
                Log -Level WARN -Message "Node collect failed: $($node.IP) - $($collect.ErrorOutput)"
                Log-NodeAction -Node $node -Action 'collect_failed' -Detail $collect.ErrorOutput -Level WARN
                continue
            }

            $collectedCount++
            Insert-NodeJobRecord -Config $config -RunId $RunId -Node $node -Status 'collected' -CollectedAtUtc $collectedAtUtc -ResultFile $collect.LocalResultPath -ErrorFile $collect.LocalErrorPath -ErrorMessage $collect.ErrorOutput
            Log-NodeAction -Node $node -Action 'collect_success' -Detail ('result_file=' + $collect.LocalResultPath)

            $parsed = Parse-MeasurementOutput -RawOutput $collect.RawOutput
            if ($parsed) {
                $parsedCount++
                Log -Message "Parse success: ip=$($node.IP), nodeid=$($parsed.NodeId), throughput_mbit=$($parsed.ThroughputMbit)"
                Log-NodeAction -Node $node -Action 'parse_success' -Detail ('nodeid=' + $parsed.NodeId + '; throughput_mbit=' + $parsed.ThroughputMbit)
            }
            else {
                Log -Level WARN -Message "Parse failed for node $($node.IP), raw stored"
                Log-NodeAction -Node $node -Action 'parse_failed' -Detail 'raw output stored, parser did not match' -Level WARN
            }

            Save-Measurement -Config $config -Node $node -RunId $RunId -RawOutput $collect.RawOutput -ParsedMeasurement $parsed
        }
        catch {
            Insert-NodeJobRecord -Config $config -RunId $RunId -Node $node -Status 'collect_exception' -CollectedAtUtc ((Get-Date).ToUniversalTime().ToString('o')) -ErrorMessage $_.Exception.Message
            Log -Level ERROR -Message "Collect exception for $($node.IP): $($_.Exception.Message)"
            Log-NodeAction -Node $node -Action 'collect_exception' -Detail $_.Exception.Message -Level ERROR
        }
    }

    Complete-MeasurementRun -Config $config -RunId $RunId -ReachableNodes $reachableCount -CollectedNodes $collectedCount -ParsedNodes $parsedCount -Status 'completed'
    Log -Message "Run summary: total=$($nodes.Count), reachable=$reachableCount, collected=$collectedCount, parsed=$parsedCount"
}
catch {
    if ($script:CurrentConfig -and $RunId) {
        try {
            Complete-MeasurementRun -Config $script:CurrentConfig -RunId $RunId -ReachableNodes 0 -CollectedNodes 0 -ParsedNodes 0 -Status 'failed' -Notes $_.Exception.Message
        }
        catch {
            # Ignore secondary failure while reporting primary error.
        }
    }

    Log -Level ERROR -Message "Fatal error: $($_.Exception.Message)"
    exit 1
}
}

