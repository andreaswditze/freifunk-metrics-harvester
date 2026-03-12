# Functions for this concern are loaded by FreifunkMetrics.psm1.



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

function ConvertTo-SqlEscapedLiteral {
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
    result_type TEXT,
    failure_reason TEXT,
    downloaded_bytes INTEGER,
    expected_bytes INTEGER,
    download_duration_seconds REAL,
    timeout_seconds INTEGER,
    wget_exit_code INTEGER,
    wget_exit_reason TEXT,
    wget_stderr TEXT,
    raw_output TEXT NOT NULL,
    collected_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS node_diagnostics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    device_id TEXT,
    name TEXT,
    ip TEXT,
    domain TEXT,
    nodeid TEXT,
    diagnostic_timestamp_ns TEXT,
    diagnosed_at_utc TEXT,
    speedtest_delay_seconds INTEGER,
    diagnostic_delay_seconds INTEGER,
    target_host TEXT,
    gateway_probe TEXT,
    gateway_probe_kind TEXT,
    ping_gateway_loss_pct REAL,
    ping_target_loss_pct REAL,
    load1 REAL,
    load5 REAL,
    load15 REAL,
    target_ipv4 TEXT,
    target_ipv6 TEXT,
    route_get_ipv4 TEXT,
    route_get_ipv6 TEXT,
    wget_stderr TEXT,
    tcp_gateway_probe_port INTEGER,
    tcp_gateway_probe_result TEXT,
    tcp_target_probe_port INTEGER,
    tcp_target_probe_result TEXT,
    target_resolution TEXT,
    route_get TEXT,
    tcp_gateway_probe TEXT,
    tcp_target_probe TEXT,
    ip_rule TEXT,
    batctl_if TEXT,
    batctl_n TEXT,
    ubus_network_dump TEXT,
    ubus_ifstatus_wan TEXT,
    ubus_ifstatus_wan6 TEXT,
    local_path TEXT,
    raw_output TEXT NOT NULL,
    collected_at_utc TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_nodes_last_seen_utc ON nodes(last_seen_utc);
CREATE INDEX IF NOT EXISTS idx_node_jobs_run_id ON node_jobs(run_id);
CREATE INDEX IF NOT EXISTS idx_node_jobs_run_status ON node_jobs(run_id, status);
CREATE INDEX IF NOT EXISTS idx_measurements_run_id ON measurements(run_id);
CREATE INDEX IF NOT EXISTS idx_measurements_device_id ON measurements(device_id);
CREATE INDEX IF NOT EXISTS idx_measurements_nodeid ON measurements(nodeid);
CREATE INDEX IF NOT EXISTS idx_measurements_run_device_id ON measurements(run_id, device_id);
CREATE INDEX IF NOT EXISTS idx_measurements_measured_at_utc ON measurements(measured_at_utc);
CREATE INDEX IF NOT EXISTS idx_node_diagnostics_run_id ON node_diagnostics(run_id);
CREATE INDEX IF NOT EXISTS idx_node_diagnostics_device_id ON node_diagnostics(device_id);
CREATE INDEX IF NOT EXISTS idx_node_diagnostics_run_device_id ON node_diagnostics(run_id, device_id);
"@
    Invoke-Sqlite -Config $Config -Sql $ddl | Out-Null
    Write-Log -Message "Database initialized: $($Config.DatabasePath)"
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
    $sourceJoined = ConvertTo-SqlEscapedLiteral -Value ($SourceFiles -join ';')
    $sql = "INSERT INTO runs (run_id, started_at_utc, status, source_files, total_nodes) VALUES ('$((ConvertTo-SqlEscapedLiteral -Value $RunId))', '$startedUtc', 'running', '$sourceJoined', $TotalNodes);"

    Invoke-Sqlite -Config $Config -Sql $sql | Out-Null

    $runRawDir = Join-Path $Config.RawResultBaseDir $RunId
    New-Item -ItemType Directory -Path $runRawDir -Force | Out-Null

    Write-Log -Message "Measurement run started: ${RunId}"

    return [pscustomobject]@{
        RunId        = $RunId
        StartedAtUtc = $startedUtc
        RawDir       = $runRawDir
    }
}

function Get-RecentAverageThroughputByIp {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Config
    )

    $throughputByIp = @{}
    $throughputSumByIp = @{}
    $sampleCountByIp = @{}
    $dbPath = if ($Config.ContainsKey('DatabasePath')) { Convert-ToTrimmedString -Value $Config.DatabasePath } else { '' }
    $sqliteBinary = if ($Config.ContainsKey('SQLiteBinary')) { Convert-ToTrimmedString -Value $Config.SQLiteBinary } else { '' }

    if ([string]::IsNullOrWhiteSpace($dbPath) -or [string]::IsNullOrWhiteSpace($sqliteBinary) -or -not (Test-Path -Path $dbPath -PathType Leaf)) {
        return $throughputByIp
    }

    $sql = @"
SELECT ip, COALESCE(throughput_mbit, 0), COALESCE(NULLIF(measured_at_utc, ''), collected_at_utc, ''), id
FROM measurements
WHERE ip IS NOT NULL AND ip <> ''
ORDER BY ip ASC, COALESCE(NULLIF(measured_at_utc, ''), collected_at_utc, '') DESC, id DESC;
"@

    foreach ($row in @(Invoke-Sqlite -Config $Config -Sql $sql)) {
        $parts = @([string]$row -split '\|', 4)
        if ($parts.Count -lt 2) {
            continue
        }

        $ip = Convert-ToTrimmedString -Value $parts[0]
        if ([string]::IsNullOrWhiteSpace($ip)) {
            continue
        }

        $throughput = 0.0
        $rawThroughput = Convert-ToTrimmedString -Value $parts[1]
        if (-not [string]::IsNullOrWhiteSpace($rawThroughput)) {
            [double]::TryParse($rawThroughput, [System.Globalization.NumberStyles]::Float, [System.Globalization.CultureInfo]::InvariantCulture, [ref]$throughput) | Out-Null
        }

        if (-not $sampleCountByIp.ContainsKey($ip)) {
            $sampleCountByIp[$ip] = 0
            $throughputSumByIp[$ip] = 0.0
        }

        if ($sampleCountByIp[$ip] -ge 7) {
            continue
        }

        $sampleCountByIp[$ip]++
        $throughputSumByIp[$ip] += $throughput
    }

    foreach ($ip in $sampleCountByIp.Keys) {
        $throughputByIp[$ip] = $throughputSumByIp[$ip] / [double]$sampleCountByIp[$ip]
    }

    return $throughputByIp
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
        [AllowEmptyString()]
        [string]$RawOutput,
        [AllowNull()]
        [pscustomobject]$ParsedMeasurement
    )

    $nowUtc = (Get-Date).ToUniversalTime().ToString('o')

    $deviceId = ConvertTo-SqlEscapedLiteral -Value $Node.DeviceID
    $name = ConvertTo-SqlEscapedLiteral -Value $Node.Name
    $ip = ConvertTo-SqlEscapedLiteral -Value $Node.IP
    $domain = ConvertTo-SqlEscapedLiteral -Value $Node.Domain
    $rawEsc = ConvertTo-SqlEscapedLiteral -Value $RawOutput

    $upsertNodeSql = "INSERT INTO nodes (device_id, name, ip, domain, first_seen_utc, last_seen_utc) VALUES ('$deviceId', '$name', '$ip', '$domain', '$nowUtc', '$nowUtc') ON CONFLICT(device_id, ip) DO UPDATE SET name = excluded.name, domain = excluded.domain, last_seen_utc = excluded.last_seen_utc;"
    Invoke-Sqlite -Config $Config -Sql $upsertNodeSql | Out-Null

    $nodeId = ''
    $target = ''
    $throughput = 'NULL'
    $tsNs = ''
    $measuredAtUtc = ''
    $resultType = ''
    $failureReason = ''
    $downloadedBytes = 'NULL'
    $expectedBytes = 'NULL'
    $downloadDurationSeconds = 'NULL'
    $timeoutSeconds = 'NULL'
    $wgetExitCode = 'NULL'
    $wgetExitReason = ''
    $wgetStderr = ''

    if ($ParsedMeasurement) {
        $nodeId = ConvertTo-SqlEscapedLiteral -Value $ParsedMeasurement.NodeId
        $target = ConvertTo-SqlEscapedLiteral -Value $ParsedMeasurement.Target
        $throughput = [string]::Format([System.Globalization.CultureInfo]::InvariantCulture, '{0:0.00}', $ParsedMeasurement.ThroughputMbit)
        $tsNs = ConvertTo-SqlEscapedLiteral -Value $ParsedMeasurement.TimestampNs
        $measuredAtUtc = Convert-NodeTimestampToUtc -Timestamp $ParsedMeasurement.TimestampNs
        $resultType = ConvertTo-SqlEscapedLiteral -Value $ParsedMeasurement.ResultType
        $failureReason = ConvertTo-SqlEscapedLiteral -Value $ParsedMeasurement.FailureReason
        if ($null -ne $ParsedMeasurement.PSObject.Properties['DownloadedBytes'] -and $null -ne $ParsedMeasurement.DownloadedBytes) {
            $downloadedBytes = [string]$ParsedMeasurement.DownloadedBytes
        }
        if ($null -ne $ParsedMeasurement.PSObject.Properties['ExpectedBytes'] -and $null -ne $ParsedMeasurement.ExpectedBytes) {
            $expectedBytes = [string]$ParsedMeasurement.ExpectedBytes
        }
        if ($null -ne $ParsedMeasurement.PSObject.Properties['DownloadDurationSeconds'] -and $null -ne $ParsedMeasurement.DownloadDurationSeconds) {
            $downloadDurationSeconds = [string]::Format([System.Globalization.CultureInfo]::InvariantCulture, '{0:0.000000}', [double]$ParsedMeasurement.DownloadDurationSeconds)
        }
        if ($null -ne $ParsedMeasurement.PSObject.Properties['TimeoutSeconds'] -and $null -ne $ParsedMeasurement.TimeoutSeconds) {
            $timeoutSeconds = [string]$ParsedMeasurement.TimeoutSeconds
        }
        if ($null -ne $ParsedMeasurement.PSObject.Properties['WgetExitCode'] -and $null -ne $ParsedMeasurement.WgetExitCode) {
            $wgetExitCode = [string]$ParsedMeasurement.WgetExitCode
        }
        if ($null -ne $ParsedMeasurement.PSObject.Properties['WgetExitReason'] -and $null -ne $ParsedMeasurement.WgetExitReason) {
            $wgetExitReason = ConvertTo-SqlEscapedLiteral -Value $ParsedMeasurement.WgetExitReason
        }
        if ($null -ne $ParsedMeasurement.PSObject.Properties['WgetStderr'] -and $null -ne $ParsedMeasurement.WgetStderr) {
            $wgetStderr = ConvertTo-SqlEscapedLiteral -Value $ParsedMeasurement.WgetStderr
        }
    }

    $measuredAtEsc = ConvertTo-SqlEscapedLiteral -Value $measuredAtUtc

    $insertMeasurement = "INSERT INTO measurements (run_id, device_id, name, ip, domain, nodeid, target, throughput_mbit, measurement_timestamp_ns, measured_at_utc, result_type, failure_reason, downloaded_bytes, expected_bytes, download_duration_seconds, timeout_seconds, wget_exit_code, wget_exit_reason, wget_stderr, raw_output, collected_at_utc) VALUES ('$((ConvertTo-SqlEscapedLiteral -Value $RunId))', '$deviceId', '$name', '$ip', '$domain', '$nodeId', '$target', $throughput, '$tsNs', '$measuredAtEsc', '$resultType', '$failureReason', $downloadedBytes, $expectedBytes, $downloadDurationSeconds, $timeoutSeconds, $wgetExitCode, '$wgetExitReason', '$wgetStderr', '$rawEsc', '$nowUtc');"

    Invoke-Sqlite -Config $Config -Sql $insertMeasurement | Out-Null
    Write-Log -Message "DB insert complete for node ${ip}"
}

function Save-NodeDiagnostic {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Config,
        [Parameter(Mandatory = $true)]
        [pscustomobject]$Node,
        [Parameter(Mandatory = $true)]
        [string]$RunId,
        [Parameter(Mandatory = $true)]
        [pscustomobject]$Diagnostic
    )

    $nowUtc = (Get-Date).ToUniversalTime().ToString('o')
    $diagnosedAtUtc = Convert-NodeTimestampToUtc -Timestamp $Diagnostic.TimestampNs

    $insertSql = @"
INSERT INTO node_diagnostics (
    run_id, device_id, name, ip, domain, nodeid, diagnostic_timestamp_ns, diagnosed_at_utc,
    speedtest_delay_seconds, diagnostic_delay_seconds, target_host, gateway_probe, gateway_probe_kind,
    ping_gateway_loss_pct, ping_target_loss_pct, load1, load5, load15,
    target_ipv4, target_ipv6, route_get_ipv4, route_get_ipv6, wget_stderr,
    tcp_gateway_probe_port, tcp_gateway_probe_result, tcp_target_probe_port, tcp_target_probe_result,
    target_resolution, route_get, tcp_gateway_probe, tcp_target_probe, ip_rule, batctl_if, batctl_n, ubus_network_dump, ubus_ifstatus_wan, ubus_ifstatus_wan6,
    local_path, raw_output, collected_at_utc
) VALUES (
    '$((ConvertTo-SqlEscapedLiteral -Value $RunId))',
    '$((ConvertTo-SqlEscapedLiteral -Value $Node.DeviceID))',
    '$((ConvertTo-SqlEscapedLiteral -Value $Node.Name))',
    '$((ConvertTo-SqlEscapedLiteral -Value $Node.IP))',
    '$((ConvertTo-SqlEscapedLiteral -Value $Node.Domain))',
    '$((ConvertTo-SqlEscapedLiteral -Value $Diagnostic.NodeId))',
    '$((ConvertTo-SqlEscapedLiteral -Value $Diagnostic.TimestampNs))',
    '$((ConvertTo-SqlEscapedLiteral -Value $diagnosedAtUtc))',
    $([int]$Diagnostic.SpeedtestDelaySeconds),
    $([int]$Diagnostic.DiagnosticDelaySeconds),
    '$((ConvertTo-SqlEscapedLiteral -Value $Diagnostic.TargetHost))',
    '$((ConvertTo-SqlEscapedLiteral -Value $Diagnostic.GatewayProbe))',
    '$((ConvertTo-SqlEscapedLiteral -Value $Diagnostic.GatewayProbeKind))',
    $([string]::Format([System.Globalization.CultureInfo]::InvariantCulture, '{0:0.###}', [double]$Diagnostic.PingGatewayLossPct)),
    $([string]::Format([System.Globalization.CultureInfo]::InvariantCulture, '{0:0.###}', [double]$Diagnostic.PingTargetLossPct)),
    $([string]::Format([System.Globalization.CultureInfo]::InvariantCulture, '{0:0.###}', [double]$Diagnostic.Load1)),
    $([string]::Format([System.Globalization.CultureInfo]::InvariantCulture, '{0:0.###}', [double]$Diagnostic.Load5)),
    $([string]::Format([System.Globalization.CultureInfo]::InvariantCulture, '{0:0.###}', [double]$Diagnostic.Load15)),
    '$((ConvertTo-SqlEscapedLiteral -Value $Diagnostic.TargetIPv4))',
    '$((ConvertTo-SqlEscapedLiteral -Value $Diagnostic.TargetIPv6))',
    '$((ConvertTo-SqlEscapedLiteral -Value $Diagnostic.RouteGetIPv4))',
    '$((ConvertTo-SqlEscapedLiteral -Value $Diagnostic.RouteGetIPv6))',
    '$((ConvertTo-SqlEscapedLiteral -Value $Diagnostic.WgetStderr))',
    $([int]$Diagnostic.TcpGatewayProbePort),
    '$((ConvertTo-SqlEscapedLiteral -Value $Diagnostic.TcpGatewayProbeResult))',
    $([int]$Diagnostic.TcpTargetProbePort),
    '$((ConvertTo-SqlEscapedLiteral -Value $Diagnostic.TcpTargetProbeResult))',
    '$((ConvertTo-SqlEscapedLiteral -Value $Diagnostic.TargetResolution))',
    '$((ConvertTo-SqlEscapedLiteral -Value $Diagnostic.RouteGet))',
    '$((ConvertTo-SqlEscapedLiteral -Value $Diagnostic.TcpGatewayProbe))',
    '$((ConvertTo-SqlEscapedLiteral -Value $Diagnostic.TcpTargetProbe))',
    '$((ConvertTo-SqlEscapedLiteral -Value $Diagnostic.IpRule))',
    '$((ConvertTo-SqlEscapedLiteral -Value $Diagnostic.BatctlIf))',
    '$((ConvertTo-SqlEscapedLiteral -Value $Diagnostic.BatctlN))',
    '$((ConvertTo-SqlEscapedLiteral -Value $Diagnostic.UbusNetworkDump))',
    '$((ConvertTo-SqlEscapedLiteral -Value $Diagnostic.UbusIfstatusWan))',
    '$((ConvertTo-SqlEscapedLiteral -Value $Diagnostic.UbusIfstatusWan6))',
    '$((ConvertTo-SqlEscapedLiteral -Value $Diagnostic.LocalPath))',
    '$((ConvertTo-SqlEscapedLiteral -Value $Diagnostic.RawOutput))',
    '$nowUtc'
);
"@

    Invoke-Sqlite -Config $Config -Sql $insertSql | Out-Null
    Write-Log -Message "Diagnostic DB insert complete for node $($Node.IP)"
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
    $notesEsc = ConvertTo-SqlEscapedLiteral -Value $Notes

    $sql = "UPDATE runs SET completed_at_utc = '$completedUtc', status = '$((ConvertTo-SqlEscapedLiteral -Value $Status))', reachable_nodes = $ReachableNodes, collected_nodes = $CollectedNodes, parsed_nodes = $ParsedNodes, notes = '$notesEsc' WHERE run_id = '$((ConvertTo-SqlEscapedLiteral -Value $RunId))';"

    Invoke-Sqlite -Config $Config -Sql $sql | Out-Null
    Write-Log -Message "Run completed: ${RunId}, status=${Status}, reachable=${ReachableNodes}, collected=${CollectedNodes}, parsed=${ParsedNodes}"
}

function Add-NodeJobRecord {
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

    $sql = "INSERT INTO node_jobs (run_id, device_id, name, ip, domain, status, triggered_at_utc, collected_at_utc, result_file, error_file, error_message) VALUES ('$((ConvertTo-SqlEscapedLiteral -Value $RunId))', '$((ConvertTo-SqlEscapedLiteral -Value $Node.DeviceID))', '$((ConvertTo-SqlEscapedLiteral -Value $Node.Name))', '$((ConvertTo-SqlEscapedLiteral -Value $Node.IP))', '$((ConvertTo-SqlEscapedLiteral -Value $Node.Domain))', '$((ConvertTo-SqlEscapedLiteral -Value $Status))', '$((ConvertTo-SqlEscapedLiteral -Value $TriggeredAtUtc))', '$((ConvertTo-SqlEscapedLiteral -Value $CollectedAtUtc))', '$((ConvertTo-SqlEscapedLiteral -Value $ResultFile))', '$((ConvertTo-SqlEscapedLiteral -Value $ErrorFile))', '$((ConvertTo-SqlEscapedLiteral -Value $ErrorMessage))');"

    Invoke-Sqlite -Config $Config -Sql $sql | Out-Null
}
