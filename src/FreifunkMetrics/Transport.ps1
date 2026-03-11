# Functions for this concern are loaded by FreifunkMetrics.psm1.



function Get-SshHostKeyArgs {
    [CmdletBinding()]
    param()

    $nullDevice = if ($IsWindows) { 'NUL' } else { '/dev/null' }

    return @(
        '-o', 'StrictHostKeyChecking=no',
        '-o', "UserKnownHostsFile=$nullDevice",
        '-o', "GlobalKnownHostsFile=$nullDevice",
        '-o', 'LogLevel=ERROR'
    )
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
        '-o', "ConnectTimeout=$($Config.SshConnectTimeoutSeconds)"
    ) + (Get-SshHostKeyArgs) + @(
        "$($Config.SshUser)@${NodeIp}"
    )
}

function Get-RemoteRunResultDir {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Config,
        [Parameter(Mandatory = $true)]
        [string]$RunId
    )

    $baseDir = (Convert-ToTrimmedString -Value $Config.RemoteResultDir).TrimEnd('/')
    $safeRunId = Get-SafeFileNamePart -Value $RunId

    if ([string]::IsNullOrWhiteSpace($baseDir)) {
        throw 'Config value RemoteResultDir must not be empty.'
    }

    if ([string]::IsNullOrWhiteSpace($safeRunId)) {
        throw 'RunId must not be empty.'
    }

    return "$baseDir/$safeRunId"
}

function Test-NodeResultFinished {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Config,
        [Parameter(Mandatory = $true)]
        [string]$RunId,
        [Parameter(Mandatory = $true)]
        [object]$Node
    )

    $sshArgs = New-SshArgs -Config $Config -NodeIp $Node.IP
    $remoteRunDir = Get-RemoteRunResultDir -Config $Config -RunId $RunId
    $remoteDirEscaped = Convert-ToShellSingleQuoted -Value $remoteRunDir
    $probeCmd = @"
find '$remoteDirEscaped' -maxdepth 1 -type f -name '*.txt' -print | sort | while IFS= read -r file; do
    if grep -Eq '^(speedtest,nodeid=|wget_failed,nodeid=|speedtest_invalid,nodeid=|speedtest_size_mismatch,nodeid=)' "`$file"; then
        printf '%s\n' "`$file"
        break
    fi
done
"@
    $output = & $Config.SshBinary @sshArgs $probeCmd 2>&1
    $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0) {
        return $false
    }

    $text = Convert-ToTrimmedString -Value ($output -join "`n")
    return -not [string]::IsNullOrWhiteSpace($text)
}

function Get-FinishedNodeResultCountBatch {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Config,
        [Parameter(Mandatory = $true)]
        [string]$RunId,
        [Parameter(Mandatory = $true)]
        [object[]]$Nodes
    )

    if (@($Nodes).Count -eq 0) {
        return 0
    }

    $indexedNodes = for ($i = 0; $i -lt $Nodes.Count; $i++) {
        [pscustomobject]@{
            Index = $i
            Node  = $Nodes[$i]
        }
    }

    $parallelism = [Math]::Max(1, [int]([Math]::Min([Math]::Max(1, [int]$Config.CollectParallelism), $indexedNodes.Count)))
    if ($parallelism -le 1 -or $indexedNodes.Count -le 1) {
        return @($indexedNodes | Where-Object { Test-NodeResultFinished -Config $Config -RunId $RunId -Node $_.Node }).Count
    }

    $batchConfig = $Config
    $batchRunId = $RunId
    $modulePath = $script:ModuleFilePath
    $ready = @(
        $indexedNodes |
            ForEach-Object -Parallel {
                $item = $_
                $config = $using:batchConfig
                $runId = $using:batchRunId
                $modulePath = $using:modulePath
                Import-Module $modulePath -Force | Out-Null
                if (Test-NodeResultFinished -Config $config -RunId $runId -Node $item.Node) {
                    $item.Index
                }
            } -ThrottleLimit $parallelism
    )

    return @($ready).Count
}

function Get-NodeTriggerAssignmentOrderKey {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$RunId,
        [Parameter(Mandatory = $true)]
        [object]$Node
    )

    $seed = @(
        $RunId,
        (Convert-ToTrimmedString -Value $Node.DeviceID),
        (Convert-ToTrimmedString -Value $Node.IP),
        (Convert-ToTrimmedString -Value $Node.Name)
    ) -join '|'

    $bytes = [System.Text.Encoding]::UTF8.GetBytes($seed)
    $hashBytes = [System.Security.Cryptography.SHA256]::HashData($bytes)
    return [Convert]::ToHexString($hashBytes)
}

function Get-NodeTriggerAssignments {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Config,
        [Parameter(Mandatory = $true)]
        [string]$RunId,
        [Parameter(Mandatory = $true)]
        [object[]]$Nodes
    )

    if (@($Nodes).Count -eq 0) {
        return @()
    }

    $indexedNodes = for ($i = 0; $i -lt $Nodes.Count; $i++) {
        [pscustomobject]@{
            Index = $i
            Node  = $Nodes[$i]
        }
    }

    $delayMaxSeconds = [Math]::Max(0, [int]$Config.TriggerRandomDelayMaxSeconds)
    $throughputByIp = Get-LatestThroughputByIp -Config $Config
    $delayByIndex = @{}

    foreach ($item in $indexedNodes) {
        $delayByIndex[$item.Index] = 0
    }

    if ($delayMaxSeconds -le 0) {
        return @(
            $indexedNodes |
                ForEach-Object {
                    [pscustomobject]@{
                        Index                = $_.Index
                        Node                 = $_.Node
                        AssignedDelaySeconds = 0
                    }
                }
        )
    }

    $orderedNodes = @(
        $indexedNodes |
            ForEach-Object {
                $ip = Convert-ToTrimmedString -Value $_.Node.IP
                $latestThroughput = 0.0
                if (-not [string]::IsNullOrWhiteSpace($ip) -and $throughputByIp.ContainsKey($ip)) {
                    $latestThroughput = [double]$throughputByIp[$ip]
                }

                [pscustomobject]@{
                    Index            = $_.Index
                    Node             = $_.Node
                    LatestThroughput = $latestThroughput
                }
            } |
            Sort-Object @{ Expression = { $_.LatestThroughput } }, @{ Expression = { Get-NodeTriggerAssignmentOrderKey -RunId $RunId -Node $_.Node } }, @{ Expression = { $_.Index } }
    )

    if ($orderedNodes.Count -gt 0) {
        for ($rank = 0; $rank -lt $orderedNodes.Count; $rank++) {
            $assignedDelay = if ($orderedNodes.Count -eq 1) {
                0
            }
            else {
                $rawDelay = [int][Math]::Round((($rank * $delayMaxSeconds) / [double]($orderedNodes.Count - 1)), [System.MidpointRounding]::AwayFromZero)
                [Math]::Max(0, [Math]::Min($delayMaxSeconds, $rawDelay))
            }

            $delayByIndex[$orderedNodes[$rank].Index] = $assignedDelay
        }
    }

    return @(
        $indexedNodes |
            ForEach-Object {
                [pscustomobject]@{
                    Index                = $_.Index
                    Node                 = $_.Node
                    AssignedDelaySeconds = $delayByIndex[$_.Index]
                }
            }
    )
}


function Get-NodeDiagnosticsSettings {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Config
    )

    $enabled = if ($Config.ContainsKey('EnableNodeDiagnostics')) { [bool]$Config.EnableNodeDiagnostics } else { $true }
    $delaySeconds = if ($Config.ContainsKey('NodeDiagnosticsDelaySeconds')) { [Math]::Max(0, [int]$Config.NodeDiagnosticsDelaySeconds) } else { 60 }
    $keepThresholdMbit = if ($Config.ContainsKey('NodeDiagnosticsKeepThresholdMbit')) {
        [double]::Parse([string]$Config.NodeDiagnosticsKeepThresholdMbit, [System.Globalization.CultureInfo]::InvariantCulture)
    }
    else {
        10.0
    }

    return [pscustomobject]@{
        Enabled           = $enabled
        DelaySeconds      = $delaySeconds
        KeepThresholdMbit = $keepThresholdMbit
    }
}

function Get-NodeDiagnosticsTargetHost {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Config
    )

    $uri = $null
    if (-not [uri]::TryCreate((Convert-ToTrimmedString -Value $Config.SpeedtestTargetUrl), [System.UriKind]::Absolute, [ref]$uri)) {
        return ''
    }

    return $uri.Host
}

function Test-ShouldKeepNodeDiagnostics {
    [CmdletBinding()]
    param(
        [AllowEmptyCollection()]
        [object[]]$MeasurementFiles = @(),
        [Parameter(Mandatory = $true)]
        [double]$KeepThresholdMbit
    )

    if (@($MeasurementFiles).Count -eq 0) {
        return $true
    }

    foreach ($file in @($MeasurementFiles)) {
        if ($null -eq $file.ParsedMeasurement) {
            continue
        }

        if ($file.ParsedMeasurement.ResultType -eq 'final_failed') {
            return $true
        }

        if ($file.ParsedMeasurement.ResultType -eq 'success' -and [double]$file.ParsedMeasurement.ThroughputMbit -le $KeepThresholdMbit) {
            return $true
        }
    }

    return $false
}

function Remove-NodeDiagnosticArtifacts {
    [CmdletBinding()]
    param(
        [AllowEmptyCollection()]
        [object[]]$DiagnosticFiles = @()
    )

    foreach ($file in @($DiagnosticFiles)) {
        $localPath = Convert-ToTrimmedString -Value $file.LocalPath
        if (-not [string]::IsNullOrWhiteSpace($localPath) -and (Test-Path -Path $localPath -PathType Leaf)) {
            Remove-Item -Path $localPath -Force
        }
    }
}

function Get-NodeTriggerCommandInfo {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Config,
        [Parameter(Mandatory = $true)]
        [string]$RunId,
        [Parameter(Mandatory = $true)]
        [int]$AssignedDelaySeconds
    )

    $remoteRunDir = Get-RemoteRunResultDir -Config $Config -RunId $RunId
    $remoteResultPattern = "$remoteRunDir/*.txt"

    $delaySeconds = [Math]::Max(0, [int]$AssignedDelaySeconds)
    $targetUrl = Convert-ToTrimmedString -Value $Config.SpeedtestTargetUrl
    $targetUrlShell = Convert-ToShellSingleQuoted -Value $targetUrl
    $targetBytes = [Math]::Max(1, [int64]$Config.SpeedtestTargetBytes)
    $downloadTimeoutSeconds = if ($Config.ContainsKey('SpeedtestDownloadTimeoutSeconds')) { [Math]::Max(1, [int]$Config.SpeedtestDownloadTimeoutSeconds) } else { 180 }
    $diagnostics = Get-NodeDiagnosticsSettings -Config $Config
    $diagnosticDelaySeconds = $delaySeconds + $diagnostics.DelaySeconds
    $targetHost = Convert-ToShellSingleQuoted -Value (Get-NodeDiagnosticsTargetHost -Config $Config)

    $payload = @"
nodeid=`$(tr -d ':' </lib/gluon/core/sysconfig/primary_mac)
target_url='$targetUrlShell'
delay_seconds=$delaySeconds
sleep "`$delay_seconds"
start=`$(date +%s%N)
wget_exit_file="/tmp/harvester-wget-exit-`$$.txt"
rm -f "`$wget_exit_file"
t0=`$(date +%s.%N)
wget -O /dev/null -q -T $downloadTimeoutSeconds "`$target_url" &
wget_pid=`$!
(
    sleep $downloadTimeoutSeconds
    if kill -0 "`$wget_pid" 2>/dev/null; then
        kill "`$wget_pid" 2>/dev/null || true
        sleep 1
        kill -9 "`$wget_pid" 2>/dev/null || true
        printf '%s' '124' > "`$wget_exit_file"
    fi
) &
wget_watchdog_pid=`$!
wait "`$wget_pid"
wget_wait_exit=`$?
kill "`$wget_watchdog_pid" 2>/dev/null || true
wait "`$wget_watchdog_pid" 2>/dev/null || true
if [ -f "`$wget_exit_file" ]; then
    wget_exit=`$(cat "`$wget_exit_file" 2>/dev/null)
else
    wget_exit="`$wget_wait_exit"
    printf '%s' "`$wget_exit" > "`$wget_exit_file"
fi
t1=`$(date +%s.%N)
bytes=0
if [ "`$wget_exit" = "0" ]; then
    bytes="$targetBytes"
fi
rm -f "`$wget_exit_file"
awk -v nodeid="`$nodeid" -v start="`$start" -v t0="`$t0" -v t1="`$t1" -v target="`$target_url" -v bytes="`$bytes" -v wget_exit="`$wget_exit" -v expected_bytes="$targetBytes" 'BEGIN{
    sec=t1-t0
    if (wget_exit != 0) {
        printf "wget_failed,nodeid=%s exit=%s bytes=%s expected_bytes=%s target=\"%s\" %s\n",nodeid,wget_exit,bytes,expected_bytes,target,start
        exit 0
    }
    if (bytes <= 0 || sec <= 0) {
        printf "speedtest_invalid,nodeid=%s bytes=%s sec=%s expected_bytes=%s target=\"%s\" %s\n",nodeid,bytes,sec,expected_bytes,target,start
        exit 0
    }
    if (bytes != expected_bytes) {
        printf "speedtest_size_mismatch,nodeid=%s bytes=%s expected_bytes=%s target=\"%s\" %s\n",nodeid,bytes,expected_bytes,target,start
        exit 0
    }
    printf "speedtest,nodeid=%s download_mbit=%.2f,target=\"%s\" %s\n",nodeid,(bytes*8)/(sec*1000000),target,start
}'
"@

    $diagnosticPayload = @"
nodeid=`$(tr -d ':' </lib/gluon/core/sysconfig/primary_mac)
target_host='$targetHost'
speedtest_delay_seconds=$delaySeconds
diagnostic_delay_seconds=$diagnosticDelaySeconds
sleep "`$diagnostic_delay_seconds"
ts=`$(date +%s%N)
gateway4=`$(ip route 2>/dev/null | awk '/^default / { print `$3; exit }')
gateway6=`$(ip -6 route 2>/dev/null | awk '/^default / { print `$3; exit }')
gateway_probe="`$gateway4"
gateway_probe_kind='ipv4'
if [ -z "`$gateway_probe" ] && [ -n "`$gateway6" ]; then
    gateway_probe="`$gateway6"
    gateway_probe_kind='ipv6'
fi
ping_gateway_loss='-1'
if [ -n "`$gateway_probe" ]; then
    if [ "`$gateway_probe_kind" = 'ipv6' ]; then
        ping_gateway_output=`$(ping6 -q -c 4 -w 8 "`$gateway_probe" 2>&1 || true)
    else
        ping_gateway_output=`$(ping -q -c 4 -w 8 "`$gateway_probe" 2>&1 || true)
    fi
    ping_gateway_loss=`$(printf '%s\n' "`$ping_gateway_output" | awk -F', ' '/packet loss/ { gsub(/% packet loss/, "", `$3); print `$3; found=1; exit } END { if (!found) print "-1" }')
fi
ping_target_loss='-1'
if [ -n "`$target_host" ]; then
    ping_target_output=`$(ping -q -c 4 -w 8 "`$target_host" 2>&1 || true)
    ping_target_loss=`$(printf '%s\n' "`$ping_target_output" | awk -F', ' '/packet loss/ { gsub(/% packet loss/, "", `$3); print `$3; found=1; exit } END { if (!found) print "-1" }')
fi
load1='0'
load5='0'
load15='0'
if [ -r /proc/loadavg ]; then
    read load1 load5 load15 _ </proc/loadavg
fi
printf 'diagnostic,nodeid=%s target_host="%s" speedtest_delay_seconds=%s diagnostic_delay_seconds=%s timestamp=%s\n' "`$nodeid" "`$target_host" "`$speedtest_delay_seconds" "`$diagnostic_delay_seconds" "`$ts"
printf 'diag_summary,load1=%s load5=%s load15=%s gateway_probe="%s" gateway_probe_kind="%s" ping_gateway_loss=%s ping_target_loss=%s\n' "`$load1" "`$load5" "`$load15" "`$gateway_probe" "`$gateway_probe_kind" "`$ping_gateway_loss" "`$ping_target_loss"
echo 'diag_section,name=ip_route'
ip route 2>&1 || true
echo 'diag_section_end,name=ip_route'
echo 'diag_section,name=ip6_route'
ip -6 route 2>&1 || true
echo 'diag_section_end,name=ip6_route'
echo 'diag_section,name=ip_addr'
ip addr 2>&1 || true
echo 'diag_section_end,name=ip_addr'
echo 'diag_section,name=ip_link_stats'
ip -s link 2>&1 || true
echo 'diag_section_end,name=ip_link_stats'
echo 'diag_section,name=loadavg'
cat /proc/loadavg 2>&1 || true
echo 'diag_section_end,name=loadavg'
echo 'diag_section,name=meminfo_head'
sed -n '1,5p' /proc/meminfo 2>&1 || true
echo 'diag_section_end,name=meminfo_head'
if command -v batctl >/dev/null 2>&1; then
    echo 'diag_section,name=batctl_if'
    batctl if 2>&1 || true
    echo 'diag_section_end,name=batctl_if'
    echo 'diag_section,name=batctl_n'
    batctl n 2>&1 || true
    echo 'diag_section_end,name=batctl_n'
fi
if command -v logread >/dev/null 2>&1; then
    echo 'diag_section,name=logread_tail'
    logread 2>&1 | tail -n 40 || true
    echo 'diag_section_end,name=logread_tail'
fi
"@

    $remoteDirEscaped = Convert-ToShellSingleQuoted -Value $remoteRunDir
    $triggerSegments = @(
        "mkdir -p '$remoteDirEscaped'",
        "ts=`$(date +%s%N)",
        "out='$remoteDirEscaped/'`$ts.txt",
        "( $payload ) > `"`$out`" 2>&1 &"
    )

    if ($diagnostics.Enabled) {
        $triggerSegments += @(
            "diag_ts=`$(date +%s%N)",
            "diag_out='$remoteDirEscaped/diag-'`$diag_ts.txt",
            "( $diagnosticPayload ) > `"`$diag_out`" 2>&1 &"
        )
    }

    $triggerCmd = $triggerSegments -join "`n"

    return [pscustomobject]@{
        RemoteResultFile              = $remoteResultPattern
        RemoteErrorFile               = ''
        TriggerCommand                = $triggerCmd
        AssignedDelaySeconds          = $delaySeconds
        DiagnosticDelaySeconds        = $diagnosticDelaySeconds
        DiagnosticsEnabled            = $diagnostics.Enabled
        DiagnosticsKeepThresholdMbit  = $diagnostics.KeepThresholdMbit
    }
}

function Invoke-NodeTriggerCommand {

    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Config,
        [Parameter(Mandatory = $true)]
        [pscustomobject]$Node,
        [Parameter(Mandatory = $true)]
        [string]$RunId,
        [Parameter(Mandatory = $true)]
        [int]$AssignedDelaySeconds
    )

    $triggerInfo = Get-NodeTriggerCommandInfo -Config $Config -RunId $RunId -AssignedDelaySeconds $AssignedDelaySeconds
    $sshArgs = New-SshArgs -Config $Config -NodeIp $Node.IP
    $output = & $Config.SshBinary @sshArgs $triggerInfo.TriggerCommand 2>&1
    $exitCode = $LASTEXITCODE

    if ($exitCode -ne 0) {
        return [pscustomobject]@{
            Reachable            = $false
            Triggered            = $false
            RemoteResultFile     = $triggerInfo.RemoteResultFile
            RemoteErrorFile      = $triggerInfo.RemoteErrorFile
            AssignedDelaySeconds = $triggerInfo.AssignedDelaySeconds
            Error                = ($output -join ' ')
        }
    }

    return [pscustomobject]@{
        Reachable            = $true
        Triggered            = $true
        RemoteResultFile     = $triggerInfo.RemoteResultFile
        RemoteErrorFile      = $triggerInfo.RemoteErrorFile
        AssignedDelaySeconds = $triggerInfo.AssignedDelaySeconds
        Error                = ''
    }
}

function Invoke-NodeTriggerBatch {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Config,
        [Parameter(Mandatory = $true)]
        [pscustomobject[]]$Nodes,
        [Parameter(Mandatory = $true)]
        [string]$RunId
    )

    if ($Nodes.Count -eq 0) {
        return @()
    }

    $triggerAssignments = @(
        Get-NodeTriggerAssignments -Config $Config -RunId $RunId -Nodes $Nodes |
            ForEach-Object {
                [pscustomobject]@{
                    Index                = $_.Index
                    Node                 = $_.Node
                    AssignedDelaySeconds = $_.AssignedDelaySeconds
                    TriggerInfo          = (Get-NodeTriggerCommandInfo -Config $Config -RunId $RunId -AssignedDelaySeconds $_.AssignedDelaySeconds)
                }
            } |
            Sort-Object @{ Expression = { $_.AssignedDelaySeconds } }, @{ Expression = { $_.Index } }
    )

    $parallelism = [Math]::Max(1, [int]$Config.TriggerParallelism)
    if ($parallelism -le 1 -or $triggerAssignments.Count -le 1) {
        foreach ($item in $triggerAssignments) {
            $triggerResult = Invoke-NodeTriggerCommand -Config $Config -Node $item.Node -RunId $RunId -AssignedDelaySeconds $item.AssignedDelaySeconds
            [pscustomobject]@{
                Index         = $item.Index
                Node          = $item.Node
                TriggerResult = $triggerResult
            }
        }
        return
    }

    $sshHostKeyArgs = Get-SshHostKeyArgs
    $throttle = [Math]::Min($parallelism, $triggerAssignments.Count)

    $triggerAssignments |
        ForEach-Object -Parallel {
            $item = $_
            $node = $item.Node
            $config = $using:Config
            $triggerInfo = $item.TriggerInfo

            $sshHostKeyArgs = $using:sshHostKeyArgs
            $sshArgs = @(
                '-i', $config.SshKeyPath,
                '-o', 'BatchMode=yes',
                '-o', "ConnectTimeout=$($config.SshConnectTimeoutSeconds)"
            ) + $sshHostKeyArgs + @(
                "$($config.SshUser)@$($node.IP)"
            )

            $output = & $config.SshBinary @sshArgs $triggerInfo.TriggerCommand 2>&1
            $exitCode = $LASTEXITCODE
            if ($null -eq $output) {
                $output = @()
            }

            $triggerResult = if ($exitCode -eq 0) {
                [pscustomobject]@{
                    Reachable            = $true
                    Triggered            = $true
                    RemoteResultFile     = $triggerInfo.RemoteResultFile
                    RemoteErrorFile      = $triggerInfo.RemoteErrorFile
                    AssignedDelaySeconds = $triggerInfo.AssignedDelaySeconds
                    Error                = ''
                }
            }
            else {
                [pscustomobject]@{
                    Reachable            = $false
                    Triggered            = $false
                    RemoteResultFile     = $triggerInfo.RemoteResultFile
                    RemoteErrorFile      = $triggerInfo.RemoteErrorFile
                    AssignedDelaySeconds = $triggerInfo.AssignedDelaySeconds
                    Error                = ($output -join ' ')
                }
            }

            [pscustomobject]@{
                Index         = $item.Index
                Node          = $node
                TriggerResult = $triggerResult
            }
        } -ThrottleLimit $throttle

    return
}


function Get-CollectStreamMarkers {
    [CmdletBinding()]
    param()

    return [pscustomobject]@{
        BeginPrefix = '__FFMH_FILE_BEGIN__'
        EndPrefix   = '__FFMH_FILE_END__'
    }
}

function Convert-CollectStreamToFiles {
    [CmdletBinding()]
    param(
        [AllowEmptyString()]
        [string]$RawOutput,
        [string]$BeginPrefix = '__FFMH_FILE_BEGIN__',
        [string]$EndPrefix = '__FFMH_FILE_END__'
    )

    if ([string]::IsNullOrWhiteSpace($RawOutput)) {
        return @()
    }

    $records = New-Object System.Collections.Generic.List[object]
    $currentPath = ''
    $currentLines = New-Object System.Collections.Generic.List[string]

    foreach ($line in ($RawOutput -split '\r?\n')) {
        if ($line.StartsWith($BeginPrefix, [System.StringComparison]::Ordinal)) {
            $currentPath = $line.Substring($BeginPrefix.Length)
            $currentLines = New-Object System.Collections.Generic.List[string]
            continue
        }

        if ($line.StartsWith($EndPrefix, [System.StringComparison]::Ordinal)) {
            $endPath = $line.Substring($EndPrefix.Length)
            if ($currentPath -and ($currentPath -eq $endPath)) {
                $records.Add([pscustomobject]@{
                    RemotePath = $currentPath
                    RawOutput  = ($currentLines -join "`n")
                })
            }

            $currentPath = ''
            $currentLines = New-Object System.Collections.Generic.List[string]
            continue
        }

        if ($currentPath) {
            $currentLines.Add($line)
        }
    }

    return @($records.ToArray())
}

function Receive-NodeResults {
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

    $sshArgs = New-SshArgs -Config $Config -NodeIp $Node.IP
    $remoteRunDir = Get-RemoteRunResultDir -Config $Config -RunId $RunId
    $remoteDirEscaped = Convert-ToShellSingleQuoted -Value $remoteRunDir
    $markers = Get-CollectStreamMarkers
    $beginPrefixEscaped = Convert-ToShellSingleQuoted -Value $markers.BeginPrefix
    $endPrefixEscaped = Convert-ToShellSingleQuoted -Value $markers.EndPrefix
    $streamCmd = @"
find '$remoteDirEscaped' -maxdepth 1 -type f -name '*.txt' -print | sort | while IFS= read -r file; do printf '$beginPrefixEscaped%s\n' "`$file"; cat "`$file"; printf '\n$endPrefixEscaped%s\n' "`$file"; done
"@

    $streamOutput = & $Config.SshBinary @sshArgs $streamCmd 2>&1
    $streamExit = $LASTEXITCODE

    if ($streamExit -ne 0) {
        return [pscustomobject]@{
            Success         = $false
            LocalResultPath = ''
            LocalErrorPath  = ''
            RawOutput       = ''
            ErrorOutput     = ($streamOutput -join ' ')
            Files           = @()
            DiagnosticFiles = @()
            PendingFiles    = @()
        }
    }

    $streamText = if ($null -eq $streamOutput) { '' } else { $streamOutput -join "`n" }
    $remoteFiles = @(Convert-CollectStreamToFiles -RawOutput $streamText -BeginPrefix $markers.BeginPrefix -EndPrefix $markers.EndPrefix)

    if ($remoteFiles.Count -eq 0) {
        return [pscustomobject]@{
            Success         = $true
            LocalResultPath = ''
            LocalErrorPath  = ''
            RawOutput       = ''
            ErrorOutput     = ''
            Files           = @()
            DiagnosticFiles = @()
            PendingFiles    = @()
        }
    }

    $safeIp = Get-SafeFileNamePart -Value $Node.IP
    $downloadedMeasurements = @()
    $downloadedDiagnostics = @()
    $downloadedArtifacts = @()
    $pendingFiles = @()
    $downloadErrors = @()

    foreach ($remoteFile in $remoteFiles) {
        $remotePath = Convert-ToTrimmedString -Value $remoteFile.RemotePath
        if ([string]::IsNullOrWhiteSpace($remotePath)) {
            continue
        }

        $trimmedRawOutput = Convert-ToTrimmedString -Value $remoteFile.RawOutput
        $remoteLeaf = Split-Path -Path $remotePath -Leaf
        $safeLeaf = Get-SafeFileNamePart -Value $remoteLeaf
        $localPath = Join-Path $RawDir ("{0}_{1}_{2}" -f $Node.DeviceID, $safeIp, $safeLeaf)

        Set-Content -Path $localPath -Value $trimmedRawOutput

        $parsedMeasurement = ConvertFrom-MeasurementOutput -RawOutput $trimmedRawOutput
        if ($null -ne $parsedMeasurement) {
            $measurementFile = [pscustomobject]@{
                RemotePath        = $remotePath
                LocalPath         = $localPath
                RawOutput         = $trimmedRawOutput
                ParsedMeasurement = $parsedMeasurement
            }
            $downloadedMeasurements += $measurementFile
            $downloadedArtifacts += $measurementFile
            continue
        }

        $parsedDiagnostic = ConvertFrom-NodeDiagnosticOutput -RawOutput $trimmedRawOutput
        if ($null -ne $parsedDiagnostic) {
            $diagnosticFile = [pscustomobject]@{
                RemotePath       = $remotePath
                LocalPath        = $localPath
                RawOutput        = $trimmedRawOutput
                ParsedDiagnostic = $parsedDiagnostic
            }
            $downloadedDiagnostics += $diagnosticFile
            $downloadedArtifacts += $diagnosticFile
            continue
        }

        $pendingFiles += [pscustomobject]@{
            RemotePath = $remotePath
            LocalPath  = $localPath
            RawOutput  = $trimmedRawOutput
            RawSize    = $trimmedRawOutput.Length
        }
    }

    if ($downloadedArtifacts.Count -gt 0) {
        $deleteTargets = @($downloadedArtifacts | ForEach-Object { "'$(Convert-ToShellSingleQuoted -Value $_.RemotePath)'" })
        $deleteCmd = 'rm -f ' + ($deleteTargets -join ' ')
        $deleteOutput = & $Config.SshBinary @sshArgs $deleteCmd 2>&1
        $deleteExit = $LASTEXITCODE
        if ($deleteExit -ne 0) {
            $downloadErrors += ('delete failed for ' + (@($downloadedArtifacts | ForEach-Object { $_.RemotePath }) -join ', ') + ': ' + ($deleteOutput -join ' '))
        }
    }

    if ($downloadedArtifacts.Count -gt 0 -and $pendingFiles.Count -eq 0) {
        $cleanupCmd = "rmdir '$remoteDirEscaped' >/dev/null 2>&1 || true"
        $cleanupOutput = & $Config.SshBinary @sshArgs $cleanupCmd 2>&1
        if ($LASTEXITCODE -ne 0) {
            $downloadErrors += ('remote run dir cleanup failed for ' + $remoteRunDir + ': ' + ($cleanupOutput -join ' '))
        }
    }

    if ($downloadedMeasurements.Count -eq 0) {
        return [pscustomobject]@{
            Success         = $true
            LocalResultPath = ''
            LocalErrorPath  = ''
            RawOutput       = ''
            ErrorOutput     = (@($downloadErrors) -join ' | ')
            Files           = @()
            DiagnosticFiles = @($downloadedDiagnostics)
            PendingFiles    = @($pendingFiles)
        }
    }

    $firstFile = $downloadedMeasurements[0]

    return [pscustomobject]@{
        Success         = $true
        LocalResultPath = $firstFile.LocalPath
        LocalErrorPath  = ''
        RawOutput       = $firstFile.RawOutput
        ErrorOutput     = (@($downloadErrors) -join ' | ')
        Files           = @($downloadedMeasurements)
        DiagnosticFiles = @($downloadedDiagnostics)
        PendingFiles    = @($pendingFiles)
    }
}

function Invoke-NodeCollectBatch {

    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Config,
        [Parameter(Mandatory = $true)]
        [pscustomobject[]]$Nodes,
        [Parameter(Mandatory = $true)]
        [string]$RunId,
        [Parameter(Mandatory = $true)]
        [string]$RawDir
    )

    if ($Nodes.Count -eq 0) {
        return
    }

    $indexedNodes = for ($i = 0; $i -lt $Nodes.Count; $i++) {
        [pscustomobject]@{
            Index = $i
            Node  = $Nodes[$i]
        }
    }

    $parallelism = [Math]::Max(1, [int]$Config.CollectParallelism)
    if ($parallelism -le 1 -or $indexedNodes.Count -le 1) {
        foreach ($item in $indexedNodes) {
            [pscustomobject]@{
                Index         = $item.Index
                Node          = $item.Node
                CollectResult = (Receive-NodeResults -Config $Config -Node $item.Node -RunId $RunId -RawDir $RawDir)
            }
        }
        return
    }

    $throttle = [Math]::Min($parallelism, $indexedNodes.Count)
    $modulePath = $script:ModuleFilePath
    $batchConfig = $Config
    $batchRunId = $RunId
    $batchRawDir = $RawDir

    $indexedNodes |
        ForEach-Object -Parallel {
            $item = $_
            $config = $using:batchConfig
            $collectRunIdentifier = $using:batchRunId
            $rawDir = $using:batchRawDir
            $modulePath = $using:modulePath

            Import-Module $modulePath -Force | Out-Null

            try {
                $collectResult = Receive-NodeResults -Config $config -Node $item.Node -RunId $collectRunIdentifier -RawDir $rawDir
            }
            catch {
                $collectResult = [pscustomobject]@{
                    Success         = $false
                    LocalResultPath = ''
                    LocalErrorPath  = ''
                    RawOutput       = ''
                    ErrorOutput     = $_.Exception.Message
                    Files           = @()
                    DiagnosticFiles = @()
                    PendingFiles    = @()
                }
            }

            [pscustomobject]@{
                Index         = $item.Index
                Node          = $item.Node
                CollectResult = $collectResult
            }
        } -ThrottleLimit $throttle
}

function ConvertFrom-MeasurementOutput {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [AllowEmptyString()]
        [string]$RawOutput
    )

    if ([string]::IsNullOrWhiteSpace($RawOutput)) {
        return $null
    }

    $successRegex = '^speedtest,nodeid=(?<nodeid>[^ ]+)\s+download_mbit=(?<download>[0-9]+(?:\.[0-9]+)?),target="(?<target>[^"]+)"\s+(?<timestamp>[0-9]+)$'
    $failureRegex = '^(?<kind>wget_failed|speedtest_invalid|speedtest_size_mismatch),nodeid=(?<nodeid>[^ ]+)\s+.*target="(?<target>[^"]+)"\s+(?<timestamp>[0-9]+)$'

    $lines = @(
        $RawOutput -split '\r?\n' |
            ForEach-Object { Convert-ToTrimmedString -Value $_ } |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )

    foreach ($line in $lines) {
        $successMatch = [regex]::Match($line, $successRegex)
        if ($successMatch.Success) {
            return [pscustomobject]@{
                NodeId         = $successMatch.Groups['nodeid'].Value
                ThroughputMbit = [double]::Parse($successMatch.Groups['download'].Value, [System.Globalization.CultureInfo]::InvariantCulture)
                Target         = $successMatch.Groups['target'].Value
                TimestampNs    = $successMatch.Groups['timestamp'].Value
                ResultType     = 'success'
                FailureReason  = ''
            }
        }

        $failureMatch = [regex]::Match($line, $failureRegex)
        if ($failureMatch.Success) {
            return [pscustomobject]@{
                NodeId         = $failureMatch.Groups['nodeid'].Value
                ThroughputMbit = 0.0
                Target         = $failureMatch.Groups['target'].Value
                TimestampNs    = $failureMatch.Groups['timestamp'].Value
                ResultType     = 'final_failed'
                FailureReason  = $failureMatch.Groups['kind'].Value
            }
        }
    }

    return $null
}

function ConvertFrom-NodeDiagnosticOutput {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [AllowEmptyString()]
        [string]$RawOutput
    )

    if ([string]::IsNullOrWhiteSpace($RawOutput)) {
        return $null
    }

    $headerRegex = '^diagnostic,nodeid=(?<nodeid>[^ ]+) target_host="(?<target>[^"]*)" speedtest_delay_seconds=(?<speedtest_delay>-?[0-9]+) diagnostic_delay_seconds=(?<diag_delay>-?[0-9]+) timestamp=(?<timestamp>[0-9]+)$'
    $summaryRegex = '^diag_summary,load1=(?<load1>-?[0-9]+(?:\.[0-9]+)?) load5=(?<load5>-?[0-9]+(?:\.[0-9]+)?) load15=(?<load15>-?[0-9]+(?:\.[0-9]+)?) gateway_probe="(?<gateway>[^"]*)" gateway_probe_kind="(?<gateway_kind>[^"]*)" ping_gateway_loss=(?<gateway_loss>-?[0-9]+(?:\.[0-9]+)?) ping_target_loss=(?<target_loss>-?[0-9]+(?:\.[0-9]+)?)$'

    $lines = @(
        $RawOutput -split '\r?\n' |
            ForEach-Object { Convert-ToTrimmedString -Value $_ } |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )

    $headerMatch = $null
    $summaryMatch = $null
    foreach ($line in $lines) {
        if ($null -eq $headerMatch) {
            $candidate = [regex]::Match($line, $headerRegex)
            if ($candidate.Success) {
                $headerMatch = $candidate
                continue
            }
        }

        if ($null -eq $summaryMatch) {
            $candidate = [regex]::Match($line, $summaryRegex)
            if ($candidate.Success) {
                $summaryMatch = $candidate
            }
        }
    }

    if ($null -eq $headerMatch -or $null -eq $summaryMatch) {
        return $null
    }

    return [pscustomobject]@{
        NodeId                 = $headerMatch.Groups['nodeid'].Value
        TargetHost             = $headerMatch.Groups['target'].Value
        SpeedtestDelaySeconds  = [int]$headerMatch.Groups['speedtest_delay'].Value
        DiagnosticDelaySeconds = [int]$headerMatch.Groups['diag_delay'].Value
        TimestampNs            = $headerMatch.Groups['timestamp'].Value
        GatewayProbe           = $summaryMatch.Groups['gateway'].Value
        GatewayProbeKind       = $summaryMatch.Groups['gateway_kind'].Value
        PingGatewayLossPct     = [double]::Parse($summaryMatch.Groups['gateway_loss'].Value, [System.Globalization.CultureInfo]::InvariantCulture)
        PingTargetLossPct      = [double]::Parse($summaryMatch.Groups['target_loss'].Value, [System.Globalization.CultureInfo]::InvariantCulture)
        Load1                  = [double]::Parse($summaryMatch.Groups['load1'].Value, [System.Globalization.CultureInfo]::InvariantCulture)
        Load5                  = [double]::Parse($summaryMatch.Groups['load5'].Value, [System.Globalization.CultureInfo]::InvariantCulture)
        Load15                 = [double]::Parse($summaryMatch.Groups['load15'].Value, [System.Globalization.CultureInfo]::InvariantCulture)
    }
}
