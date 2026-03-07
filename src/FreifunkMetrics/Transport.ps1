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

function Test-NodeResultFinished {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Config,
        [Parameter(Mandatory = $true)]
        [object]$Node
    )

    $sshArgs = New-SshArgs -Config $Config -NodeIp $Node.IP
    $remoteDirEscaped = Convert-ToShellSingleQuoted -Value $Config.RemoteResultDir
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
        return @($indexedNodes | Where-Object { Test-NodeResultFinished -Config $Config -Node $_.Node }).Count
    }

    $batchConfig = $Config
    $modulePath = $script:ModuleFilePath
    $ready = @(
        $indexedNodes |
            ForEach-Object -Parallel {
                $item = $_
                $config = $using:batchConfig
                $modulePath = $using:modulePath
                Import-Module $modulePath -Force | Out-Null
                if (Test-NodeResultFinished -Config $config -Node $item.Node) {
                    $item.Index
                }
            } -ThrottleLimit $parallelism
    )

    return @($ready).Count
}

function Get-NodeTriggerCommandInfo {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Config
    )

    $remoteResultPattern = "$($Config.RemoteResultDir)/*.txt"

    $delayMaxSeconds = [Math]::Max(0, [int]$Config.TriggerRandomDelayMaxSeconds)
    $delayUpperBound = $delayMaxSeconds + 1
    $targetUrl = Convert-ToTrimmedString -Value $Config.SpeedtestTargetUrl
    $targetUrlShell = Convert-ToShellSingleQuoted -Value $targetUrl
    $targetBytes = [Math]::Max(1, [int64]$Config.SpeedtestTargetBytes)

    $payload = @"
start=`$(date +%s%N)
nodeid=`$(tr -d ':' </lib/gluon/core/sysconfig/primary_mac)
target_url='$targetUrlShell'
delay=`$(awk 'BEGIN{srand(); print int(rand()*$delayUpperBound)}')
sleep "`$delay"
wget_exit_file="/tmp/harvester-wget-exit-`$$.txt"
rm -f "`$wget_exit_file"
t0=`$(date +%s.%N)
bytes=`$({ wget -O - -q "`$target_url"; printf '%s' "`$?" > "`$wget_exit_file"; } | wc -c)
t1=`$(date +%s.%N)
wget_exit=`$(cat "`$wget_exit_file" 2>/dev/null)
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
    $remoteDirEscaped = Convert-ToShellSingleQuoted -Value $Config.RemoteResultDir
    $triggerCmd = "mkdir -p '$remoteDirEscaped'; ts=`$(date +%s%N); out='$remoteDirEscaped/'`$ts.txt; ( $payload ) > `"`$out`" 2>&1 &"

    return [pscustomobject]@{
        RemoteResultFile = $remoteResultPattern
        RemoteErrorFile  = ''
        TriggerCommand   = $triggerCmd
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
        [string]$RunId
    )

    $triggerInfo = Get-NodeTriggerCommandInfo -Config $Config
    $sshArgs = New-SshArgs -Config $Config -NodeIp $Node.IP
    $output = & $Config.SshBinary @sshArgs $triggerInfo.TriggerCommand 2>&1
    $exitCode = $LASTEXITCODE

    if ($exitCode -ne 0) {
        return [pscustomobject]@{
            Reachable        = $false
            Triggered        = $false
            RemoteResultFile = $triggerInfo.RemoteResultFile
            RemoteErrorFile  = $triggerInfo.RemoteErrorFile
            Error            = ($output -join ' ')
        }
    }

    return [pscustomobject]@{
        Reachable        = $true
        Triggered        = $true
        RemoteResultFile = $triggerInfo.RemoteResultFile
        RemoteErrorFile  = $triggerInfo.RemoteErrorFile
        Error            = ''
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

    $indexedNodes = for ($i = 0; $i -lt $Nodes.Count; $i++) {
        [pscustomobject]@{
            Index = $i
            Node  = $Nodes[$i]
        }
    }
    $parallelism = [Math]::Max(1, [int]$Config.TriggerParallelism)
    if ($parallelism -le 1 -or $indexedNodes.Count -le 1) {
        foreach ($item in $indexedNodes) {
            $triggerResult = Invoke-NodeTriggerCommand -Config $Config -Node $item.Node -RunId $RunId
            [pscustomobject]@{
                Index         = $item.Index
                Node          = $item.Node
                TriggerResult = $triggerResult
            }
        }
        return
    }

    $triggerInfo = Get-NodeTriggerCommandInfo -Config $Config
    $sshHostKeyArgs = Get-SshHostKeyArgs
    $throttle = [Math]::Min($parallelism, $indexedNodes.Count)

    $indexedNodes |
        ForEach-Object -Parallel {
            $item = $_
            $node = $item.Node
            $config = $using:Config
            $triggerInfo = $using:triggerInfo

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
                    Reachable        = $true
                    Triggered        = $true
                    RemoteResultFile = $triggerInfo.RemoteResultFile
                    RemoteErrorFile  = $triggerInfo.RemoteErrorFile
                    Error            = ''
                }
            }
            else {
                [pscustomobject]@{
                    Reachable        = $false
                    Triggered        = $false
                    RemoteResultFile = $triggerInfo.RemoteResultFile
                    RemoteErrorFile  = $triggerInfo.RemoteErrorFile
                    Error            = ($output -join ' ')
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
    $remoteDirEscaped = Convert-ToShellSingleQuoted -Value $Config.RemoteResultDir
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
            PendingFiles    = @()
        }
    }

    $safeIp = Get-SafeFileNamePart -Value $Node.IP
    $downloaded = @()
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
        if ($null -eq $parsedMeasurement) {
            $pendingFiles += [pscustomobject]@{
                RemotePath = $remotePath
                LocalPath  = $localPath
                RawOutput  = $trimmedRawOutput
                RawSize    = $trimmedRawOutput.Length
            }
            continue
        }

        $downloaded += [pscustomobject]@{
            RemotePath        = $remotePath
            LocalPath         = $localPath
            RawOutput         = $trimmedRawOutput
            ParsedMeasurement = $parsedMeasurement
        }
    }

    if ($downloaded.Count -gt 0) {
        $deleteTargets = @($downloaded | ForEach-Object { "'$(Convert-ToShellSingleQuoted -Value $_.RemotePath)'" })
        $deleteCmd = 'rm -f ' + ($deleteTargets -join ' ')
        $deleteOutput = & $Config.SshBinary @sshArgs $deleteCmd 2>&1
        $deleteExit = $LASTEXITCODE
        if ($deleteExit -ne 0) {
            $downloadErrors += ('delete failed for ' + (@($downloaded | ForEach-Object { $_.RemotePath }) -join ', ') + ': ' + ($deleteOutput -join ' '))
        }
    }

    if ($downloaded.Count -eq 0) {
        return [pscustomobject]@{
            Success         = $true
            LocalResultPath = ''
            LocalErrorPath  = ''
            RawOutput       = ''
            ErrorOutput     = (@($downloadErrors) -join ' | ')
            Files           = @()
            PendingFiles    = @($pendingFiles)
        }
    }

    $firstFile = $downloaded[0]

    return [pscustomobject]@{
        Success         = $true
        LocalResultPath = $firstFile.LocalPath
        LocalErrorPath  = ''
        RawOutput       = $firstFile.RawOutput
        ErrorOutput     = (@($downloadErrors) -join ' | ')
        Files           = @($downloaded)
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



