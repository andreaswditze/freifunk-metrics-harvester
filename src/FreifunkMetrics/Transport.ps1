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

function Invoke-SshShellScript {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Config,
        [Parameter(Mandatory = $true)]
        [string]$NodeIp,
        [Parameter(Mandatory = $true)]
        [string]$ScriptContent
    )

    $sshArgs = New-SshArgs -Config $Config -NodeIp $NodeIp
    $scriptText = [string]$ScriptContent
    if (-not $scriptText.EndsWith("`n")) {
        $scriptText += "`n"
    }

    # Stream large trigger payloads over stdin so the SSH exec request stays small.
    $output = $scriptText | & $Config.SshBinary @sshArgs 'sh -s' 2>&1
    $exitCode = $LASTEXITCODE
    if ($null -eq $exitCode) {
        $exitCode = if ($?) { 0 } else { 1 }
    }

    return [pscustomobject]@{
        Output   = if ($null -eq $output) { @() } else { @($output) }
        ExitCode = [int]$exitCode
    }
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
    if grep -Eq '^(speedtest,nodeid=|wget_failed,nodeid=|speedtest_invalid,nodeid=|speedtest_size_mismatch,nodeid=|speedtest_timeout,nodeid=)' "`$file"; then
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


function Get-NodeTriggerSchedulingMode {
    [CmdletBinding()]
    param(
        [datetime]$Timestamp = (Get-Date)
    )

    $weekdayNumber = [int]$Timestamp.DayOfWeek
    if ($weekdayNumber -eq 0) {
        $weekdayNumber = 7
    }

    if (($weekdayNumber % 2) -eq 0) {
        return 'NodeId'
    }

    return 'Throughput'
}

function Get-NodeTriggerNodeIdSortKey {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object]$Node
    )

    foreach ($propertyName in @('NodeId', 'NodeID', 'DeviceID', 'Id', 'ID')) {
        if ($null -eq $Node.PSObject.Properties[$propertyName]) {
            continue
        }

        $value = Convert-ToTrimmedString -Value $Node.$propertyName
        if (-not [string]::IsNullOrWhiteSpace($value)) {
            return $value
        }
    }

    return ''
}


function Get-NodeDiagnosticsSettings {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Config
    )

    $enabled = if ($Config.ContainsKey('EnableNodeDiagnostics')) { [bool]$Config.EnableNodeDiagnostics } else { $true }
    $delaySeconds = if ($Config.ContainsKey('NodeDiagnosticsDelaySeconds')) { [Math]::Max(0, [int]$Config.NodeDiagnosticsDelaySeconds) } else { 60 }

    return [pscustomobject]@{
        Enabled           = $enabled
        DelaySeconds      = $delaySeconds
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

    $schedulingMode = Get-NodeTriggerSchedulingMode
    $sortProperties = if ($schedulingMode -eq 'NodeId') {
        @(
            @{ Expression = { Get-NodeTriggerNodeIdSortKey -Node $_.Node } },
            @{ Expression = { $_.Index } }
        )
    }
    else {
        @(
            @{ Expression = { $_.LatestThroughput } },
            @{ Expression = { Get-NodeTriggerAssignmentOrderKey -RunId $RunId -Node $_.Node } },
            @{ Expression = { $_.Index } }
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
            Sort-Object $sortProperties
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

    return [pscustomobject]@{
        Enabled           = $enabled
        DelaySeconds      = $delaySeconds
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
    $invocation = Invoke-SshShellScript -Config $Config -NodeIp $Node.IP -ScriptContent $triggerInfo.TriggerCommand
    $output = $invocation.Output
    $exitCode = $invocation.ExitCode

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

    $modulePath = $script:ModuleFilePath
    $throttle = [Math]::Min($parallelism, $triggerAssignments.Count)

    $triggerAssignments |
        ForEach-Object -Parallel {
            $item = $_
            $node = $item.Node
            $config = $using:Config
            $triggerInfo = $item.TriggerInfo
            $modulePath = $using:modulePath

            Import-Module $modulePath -Force | Out-Null

            $invocation = Invoke-SshShellScript -Config $config -NodeIp $node.IP -ScriptContent $triggerInfo.TriggerCommand
            $output = $invocation.Output
            $exitCode = $invocation.ExitCode

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

    $successRegex = '^speedtest,nodeid=(?<nodeid>[^ ]+)\s+download_mbit=(?<download>[0-9]+(?:\.[0-9]+)?)\s+bytes=(?<bytes>[0-9]+)\s+sec=(?<sec>[0-9]+(?:\.[0-9]+)?)\s+timeout_seconds=(?<timeout>[0-9]+),target="(?<target>[^"]+)"\s+(?<timestamp>[0-9]+)$'
    $failureRegex = '^(?<kind>wget_failed|speedtest_invalid|speedtest_size_mismatch|speedtest_timeout),nodeid=(?<nodeid>[^ ]+)\s+(?:exit=(?<exit>-?[0-9]+)\s+)?bytes=(?<bytes>[0-9]+)\s+sec=(?<sec>[0-9]+(?:\.[0-9]+)?)\s+expected_bytes=(?<expected>[0-9]+)\s+timeout_seconds=(?<timeout>[0-9]+)\s+target="(?<target>[^"]+)"\s+(?<timestamp>[0-9]+)$'

    $lines = @(
        $RawOutput -split '\r?\n' |
            ForEach-Object { Convert-ToTrimmedString -Value $_ } |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )

    foreach ($line in $lines) {
        $successMatch = [regex]::Match($line, $successRegex)
        if ($successMatch.Success) {
            return [pscustomobject]@{
                NodeId                  = $successMatch.Groups['nodeid'].Value
                ThroughputMbit          = [double]::Parse($successMatch.Groups['download'].Value, [System.Globalization.CultureInfo]::InvariantCulture)
                Target                  = $successMatch.Groups['target'].Value
                TimestampNs             = $successMatch.Groups['timestamp'].Value
                ResultType              = 'success'
                FailureReason           = ''
                DownloadedBytes         = [int64]$successMatch.Groups['bytes'].Value
                ExpectedBytes           = [int64]$successMatch.Groups['bytes'].Value
                DownloadDurationSeconds = [double]::Parse($successMatch.Groups['sec'].Value, [System.Globalization.CultureInfo]::InvariantCulture)
                TimeoutSeconds          = [int]$successMatch.Groups['timeout'].Value
                WgetExitCode            = 0
            }
        }

        $failureMatch = [regex]::Match($line, $failureRegex)
        if ($failureMatch.Success) {
            $wgetExitCode = $null
            if ($failureMatch.Groups['exit'].Success -and -not [string]::IsNullOrWhiteSpace($failureMatch.Groups['exit'].Value)) {
                $wgetExitCode = [int]$failureMatch.Groups['exit'].Value
            }

            return [pscustomobject]@{
                NodeId                  = $failureMatch.Groups['nodeid'].Value
                ThroughputMbit          = 0.0
                Target                  = $failureMatch.Groups['target'].Value
                TimestampNs             = $failureMatch.Groups['timestamp'].Value
                ResultType              = 'final_failed'
                FailureReason           = $failureMatch.Groups['kind'].Value
                DownloadedBytes         = [int64]$failureMatch.Groups['bytes'].Value
                ExpectedBytes           = [int64]$failureMatch.Groups['expected'].Value
                DownloadDurationSeconds = [double]::Parse($failureMatch.Groups['sec'].Value, [System.Globalization.CultureInfo]::InvariantCulture)
                TimeoutSeconds          = [int]$failureMatch.Groups['timeout'].Value
                WgetExitCode            = $wgetExitCode
            }
        }
    }

    return $null
}

function Get-NodeDiagnosticSections {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [AllowEmptyCollection()]
        [AllowEmptyString()]
        [string[]]$Lines
    )

    $sections = @{}
    $currentName = ''
    $buffer = New-Object System.Collections.Generic.List[string]

    foreach ($line in $Lines) {
        $trimmed = Convert-ToTrimmedString -Value $line
        if ($trimmed -match '^diag_section,name=(?<name>.+)$') {
            $currentName = $Matches['name']
            $buffer = New-Object System.Collections.Generic.List[string]
            continue
        }

        if ($trimmed -match '^diag_section_end,name=(?<name>.+)$') {
            if (-not [string]::IsNullOrWhiteSpace($currentName) -and $currentName -eq $Matches['name']) {
                $sections[$currentName] = (@($buffer) -join "`n")
            }

            $currentName = ''
            $buffer = New-Object System.Collections.Generic.List[string]
            continue
        }

        if (-not [string]::IsNullOrWhiteSpace($currentName)) {
            $buffer.Add($trimmed)
        }
    }

    return $sections
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

    $lines = @(
        $RawOutput -split '\r?\n' |
            ForEach-Object { Convert-ToTrimmedString -Value $_ } |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )

    $headerMatch = $null
    $summaryValues = @{}
    foreach ($line in $lines) {
        if ($null -eq $headerMatch) {
            $candidate = [regex]::Match($line, $headerRegex)
            if ($candidate.Success) {
                $headerMatch = $candidate
                continue
            }
        }

        if ($line.StartsWith('diag_summary,')) {
            foreach ($pair in [regex]::Matches($line.Substring(13), '(?<key>[a-z0-9_]+)=(?:"(?<quoted>[^"]*)"|(?<bare>[^ ]+))')) {
                if ($pair.Groups['quoted'].Success) {
                    $value = $pair.Groups['quoted'].Value
                }
                else {
                    $value = $pair.Groups['bare'].Value
                }

                $summaryValues[$pair.Groups['key'].Value] = $value
            }
        }
    }

    if ($null -eq $headerMatch -or $summaryValues.Count -eq 0) {
        return $null
    }

    $sections = Get-NodeDiagnosticSections -Lines $lines

    return [pscustomobject]@{
        NodeId                 = $headerMatch.Groups['nodeid'].Value
        TargetHost             = $headerMatch.Groups['target'].Value
        SpeedtestDelaySeconds  = [int]$headerMatch.Groups['speedtest_delay'].Value
        DiagnosticDelaySeconds = [int]$headerMatch.Groups['diag_delay'].Value
        TimestampNs            = $headerMatch.Groups['timestamp'].Value
        GatewayProbe           = if ($summaryValues.ContainsKey('gateway_probe')) { $summaryValues['gateway_probe'] } else { '' }
        GatewayProbeKind       = if ($summaryValues.ContainsKey('gateway_probe_kind')) { $summaryValues['gateway_probe_kind'] } else { '' }
        PingGatewayLossPct     = if ($summaryValues.ContainsKey('ping_gateway_loss')) { [double]::Parse($summaryValues['ping_gateway_loss'], [System.Globalization.CultureInfo]::InvariantCulture) } else { -1 }
        PingTargetLossPct      = if ($summaryValues.ContainsKey('ping_target_loss')) { [double]::Parse($summaryValues['ping_target_loss'], [System.Globalization.CultureInfo]::InvariantCulture) } else { -1 }
        Load1                  = if ($summaryValues.ContainsKey('load1')) { [double]::Parse($summaryValues['load1'], [System.Globalization.CultureInfo]::InvariantCulture) } else { 0 }
        Load5                  = if ($summaryValues.ContainsKey('load5')) { [double]::Parse($summaryValues['load5'], [System.Globalization.CultureInfo]::InvariantCulture) } else { 0 }
        Load15                 = if ($summaryValues.ContainsKey('load15')) { [double]::Parse($summaryValues['load15'], [System.Globalization.CultureInfo]::InvariantCulture) } else { 0 }
        TargetIPv4             = if ($summaryValues.ContainsKey('target_ipv4')) { $summaryValues['target_ipv4'] } else { '' }
        TargetIPv6             = if ($summaryValues.ContainsKey('target_ipv6')) { $summaryValues['target_ipv6'] } else { '' }
        RouteGetIPv4           = if ($summaryValues.ContainsKey('route_get_ipv4')) { $summaryValues['route_get_ipv4'] } else { '' }
        RouteGetIPv6           = if ($summaryValues.ContainsKey('route_get_ipv6')) { $summaryValues['route_get_ipv6'] } else { '' }
        WgetStderr             = if ($summaryValues.ContainsKey('wget_stderr')) { $summaryValues['wget_stderr'] } else { '' }
        TcpGatewayProbePort    = if ($summaryValues.ContainsKey('tcp_gateway_probe_port')) { [int]$summaryValues['tcp_gateway_probe_port'] } else { 0 }
        TcpGatewayProbeResult  = if ($summaryValues.ContainsKey('tcp_gateway_probe_result')) { $summaryValues['tcp_gateway_probe_result'] } else { '' }
        TcpTargetProbePort     = if ($summaryValues.ContainsKey('tcp_target_probe_port')) { [int]$summaryValues['tcp_target_probe_port'] } else { 0 }
        TcpTargetProbeResult   = if ($summaryValues.ContainsKey('tcp_target_probe_result')) { $summaryValues['tcp_target_probe_result'] } else { '' }
        TargetResolution       = if ($sections.ContainsKey('target_resolution')) { $sections['target_resolution'] } else { '' }
        RouteGet               = if ($sections.ContainsKey('route_get')) { $sections['route_get'] } else { '' }
        UbusNetworkDump        = if ($sections.ContainsKey('ubus_network_dump')) { $sections['ubus_network_dump'] } else { '' }
        UbusIfstatusWan        = if ($sections.ContainsKey('ubus_ifstatus_wan')) { $sections['ubus_ifstatus_wan'] } else { '' }
        UbusIfstatusWan6       = if ($sections.ContainsKey('ubus_ifstatus_wan6')) { $sections['ubus_ifstatus_wan6'] } else { '' }
    }
}

function Get-MeasurementSections {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [AllowEmptyCollection()]
        [AllowEmptyString()]
        [string[]]$Lines
    )

    $sections = @{}
    $currentName = ''
    $buffer = New-Object System.Collections.Generic.List[string]

    foreach ($line in $Lines) {
        $rawLine = [string]$line
        $trimmed = Convert-ToTrimmedString -Value $rawLine
        if ($trimmed -match '^measurement_section,name=(?<name>.+)$') {
            $currentName = $Matches['name']
            $buffer = New-Object System.Collections.Generic.List[string]
            continue
        }

        if ($trimmed -match '^measurement_section_end,name=(?<name>.+)$') {
            if (-not [string]::IsNullOrWhiteSpace($currentName) -and $currentName -eq $Matches['name']) {
                $sections[$currentName] = (@($buffer) -join "`n").TrimEnd()
            }

            $currentName = ''
            $buffer = New-Object System.Collections.Generic.List[string]
            continue
        }

        if (-not [string]::IsNullOrWhiteSpace($currentName)) {
            $buffer.Add($rawLine)
        }
    }

    return $sections
}

function Get-WgetExitReason {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [Nullable[int]]$ExitCode,
        [string]$Fallback = ''
    )

    if ($null -eq $ExitCode) {
        return (Convert-ToTrimmedString -Value $Fallback)
    }

    switch ([int]$ExitCode) {
        0 { return 'success' }
        1 { return 'generic_error' }
        2 { return 'parse_error' }
        3 { return 'file_io_error' }
        4 { return 'network_failure' }
        5 { return 'ssl_verification_failure' }
        6 { return 'auth_failure' }
        7 { return 'protocol_error' }
        8 { return 'server_error' }
        124 { return 'watchdog_timeout' }
        default { return ('exit_' + [string]$ExitCode) }
    }
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

    $successRegex = '^speedtest,nodeid=(?<nodeid>[^ ]+)\s+download_mbit=(?<download>[0-9]+(?:\.[0-9]+)?)\s+bytes=(?<bytes>[0-9]+)\s+sec=(?<sec>[0-9]+(?:\.[0-9]+)?)\s+timeout_seconds=(?<timeout>[0-9]+),target="(?<target>[^"]+)"\s+(?<timestamp>[0-9]+)$'
    $failureRegex = '^(?<kind>wget_failed|speedtest_invalid|speedtest_size_mismatch|speedtest_timeout),nodeid=(?<nodeid>[^ ]+)\s+(?:exit=(?<exit>-?[0-9]+)\s+)?bytes=(?<bytes>[0-9]+)\s+sec=(?<sec>[0-9]+(?:\.[0-9]+)?)\s+expected_bytes=(?<expected>[0-9]+)\s+timeout_seconds=(?<timeout>[0-9]+)\s+target="(?<target>[^"]+)"\s+(?<timestamp>[0-9]+)$'

    $rawLines = @($RawOutput -split '\r?\n')
    $lines = @(
        $rawLines |
            ForEach-Object { Convert-ToTrimmedString -Value $_ } |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )

    $summaryValues = @{}
    foreach ($line in $lines) {
        if ($line.StartsWith('measurement_meta,')) {
            foreach ($pair in [regex]::Matches($line.Substring(17), '(?<key>[a-z0-9_]+)=(?:"(?<quoted>[^"]*)"|(?<bare>[^ ]+))')) {
                $value = if ($pair.Groups['quoted'].Success) { $pair.Groups['quoted'].Value } else { $pair.Groups['bare'].Value }
                $summaryValues[$pair.Groups['key'].Value] = $value
            }
        }
    }

    $sections = Get-MeasurementSections -Lines $rawLines
    $wgetStderr = if ($sections.ContainsKey('wget_stderr')) { $sections['wget_stderr'] } else { '' }

    foreach ($line in $lines) {
        $successMatch = [regex]::Match($line, $successRegex)
        if ($successMatch.Success) {
            $wgetExitCode = 0
            if ($summaryValues.ContainsKey('wget_exit_code') -and -not [string]::IsNullOrWhiteSpace($summaryValues['wget_exit_code'])) {
                $wgetExitCode = [int]$summaryValues['wget_exit_code']
            }

            $wgetExitReason = if ($summaryValues.ContainsKey('wget_exit_reason')) {
                $summaryValues['wget_exit_reason']
            }
            else {
                Get-WgetExitReason -ExitCode $wgetExitCode
            }

            return [pscustomobject]@{
                NodeId                  = $successMatch.Groups['nodeid'].Value
                ThroughputMbit          = [double]::Parse($successMatch.Groups['download'].Value, [System.Globalization.CultureInfo]::InvariantCulture)
                Target                  = $successMatch.Groups['target'].Value
                TimestampNs             = $successMatch.Groups['timestamp'].Value
                ResultType              = 'success'
                FailureReason           = ''
                DownloadedBytes         = [int64]$successMatch.Groups['bytes'].Value
                ExpectedBytes           = [int64]$successMatch.Groups['bytes'].Value
                DownloadDurationSeconds = [double]::Parse($successMatch.Groups['sec'].Value, [System.Globalization.CultureInfo]::InvariantCulture)
                TimeoutSeconds          = [int]$successMatch.Groups['timeout'].Value
                WgetExitCode            = $wgetExitCode
                WgetExitReason          = $wgetExitReason
                WgetStderr              = $wgetStderr
            }
        }

        $failureMatch = [regex]::Match($line, $failureRegex)
        if ($failureMatch.Success) {
            $wgetExitCode = $null
            if ($failureMatch.Groups['exit'].Success -and -not [string]::IsNullOrWhiteSpace($failureMatch.Groups['exit'].Value)) {
                $wgetExitCode = [int]$failureMatch.Groups['exit'].Value
            }
            elseif ($summaryValues.ContainsKey('wget_exit_code') -and -not [string]::IsNullOrWhiteSpace($summaryValues['wget_exit_code'])) {
                $wgetExitCode = [int]$summaryValues['wget_exit_code']
            }

            $wgetExitReason = if ($summaryValues.ContainsKey('wget_exit_reason')) {
                $summaryValues['wget_exit_reason']
            }
            else {
                Get-WgetExitReason -ExitCode $wgetExitCode -Fallback $failureMatch.Groups['kind'].Value
            }

            return [pscustomobject]@{
                NodeId                  = $failureMatch.Groups['nodeid'].Value
                ThroughputMbit          = 0.0
                Target                  = $failureMatch.Groups['target'].Value
                TimestampNs             = $failureMatch.Groups['timestamp'].Value
                ResultType              = 'final_failed'
                FailureReason           = $failureMatch.Groups['kind'].Value
                DownloadedBytes         = [int64]$failureMatch.Groups['bytes'].Value
                ExpectedBytes           = [int64]$failureMatch.Groups['expected'].Value
                DownloadDurationSeconds = [double]::Parse($failureMatch.Groups['sec'].Value, [System.Globalization.CultureInfo]::InvariantCulture)
                TimeoutSeconds          = [int]$failureMatch.Groups['timeout'].Value
                WgetExitCode            = $wgetExitCode
                WgetExitReason          = $wgetExitReason
                WgetStderr              = $wgetStderr
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

    $lines = @(
        $RawOutput -split '\r?\n' |
            ForEach-Object { Convert-ToTrimmedString -Value $_ } |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )

    $headerMatch = $null
    $summaryValues = @{}
    foreach ($line in $lines) {
        if ($null -eq $headerMatch) {
            $candidate = [regex]::Match($line, $headerRegex)
            if ($candidate.Success) {
                $headerMatch = $candidate
                continue
            }
        }

        if ($line.StartsWith('diag_summary,')) {
            foreach ($pair in [regex]::Matches($line.Substring(13), '(?<key>[a-z0-9_]+)=(?:"(?<quoted>[^"]*)"|(?<bare>[^ ]+))')) {
                $value = if ($pair.Groups['quoted'].Success) { $pair.Groups['quoted'].Value } else { $pair.Groups['bare'].Value }
                $summaryValues[$pair.Groups['key'].Value] = $value
            }
        }
    }

    if ($null -eq $headerMatch -or $summaryValues.Count -eq 0) {
        return $null
    }

    $sections = Get-NodeDiagnosticSections -Lines $lines

    return [pscustomobject]@{
        NodeId                 = $headerMatch.Groups['nodeid'].Value
        TargetHost             = $headerMatch.Groups['target'].Value
        SpeedtestDelaySeconds  = [int]$headerMatch.Groups['speedtest_delay'].Value
        DiagnosticDelaySeconds = [int]$headerMatch.Groups['diag_delay'].Value
        TimestampNs            = $headerMatch.Groups['timestamp'].Value
        GatewayProbe           = if ($summaryValues.ContainsKey('gateway_probe')) { $summaryValues['gateway_probe'] } else { '' }
        GatewayProbeKind       = if ($summaryValues.ContainsKey('gateway_probe_kind')) { $summaryValues['gateway_probe_kind'] } else { '' }
        PingGatewayLossPct     = if ($summaryValues.ContainsKey('ping_gateway_loss')) { [double]::Parse($summaryValues['ping_gateway_loss'], [System.Globalization.CultureInfo]::InvariantCulture) } else { -1 }
        PingTargetLossPct      = if ($summaryValues.ContainsKey('ping_target_loss')) { [double]::Parse($summaryValues['ping_target_loss'], [System.Globalization.CultureInfo]::InvariantCulture) } else { -1 }
        Load1                  = if ($summaryValues.ContainsKey('load1')) { [double]::Parse($summaryValues['load1'], [System.Globalization.CultureInfo]::InvariantCulture) } else { 0 }
        Load5                  = if ($summaryValues.ContainsKey('load5')) { [double]::Parse($summaryValues['load5'], [System.Globalization.CultureInfo]::InvariantCulture) } else { 0 }
        Load15                 = if ($summaryValues.ContainsKey('load15')) { [double]::Parse($summaryValues['load15'], [System.Globalization.CultureInfo]::InvariantCulture) } else { 0 }
        TargetIPv4             = if ($summaryValues.ContainsKey('target_ipv4')) { $summaryValues['target_ipv4'] } else { '' }
        TargetIPv6             = if ($summaryValues.ContainsKey('target_ipv6')) { $summaryValues['target_ipv6'] } else { '' }
        RouteGetIPv4           = if ($summaryValues.ContainsKey('route_get_ipv4')) { $summaryValues['route_get_ipv4'] } else { '' }
        RouteGetIPv6           = if ($summaryValues.ContainsKey('route_get_ipv6')) { $summaryValues['route_get_ipv6'] } else { '' }
        WgetStderr             = if ($summaryValues.ContainsKey('wget_stderr')) { $summaryValues['wget_stderr'] } else { '' }
        TcpGatewayProbePort    = if ($summaryValues.ContainsKey('tcp_gateway_probe_port')) { [int]$summaryValues['tcp_gateway_probe_port'] } else { 0 }
        TcpGatewayProbeResult  = if ($summaryValues.ContainsKey('tcp_gateway_probe_result')) { $summaryValues['tcp_gateway_probe_result'] } else { '' }
        TcpTargetProbePort     = if ($summaryValues.ContainsKey('tcp_target_probe_port')) { [int]$summaryValues['tcp_target_probe_port'] } else { 0 }
        TcpTargetProbeResult   = if ($summaryValues.ContainsKey('tcp_target_probe_result')) { $summaryValues['tcp_target_probe_result'] } else { '' }
        TargetResolution       = if ($sections.ContainsKey('target_resolution')) { $sections['target_resolution'] } else { '' }
        RouteGet               = if ($sections.ContainsKey('route_get')) { $sections['route_get'] } else { '' }
        TcpGatewayProbe        = if ($sections.ContainsKey('tcp_gateway_probe')) { $sections['tcp_gateway_probe'] } else { '' }
        TcpTargetProbe         = if ($sections.ContainsKey('tcp_target_probe')) { $sections['tcp_target_probe'] } else { '' }
        IpRule                 = if ($sections.ContainsKey('ip_rule')) { $sections['ip_rule'] } else { '' }
        BatctlIf               = if ($sections.ContainsKey('batctl_if')) { $sections['batctl_if'] } else { '' }
        BatctlN                = if ($sections.ContainsKey('batctl_n')) { $sections['batctl_n'] } else { '' }
        UbusNetworkDump        = if ($sections.ContainsKey('ubus_network_dump')) { $sections['ubus_network_dump'] } else { '' }
        UbusIfstatusWan        = if ($sections.ContainsKey('ubus_ifstatus_wan')) { $sections['ubus_ifstatus_wan'] } else { '' }
        UbusIfstatusWan6       = if ($sections.ContainsKey('ubus_ifstatus_wan6')) { $sections['ubus_ifstatus_wan6'] } else { '' }
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
    $downloadTimeoutSeconds = if ($Config.ContainsKey('SpeedtestDownloadTimeoutSeconds')) { [Math]::Max(1, [int]$Config.SpeedtestDownloadTimeoutSeconds) } else { 480 }
    $diagnostics = Get-NodeDiagnosticsSettings -Config $Config
    $diagnosticDelaySeconds = $delaySeconds + $diagnostics.DelaySeconds
    $gatewayTcpProbePort = if ($Config.ContainsKey('NodeDiagnosticsGatewayTcpProbePort')) { [int]$Config.NodeDiagnosticsGatewayTcpProbePort } else { 53 }
    $targetTcpProbePort = 0
    $targetUri = $null
    if ([uri]::TryCreate($targetUrl, [System.UriKind]::Absolute, [ref]$targetUri)) {
        if (-not $targetUri.IsDefaultPort) {
            $targetTcpProbePort = [int]$targetUri.Port
        }
        elseif ($targetUri.Scheme -eq 'https') {
            $targetTcpProbePort = 443
        }
        elseif ($targetUri.Scheme -eq 'http') {
            $targetTcpProbePort = 80
        }
    }
    $targetHost = Convert-ToShellSingleQuoted -Value (Get-NodeDiagnosticsTargetHost -Config $Config)
    $safeRunId = Convert-ToShellSingleQuoted -Value (Get-SafeFileNamePart -Value $RunId)

    $payload = @"
nodeid=`$(tr -d ':' </lib/gluon/core/sysconfig/primary_mac)
target_url='$targetUrlShell'
delay_seconds=$delaySeconds
sleep "`$delay_seconds"
start=`$(date +%s%N)
wget_exit_file="/tmp/harvester-wget-exit-`$$.txt"
wget_stderr_file="/tmp/ffmh-wget-stderr-$safeRunId-`$nodeid.log"
rm -f "`$wget_exit_file" "`$wget_stderr_file"
t0=`$(date +%s.%N)
wget -O /dev/null -q -T $downloadTimeoutSeconds "`$target_url" 2>"`$wget_stderr_file" &
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
case "`$wget_exit" in
    0) wget_exit_reason='success' ;;
    1) wget_exit_reason='generic_error' ;;
    2) wget_exit_reason='parse_error' ;;
    3) wget_exit_reason='file_io_error' ;;
    4) wget_exit_reason='network_failure' ;;
    5) wget_exit_reason='ssl_verification_failure' ;;
    6) wget_exit_reason='auth_failure' ;;
    7) wget_exit_reason='protocol_error' ;;
    8) wget_exit_reason='server_error' ;;
    124) wget_exit_reason='watchdog_timeout' ;;
    *) wget_exit_reason="exit_`$wget_exit" ;;
esac
t1=`$(date +%s.%N)
bytes=0
if [ "`$wget_exit" = "0" ]; then
    bytes="$targetBytes"
fi
awk -v nodeid="`$nodeid" -v start="`$start" -v t0="`$t0" -v t1="`$t1" -v target="`$target_url" -v bytes="`$bytes" -v wget_exit="`$wget_exit" -v expected_bytes="$targetBytes" -v timeout_seconds="$downloadTimeoutSeconds" 'BEGIN{
    sec=t1-t0
    if (sec < 0) {
        sec = 0
    }
    if (wget_exit != 0) {
        kind = (wget_exit == 124) ? "speedtest_timeout" : "wget_failed"
        printf "%s,nodeid=%s exit=%s bytes=%s sec=%.6f expected_bytes=%s timeout_seconds=%s target=\"%s\" %s\n",kind,nodeid,wget_exit,bytes,sec,expected_bytes,timeout_seconds,target,start
        exit 0
    }
    if (bytes <= 0 || sec <= 0) {
        printf "speedtest_invalid,nodeid=%s bytes=%s sec=%.6f expected_bytes=%s timeout_seconds=%s target=\"%s\" %s\n",nodeid,bytes,sec,expected_bytes,timeout_seconds,target,start
        exit 0
    }
    if (bytes != expected_bytes) {
        printf "speedtest_size_mismatch,nodeid=%s bytes=%s sec=%.6f expected_bytes=%s timeout_seconds=%s target=\"%s\" %s\n",nodeid,bytes,sec,expected_bytes,timeout_seconds,target,start
        exit 0
    }
    if (sec > timeout_seconds || (sec == timeout_seconds && (bytes != expected_bytes || wget_exit != 0))) {
        printf "speedtest_timeout,nodeid=%s exit=%s bytes=%s sec=%.6f expected_bytes=%s timeout_seconds=%s target=\"%s\" %s\n",nodeid,wget_exit,bytes,sec,expected_bytes,timeout_seconds,target,start
        exit 0
    }
    printf "speedtest,nodeid=%s download_mbit=%.2f bytes=%s sec=%.6f timeout_seconds=%s,target=\"%s\" %s\n",nodeid,(bytes*8)/(sec*1000000),bytes,sec,timeout_seconds,target,start
}'
printf 'measurement_meta,wget_exit_reason="%s" wget_exit_code=%s\n' "`$wget_exit_reason" "`$wget_exit"
echo 'measurement_section,name=wget_stderr'
if [ -f "`$wget_stderr_file" ]; then
    cat "`$wget_stderr_file" 2>&1 || true
fi
echo 'measurement_section_end,name=wget_stderr'
rm -f "`$wget_exit_file" "`$wget_stderr_file"
"@

    $diagnosticPayload = @"
nodeid=`$(tr -d ':' </lib/gluon/core/sysconfig/primary_mac)
target_host='$targetHost'
speedtest_delay_seconds=$delaySeconds
diagnostic_delay_seconds=$diagnosticDelaySeconds
sleep "`$diagnostic_delay_seconds"
ts=`$(date +%s%N)
wget_stderr_file="/tmp/ffmh-wget-stderr-$safeRunId-`$nodeid.log"
gateway4=`$(ip route 2>/dev/null | awk '/^default / { print `$3; exit }')
gateway6=`$(ip -6 route 2>/dev/null | awk '/^default / { print `$3; exit }')
gateway6_dev=`$(ip -6 route 2>/dev/null | awk '/^default / { for (i = 1; i <= NF; i++) if (`$i == "dev") { print `$(i+1); exit } }')
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
tcp_gateway_probe_port=$gatewayTcpProbePort
tcp_gateway_probe_result='unavailable'
tcp_gateway_probe_target="`$gateway_probe"
tcp_gateway_probe_output=''
if [ "`$tcp_gateway_probe_port" -gt 0 ] && [ -n "`$gateway_probe" ]; then
    if [ "`$gateway_probe_kind" = 'ipv6' ] && [ -n "`$gateway6_dev" ]; then
        case "`$tcp_gateway_probe_target" in
            *%*) ;;
            *) tcp_gateway_probe_target="`$tcp_gateway_probe_target%`$gateway6_dev" ;;
        esac
    fi

    if command -v nc >/dev/null 2>&1; then
        if [ "`$gateway_probe_kind" = 'ipv6' ]; then
            tcp_gateway_probe_output=`$(nc -6 -z -w 5 "`$tcp_gateway_probe_target" "`$tcp_gateway_probe_port" 2>&1)
        else
            tcp_gateway_probe_output=`$(nc -z -w 5 "`$tcp_gateway_probe_target" "`$tcp_gateway_probe_port" 2>&1)
        fi
        tcp_gateway_probe_exit=`$?
        if [ "`$tcp_gateway_probe_exit" = '0' ]; then
            tcp_gateway_probe_result='success'
        else
            tcp_gateway_probe_result="exit_`$tcp_gateway_probe_exit"
        fi
    else
        tcp_gateway_probe_result='tool_unavailable'
        tcp_gateway_probe_output='nc unavailable'
    fi
fi
wget_stderr='unavailable'
if [ -f "`$wget_stderr_file" ]; then
    wget_stderr=`$(tr '\r\n' '  ' <"`$wget_stderr_file" | sed 's/[[:space:]][[:space:]]*/ /g; s/^ //; s/ $//; s/"//g')
fi
resolved_target_ipv4=''
resolved_target_ipv6=''
if command -v nslookup >/dev/null 2>&1; then
    resolved_target_ipv4=`$(nslookup "`$target_host" 2>/dev/null | awk '/^Address [0-9]*: / { print `$3 } /^[Aa]ddress: / { print `$2 }' | grep -E '^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$' | head -n 1)
    resolved_target_ipv6=`$(nslookup "`$target_host" 2>/dev/null | awk '/^Address [0-9]*: / { print `$3 } /^[Aa]ddress: / { print `$2 }' | grep ':' | head -n 1)
fi
if [ -z "`$resolved_target_ipv4" ] && command -v getent >/dev/null 2>&1; then
    resolved_target_ipv4=`$(getent ahostsv4 "`$target_host" 2>/dev/null | awk 'NR==1 { print `$1 }')
fi
if [ -z "`$resolved_target_ipv6" ] && command -v getent >/dev/null 2>&1; then
    resolved_target_ipv6=`$(getent ahostsv6 "`$target_host" 2>/dev/null | awk 'NR==1 { print `$1 }')
fi
route_get_ipv4='unavailable'
route_get_ipv6='unavailable'
if [ -n "`$resolved_target_ipv4" ]; then
    route_get_ipv4=`$(ip route get "`$resolved_target_ipv4" 2>&1 || true)
fi
if [ -n "`$resolved_target_ipv6" ]; then
    route_get_ipv6=`$(ip -6 route get "`$resolved_target_ipv6" 2>&1 || true)
fi
route_get_ipv4=`$(printf '%s' "`$route_get_ipv4" | tr '\r\n' '  ' | sed 's/[[:space:]][[:space:]]*/ /g; s/^ //; s/ $//; s/"//g')
route_get_ipv6=`$(printf '%s' "`$route_get_ipv6" | tr '\r\n' '  ' | sed 's/[[:space:]][[:space:]]*/ /g; s/^ //; s/ $//; s/"//g')
tcp_target_probe_port=$targetTcpProbePort
tcp_target_probe_result='unavailable'
tcp_target_probe_target=''
tcp_target_probe_kind=''
tcp_target_probe_output=''
if [ "`$tcp_target_probe_port" -gt 0 ]; then
    if [ -n "`$resolved_target_ipv6" ]; then
        tcp_target_probe_target="`$resolved_target_ipv6"
        tcp_target_probe_kind='ipv6'
    elif [ -n "`$resolved_target_ipv4" ]; then
        tcp_target_probe_target="`$resolved_target_ipv4"
        tcp_target_probe_kind='ipv4'
    else
        tcp_target_probe_result='unresolved'
    fi

    if [ -n "`$tcp_target_probe_target" ]; then
        if command -v nc >/dev/null 2>&1; then
            if [ "`$tcp_target_probe_kind" = 'ipv6' ]; then
                tcp_target_probe_output=`$(nc -6 -z -w 5 "`$tcp_target_probe_target" "`$tcp_target_probe_port" 2>&1)
            else
                tcp_target_probe_output=`$(nc -z -w 5 "`$tcp_target_probe_target" "`$tcp_target_probe_port" 2>&1)
            fi
            tcp_target_probe_exit=`$?
            if [ "`$tcp_target_probe_exit" = '0' ]; then
                tcp_target_probe_result='success'
            else
                tcp_target_probe_result="exit_`$tcp_target_probe_exit"
            fi
        else
            tcp_target_probe_result='tool_unavailable'
            tcp_target_probe_output='nc unavailable'
        fi
    fi
fi
load1='0'
load5='0'
load15='0'
if [ -r /proc/loadavg ]; then
    read load1 load5 load15 _ </proc/loadavg
fi
printf 'diagnostic,nodeid=%s target_host="%s" speedtest_delay_seconds=%s diagnostic_delay_seconds=%s timestamp=%s\n' "`$nodeid" "`$target_host" "`$speedtest_delay_seconds" "`$diagnostic_delay_seconds" "`$ts"
printf 'diag_summary,load1=%s load5=%s load15=%s gateway_probe="%s" gateway_probe_kind="%s" ping_gateway_loss=%s ping_target_loss=%s target_ipv4="%s" target_ipv6="%s" route_get_ipv4="%s" route_get_ipv6="%s" wget_stderr="%s" tcp_gateway_probe_port=%s tcp_gateway_probe_result="%s" tcp_target_probe_port=%s tcp_target_probe_result="%s"\n' "`$load1" "`$load5" "`$load15" "`$gateway_probe" "`$gateway_probe_kind" "`$ping_gateway_loss" "`$ping_target_loss" "`$resolved_target_ipv4" "`$resolved_target_ipv6" "`$route_get_ipv4" "`$route_get_ipv6" "`$wget_stderr" "`$tcp_gateway_probe_port" "`$tcp_gateway_probe_result" "`$tcp_target_probe_port" "`$tcp_target_probe_result"
echo 'diag_section,name=ip_route'
ip route 2>&1 || true
echo 'diag_section_end,name=ip_route'
echo 'diag_section,name=ip6_route'
ip -6 route 2>&1 || true
echo 'diag_section_end,name=ip6_route'
echo 'diag_section,name=ip_rule'
ip rule 2>&1 || true
echo 'diag_section_end,name=ip_rule'
echo 'diag_section,name=target_resolution'
if command -v nslookup >/dev/null 2>&1; then
    nslookup "`$target_host" 2>&1 || true
elif command -v getent >/dev/null 2>&1; then
    getent ahosts "`$target_host" 2>&1 || true
else
    echo 'resolution tooling unavailable'
fi
echo 'diag_section_end,name=target_resolution'
echo 'diag_section,name=route_get'
if [ -n "`$resolved_target_ipv4" ]; then
    ip route get "`$resolved_target_ipv4" 2>&1 || true
else
    echo 'ipv4 target unresolved'
fi
if [ -n "`$resolved_target_ipv6" ]; then
    ip -6 route get "`$resolved_target_ipv6" 2>&1 || true
else
    echo 'ipv6 target unresolved'
fi
echo 'diag_section_end,name=route_get'
echo 'diag_section,name=tcp_gateway_probe'
printf 'target=%s port=%s result=%s\n' "`$tcp_gateway_probe_target" "`$tcp_gateway_probe_port" "`$tcp_gateway_probe_result"
if [ -n "`$tcp_gateway_probe_output" ]; then
    printf '%s\n' "`$tcp_gateway_probe_output"
fi
echo 'diag_section_end,name=tcp_gateway_probe'
echo 'diag_section,name=tcp_target_probe'
printf 'target=%s port=%s result=%s\n' "`$tcp_target_probe_target" "`$tcp_target_probe_port" "`$tcp_target_probe_result"
if [ -n "`$tcp_target_probe_output" ]; then
    printf '%s\n' "`$tcp_target_probe_output"
fi
echo 'diag_section_end,name=tcp_target_probe'
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
if command -v ubus >/dev/null 2>&1; then
    echo 'diag_section,name=ubus_network_dump'
    ubus call network.interface dump 2>&1 || true
    echo 'diag_section_end,name=ubus_network_dump'
    echo 'diag_section,name=ubus_ifstatus_wan'
    ubus call network.interface.wan status 2>&1 || true
    echo 'diag_section_end,name=ubus_ifstatus_wan'
    echo 'diag_section,name=ubus_ifstatus_wan6'
    ubus call network.interface.wan6 status 2>&1 || true
    echo 'diag_section_end,name=ubus_ifstatus_wan6'
fi
rm -f "`$wget_stderr_file"
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
    $probeCmd = "find '$remoteDirEscaped' -maxdepth 1 -type f -name '*.txt' | grep -q ."

    return [pscustomobject]@{
        TriggerCommand         = $triggerCmd
        RemoteResultFile       = $remoteResultPattern
        RemoteErrorFile        = ''
        ProbeCommand           = $probeCmd
        AssignedDelaySeconds   = $delaySeconds
        DiagnosticEnabled      = $diagnostics.Enabled
        DiagnosticDelaySeconds = $diagnosticDelaySeconds
    }
}





