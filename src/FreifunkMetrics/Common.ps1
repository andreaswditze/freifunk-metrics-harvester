# Functions for this concern are loaded by FreifunkMetrics.psm1.



function Update-ConsoleStatus {
    [CmdletBinding()]
    param(
        [string]$Message = '',
        [switch]$Complete
    )

    $text = Convert-ToTrimmedString -Value $Message
    $width = [Math]::Max($script:ConsoleStatusLength, $text.Length)

    if ($Complete) {
        if ($script:ConsoleStatusLength -gt 0) {
            Write-Host ("`r" + (' ' * $script:ConsoleStatusLength) + "`r") -NoNewline
            $script:ConsoleStatusLength = 0
        }
        return
    }

    $padded = $text.PadRight($width)
    Write-Host ("`r$padded") -NoNewline
    $script:ConsoleStatusLength = $width
}

function Show-StartupBanner {
    [CmdletBinding()]
    param(
        [string]$Name = 'collect-node-metrics.ps1'
    )

    if ($script:ConsoleBannerShown) {
        return
    }

    Write-Host $Name
    $script:ConsoleBannerShown = $true
}

function Write-Log {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$Message,
        [ValidateSet('INFO', 'WARN', 'ERROR', 'DEBUG')]
        [string]$Level = 'INFO'
    )

    $timestamp = Get-Date -Format 'dd.MM.yyyy HH:mm:ss'
    $line = '{0}: [{1}] {2}' -f $timestamp, $Level, $Message

    if ($script:LogFilePath) {
        Add-Content -Path $script:LogFilePath -Value $line
    }

    if ($script:DailyLogFilePath) {
        Add-Content -Path $script:DailyLogFilePath -Value $line
    }
}

function Write-NodeActionLog {
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

    $safeDetail = Convert-ToTrimmedString -Value $Detail; $safeDetail = $safeDetail.Replace('"', "'")
    $message = 'NODE action={0} device_id={1} name="{2}" ip={3} domain={4} detail="{5}"' -f $Action, $Node.DeviceID, $Node.Name, $Node.IP, $Node.Domain, $safeDetail
    Write-Log -Level $Level -Message $message
}

function Start-NodeResultCountPoll {
    [CmdletBinding()]
    param(
        [hashtable]$Config = @{},
        [string]$RunId = '',
        [object[]]$Nodes = @()
    )

    $jobScript = {
        param($PollConfig, $PollRunId, $PollNodes, $ModulePath)

        Import-Module $ModulePath -Force | Out-Null

        $finished = 0
        foreach ($pollNode in @($PollNodes)) {
            if (Test-NodeResultFinished -Config $PollConfig -RunId $PollRunId -Node $pollNode) {
                $finished++
            }
        }

        $finished
    }

    $pollConfig = @{} + $Config
    $jobArgs = @($pollConfig, $RunId, @($Nodes), $script:ModuleFilePath)
    return Start-Job -ScriptBlock $jobScript -ArgumentList $jobArgs
}

function Receive-NodeResultCountPoll {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object]$Job
    )

    try {
        $result = @(Receive-Job -Job $Job -AutoRemoveJob -ErrorAction Stop)
        if ($result.Count -eq 0) {
            return -1
        }

        return [int]$result[-1]
    }
    catch {
        return -1
    }
}

function Stop-NodeResultCountPoll {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [object]$Job
    )

    if ($null -eq $Job) {
        return
    }

    try {
        Stop-Job -Job $Job -ErrorAction SilentlyContinue | Out-Null
    }
    catch {
    }

    try {
        Remove-Job -Job $Job -Force -ErrorAction SilentlyContinue | Out-Null
    }
    catch {
    }
}

function Wait-WithProgress {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [int]$Seconds,
        [string]$Activity = 'Waiting before collect phase',
        [hashtable]$Config = @{},
        [string]$RunId = '',
        [object[]]$Nodes = @(),
        [int]$PollIntervalSeconds = 15
    )

    if ($Seconds -le 0) {
        return
    }

    $nodeCount = @($Nodes).Count
    $pollInterval = [Math]::Max(1, $PollIntervalSeconds)
    $finishedCount = -1
    $startedAt = Get-Date
    $nextPollElapsed = 0
    $pollJob = $null

    try {
        while ($true) {
            $elapsed = [Math]::Min($Seconds, [int][Math]::Floor(((Get-Date) - $startedAt).TotalSeconds))
            $remaining = [Math]::Max(0, $Seconds - $elapsed)
            $percent = [int]((($Seconds - $remaining) / [double]$Seconds) * 100)

            if ($nodeCount -gt 0 -and $null -eq $pollJob -and $elapsed -ge $nextPollElapsed) {
                $pollJob = Start-NodeResultCountPoll -Config $Config -RunId $RunId -Nodes $Nodes
                $nextPollElapsed = $elapsed + $pollInterval
            }

            if ($null -ne $pollJob -and $pollJob.State -in @('Completed', 'Failed', 'Stopped')) {
                $finishedCount = Receive-NodeResultCountPoll -Job $pollJob
                $pollJob = $null
            }

            if ($nodeCount -gt 0) {
                $finishedLabel = if ($finishedCount -ge 0) { $finishedCount } else { 'pending' }
                $status = 'Remaining: {0}s | finished: {1}/{2}' -f $remaining, $finishedLabel, $nodeCount
                Update-ConsoleStatus -Message ('Wait {0}/{1}s: finished {2}/{3} nodes' -f $elapsed, $Seconds, $finishedLabel, $nodeCount)
            }
            else {
                $status = 'Remaining: {0}s' -f $remaining
                Update-ConsoleStatus -Message ('Wait {0}/{1}s' -f $elapsed, $Seconds)
            }

            Write-Progress -Activity $Activity -Status $status -PercentComplete $percent

            if ($nodeCount -gt 0 -and $finishedCount -ge $nodeCount) {
                break
            }

            if ($elapsed -ge $Seconds) {
                break
            }

            Start-Sleep -Seconds 1
        }
    }
    finally {
        Stop-NodeResultCountPoll -Job $pollJob
        Write-Progress -Activity $Activity -Completed
    }
}

function Convert-ToTrimmedString {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [object]$Value
    )

    if ($null -eq $Value) {
        return ''
    }

    return ([string]$Value).Trim()
}

function Convert-ToShellSingleQuoted {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [string]$Value
    )

    if ($null -eq $Value) {
        return ''
    }

    return $Value.Replace("'", "'\''")
}

function Get-SafeFileNamePart {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [string]$Value
    )

    $trimmed = Convert-ToTrimmedString -Value $Value
    if ([string]::IsNullOrWhiteSpace($trimmed)) {
        return 'unknown'
    }

    return ($trimmed -replace '[^0-9A-Za-z._-]', '_')
}

function Convert-NodeTimestampToUtc {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [string]$Timestamp
    )

    $raw = Convert-ToTrimmedString -Value $Timestamp
    if ([string]::IsNullOrWhiteSpace($raw)) {
        return ''
    }

    if ($raw -notmatch '^[0-9]+$') {
        return ''
    }

    try {
        $epoch = Get-Date -Date '1970-01-01T00:00:00Z'
        $value = [double]$raw
        $utc = $null

        if ($raw.Length -le 10) {
            $utc = $epoch.AddSeconds($value)
        }
        elseif ($raw.Length -le 13) {
            $utc = $epoch.AddMilliseconds($value)
        }
        elseif ($raw.Length -le 16) {
            $utc = $epoch.AddMilliseconds($value / 1000.0)
        }
        else {
            $utc = $epoch.AddMilliseconds($value / 1000000.0)
        }

        return $utc.ToUniversalTime().ToString('o')
    }
    catch {
        return ''
    }
}

