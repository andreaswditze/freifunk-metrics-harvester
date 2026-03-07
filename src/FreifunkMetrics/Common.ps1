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

    for ($elapsed = 0; $elapsed -lt $Seconds; $elapsed++) {
        $remaining = $Seconds - $elapsed
        $percent = [int](($elapsed / [double]$Seconds) * 100)

        if ($nodeCount -gt 0 -and (($elapsed -eq 0) -or (($elapsed % $pollInterval) -eq 0))) {
            try {
                $finishedCount = Get-FinishedNodeResultCountBatch -Config $Config -RunId $RunId -Nodes $Nodes
            }
            catch {
                $finishedCount = -1
            }
        }

        if ($nodeCount -gt 0 -and $finishedCount -ge 0) {
            $status = 'Remaining: {0}s | finished: {1}/{2}' -f $remaining, $finishedCount, $nodeCount
            Update-ConsoleStatus -Message ('Wait {0}/{1}s: finished {2}/{3} nodes' -f $elapsed, $Seconds, $finishedCount, $nodeCount)
        }
        else {
            $status = 'Remaining: {0}s' -f $remaining
            Update-ConsoleStatus -Message ('Wait {0}/{1}s' -f $elapsed, $Seconds)
        }

        Write-Progress -Activity $Activity -Status $status -PercentComplete $percent
        Start-Sleep -Seconds 1
    }

    Write-Progress -Activity $Activity -Completed
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
