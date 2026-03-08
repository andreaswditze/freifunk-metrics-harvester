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

function Get-NodeProgressKey {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object]$Node
    )

    $properties = $Node.PSObject.Properties
    return @(
        Convert-ToTrimmedString -Value $(if ($null -ne $properties['DeviceID']) { $properties['DeviceID'].Value })
        Convert-ToTrimmedString -Value $(if ($null -ne $properties['IP']) { $properties['IP'].Value })
        Convert-ToTrimmedString -Value $(if ($null -ne $properties['Name']) { $properties['Name'].Value })
        Convert-ToTrimmedString -Value $(if ($null -ne $properties['Domain']) { $properties['Domain'].Value })
    ) -join '|'
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

        $pendingNodeKeys = New-Object System.Collections.Generic.List[string]
        foreach ($pollNode in @($PollNodes)) {
            if (-not (Test-NodeResultFinished -Config $PollConfig -RunId $PollRunId -Node $pollNode)) {
                $pendingNodeKeys.Add((Get-NodeProgressKey -Node $pollNode))
            }
        }

        [pscustomobject]@{
            PendingNodeKeys = @($pendingNodeKeys.ToArray())
        }
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
            return $null
        }

        $pollResult = $result[-1]
        if ($null -eq $pollResult -or -not ($pollResult.PSObject.Properties.Name -contains 'PendingNodeKeys')) {
            return $null
        }

        return $pollResult
    }
    catch {
        return $null
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
    $pendingNodes = @($Nodes)
    $pollInterval = [Math]::Max(1, $PollIntervalSeconds)
    $finishedCount = -1
    $hasFinishedCount = $false
    $startedAt = Get-Date
    $nextPollElapsed = 0
    $pollJob = $null

    try {
        while ($true) {
            $elapsed = [Math]::Min($Seconds, [int][Math]::Floor(((Get-Date) - $startedAt).TotalSeconds))
            $remaining = [Math]::Max(0, $Seconds - $elapsed)
            $percent = [int]((($Seconds - $remaining) / [double]$Seconds) * 100)

            if ($pendingNodes.Count -gt 0 -and $null -eq $pollJob -and $elapsed -ge $nextPollElapsed) {
                $pollJob = Start-NodeResultCountPoll -Config $Config -RunId $RunId -Nodes $pendingNodes
                $nextPollElapsed = $elapsed + $pollInterval
            }

            if ($null -ne $pollJob -and $pollJob.State -in @('Completed', 'Failed', 'Stopped')) {
                $pollResult = Receive-NodeResultCountPoll -Job $pollJob
                $pollJob = $null

                if ($null -ne $pollResult) {
                    $pendingNodeKeys = @($pollResult.PendingNodeKeys)
                    if ($pendingNodeKeys.Count -eq 0) {
                        $pendingNodes = @()
                    }
                    else {
                        $pendingNodes = @($pendingNodes | Where-Object { $pendingNodeKeys -contains (Get-NodeProgressKey -Node $_) })
                    }

                    $finishedCount = $nodeCount - $pendingNodes.Count
                    $hasFinishedCount = $true
                }
            }

            if ($nodeCount -gt 0) {
                $finishedLabel = if ($hasFinishedCount) { $finishedCount } else { 'pending' }
                $status = 'Remaining: {0}s | finished: {1}/{2}' -f $remaining, $finishedLabel, $nodeCount
                Update-ConsoleStatus -Message ('Wait {0}/{1}s: finished {2}/{3} nodes' -f $elapsed, $Seconds, $finishedLabel, $nodeCount)
            }
            else {
                $status = 'Remaining: {0}s' -f $remaining
                Update-ConsoleStatus -Message ('Wait {0}/{1}s' -f $elapsed, $Seconds)
            }

            Write-Progress -Activity $Activity -Status $status -PercentComplete $percent

            if ($nodeCount -gt 0 -and $hasFinishedCount -and $finishedCount -ge $nodeCount) {
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

