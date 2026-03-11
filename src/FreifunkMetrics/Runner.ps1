# Functions for this concern are loaded by FreifunkMetrics.psm1.

function Get-NodeFailureCategory {
    [CmdletBinding()]
    param(
        [string]$Stage,
        [string]$Detail
    )

    $normalizedDetail = ([string]$Detail).ToLowerInvariant()

    if ($Stage -eq 'trigger') {
        if ($normalizedDetail -match 'timed out|timeout|no route to host|network is unreachable|could not resolve|host key verification failed|connection refused|ssh failed') {
            return 'not_reachable'
        }

        return 'trigger_failed'
    }

    if ($Stage -eq 'collect') {
        if ($normalizedDetail -match 'pending_files=') {
            return 'download_pending'
        }

        if ($normalizedDetail -match 'no files found') {
            return 'no_result_file'
        }

        if ($normalizedDetail -match 'permission denied|scp|download|copy|collect failed|delete failed|cleanup failed') {
            return 'download_failed'
        }

        return 'collect_failed'
    }

    if ($Stage -eq 'final') {
        switch ($normalizedDetail) {
            'wget_failed' { return 'download_failed' }
            'speedtest_invalid' { return 'invalid_result' }
            'speedtest_size_mismatch' { return 'size_mismatch' }
            default { return 'final_failed' }
        }
    }

    return 'unknown_failure'
}

function Add-NodeFailureRecord {
    [CmdletBinding()]
    param(
        [AllowEmptyCollection()]
        [Parameter(Mandatory = $true)]
        [System.Collections.Generic.List[object]]$Failures,
        [Parameter(Mandatory = $true)]
        [object]$Node,
        [Parameter(Mandatory = $true)]
        [string]$Stage,
        [string]$Detail
    )

    $Failures.Add([pscustomobject]@{
            Node     = $Node
            Stage    = $Stage
            Category = (Get-NodeFailureCategory -Stage $Stage -Detail $Detail)
            Detail   = (Convert-ToTrimmedString -Value $Detail)
        })
}

function Format-NodeFailureSummary {
    [CmdletBinding()]
    param(
        [AllowEmptyCollection()]
        [object[]]$Failures = @()
    )

    if (@($Failures).Count -eq 0) {
        return @()
    }

    $categorySummary = @(
        $Failures |
            Group-Object -Property Category |
            Sort-Object Name |
            ForEach-Object { [string]$_.Name + '=' + [string]$_.Count }
    )

    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add('Failed node reasons: ' + ($categorySummary -join ', '))

    foreach ($failure in $Failures) {
        $label = if ([string]::IsNullOrWhiteSpace($failure.Node.Name)) { [string]$failure.Node.IP } else { ([string]$failure.Node.Name + ' (' + [string]$failure.Node.IP + ')') }
        $detail = if ([string]::IsNullOrWhiteSpace($failure.Detail)) { [string]$failure.Stage } else { ([string]$failure.Stage + '; ' + [string]$failure.Detail) }
        $lines.Add(' - ' + $label + ': ' + [string]$failure.Category + ' [' + $detail + ']')
    }

    return @($lines)
}

function Invoke-CollectNodeMetricsMain {
    [CmdletBinding()]
    param(
        [string]$ConfigPath,
        [string]$RunId,
        [switch]$VerboseLogging
    )

    $script:CurrentConfig = $null
    $script:LogFilePath = $null
    $script:DailyLogDir = $null
    $script:DailyLogFilePath = $null
    $script:ConsoleStatusLength = 0
    $script:ConsoleBannerShown = $false

    try {
        Show-StartupBanner
        Update-ConsoleStatus -Message 'Startup 1/4: loading config'
        $config = Get-EnvironmentConfig -RequestedPath $ConfigPath
        $script:CurrentConfig = $config
        Update-ConsoleStatus -Message 'Startup 2/4: preparing directories'

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

        Write-Log -Message 'Startup'
        Write-Log -Message "Using config: $($config.ConfigPath)"
        Write-Log -Message "Config summary: db=$($config.DatabasePath), raw=$($config.RawResultBaseDir), temp=$($config.TempDir), files=$(@($config.ExcelInputFiles).Count), dirs=$(@($config.ExcelInputDirectories).Count), recurse=$($config.ExcelSearchRecurse), test_mode=$($config.UseTestNodeIPs), test_ips=$(@($config.TestNodeIPs).Count), trigger_parallelism=$($config.TriggerParallelism), collect_parallelism=$($config.CollectParallelism), random_delay_max=$($config.TriggerRandomDelayMaxSeconds), target_bytes=$($config.SpeedtestTargetBytes)"

        Update-ConsoleStatus -Message 'Startup 3/4: initializing database'
        Initialize-Database -Config $config
        Update-ConsoleStatus -Message 'Startup 4/4: importing node list'

        $importResult = $null
        if ($config.UseTestNodeIPs) {
            $importResult = Get-TestNodesFromConfig -Config $config
            Write-Log -Message "Testing mode enabled: using TestNodeIPs only, count=$(@($importResult.Nodes).Count)"
        }
        else {
            $importResult = Import-NodeListFromExcel -Config $config
        }

        $nodes = @($importResult.Nodes)

        if ($nodes.Count -eq 0) {
            Write-Log -Level WARN -Message 'No valid nodes found. Exiting.'
            exit 0
        }

        if (-not $RunId) {
            $RunId = 'run-{0}' -f (Get-Date -Format 'yyyyMMdd-HHmmss')
        }

        $runInfo = Start-MeasurementRun -Config $config -RunId $RunId -SourceFiles $importResult.SourceFiles -TotalNodes $nodes.Count

        $triggeredNodes = New-Object System.Collections.Generic.List[object]
        $failedNodes = New-Object System.Collections.Generic.List[object]
        $reachableCount = 0
        $successfulDeliveryNodeCount = 0
        $failedDeliveryNodeCount = 0

        Update-ConsoleStatus -Complete
        Write-Log -Message "Trigger phase start, nodes=$($nodes.Count), parallelism=$($config.TriggerParallelism)"
        $triggerTotal = [Math]::Max(1, $nodes.Count)
        $triggerIndex = 0
        Update-ConsoleStatus -Message ("Trigger 0/{0}: waiting for completed jobs" -f $triggerTotal)
        Write-Progress -Id 1 -Activity 'Triggering nodes' -Status ("0/{0}" -f $triggerTotal) -PercentComplete 0
        foreach ($triggerEntry in Invoke-NodeTriggerBatch -Config $config -Nodes $nodes -RunId $RunId) {
            $triggerIndex++
            $node = $triggerEntry.Node
            $triggerResult = $triggerEntry.TriggerResult
            Update-ConsoleStatus -Message ("Trigger {0}/{1}: {2}" -f $triggerIndex, $triggerTotal, $node.IP)
            Write-Progress -Id 1 -Activity 'Triggering nodes' -Status ("{0}/{1}" -f $triggerIndex, $triggerTotal) -PercentComplete ([int](($triggerIndex / [double]$triggerTotal) * 100))

            try {
                Write-NodeActionLog -Node $node -Action 'trigger_start' -Detail 'attempting ssh trigger'
                $triggeredAtUtc = (Get-Date).ToUniversalTime().ToString('o')

                if ($triggerResult.Triggered) {
                    $reachableCount++
                    $triggeredNodes.Add($node)
                    Add-NodeJobRecord -Config $config -RunId $RunId -Node $node -Status 'triggered' -TriggeredAtUtc $triggeredAtUtc -ResultFile $triggerResult.RemoteResultFile -ErrorFile $triggerResult.RemoteErrorFile
                    Write-NodeActionLog -Node $node -Action 'trigger_success' -Detail ('remote background job started; assigned_delay_seconds=' + $triggerResult.AssignedDelaySeconds)
                }
                else {
                    $failedDeliveryNodeCount++
                    Add-NodeFailureRecord -Failures $failedNodes -Node $node -Stage 'trigger' -Detail $triggerResult.Error
                    Add-NodeJobRecord -Config $config -RunId $RunId -Node $node -Status 'trigger_failed' -TriggeredAtUtc $triggeredAtUtc -ErrorMessage $triggerResult.Error
                    Write-NodeActionLog -Node $node -Action 'trigger_failed' -Detail $triggerResult.Error -Level WARN
                }
            }
            catch {
                $failedDeliveryNodeCount++
                Add-NodeFailureRecord -Failures $failedNodes -Node $node -Stage 'trigger' -Detail $_.Exception.Message
                Add-NodeJobRecord -Config $config -RunId $RunId -Node $node -Status 'trigger_exception' -TriggeredAtUtc ((Get-Date).ToUniversalTime().ToString('o')) -ErrorMessage $_.Exception.Message
                Write-NodeActionLog -Node $node -Action 'trigger_exception' -Detail $_.Exception.Message -Level ERROR
            }
        }
        Write-Progress -Id 1 -Activity 'Triggering nodes' -Completed

        $collectWaitTimeoutSeconds = if ($config.ContainsKey('CollectWaitTimeoutSeconds')) { [Math]::Max(1, [int]$config.CollectWaitTimeoutSeconds) } else { 300 }
        $waitSeconds = [Math]::Max(0, [int]$config.TriggerRandomDelayMaxSeconds) + $collectWaitTimeoutSeconds
        if ($triggeredNodes.Count -gt 0) {
            Write-Log -Message "Waiting up to $waitSeconds seconds for node results before collect phase"
            Wait-WithProgress -Seconds $waitSeconds -Activity 'Waiting for node results' -Config $config -RunId $RunId -Nodes @($triggeredNodes.ToArray())
        }
        else {
            Write-Log -Level WARN -Message 'Skipping wait phase because no nodes were triggered successfully.'
        }

        $collectedCount = 0
        $collectedFileCount = 0
        $parsedCount = 0
        $collectTotal = [Math]::Max(1, $triggeredNodes.Count)
        $collectIndex = 0
        Update-ConsoleStatus -Message ("Collect 0/{0}: starting result collection" -f $collectTotal)
        Write-Progress -Id 2 -Activity 'Collecting node results' -Status ("0/{0}" -f $collectTotal) -PercentComplete 0

        Write-Log -Message "Collect phase start, nodes=$($triggeredNodes.Count), parallelism=$($config.CollectParallelism)"
        if ($triggeredNodes.Count -eq 0) {
            Write-Log -Level WARN -Message 'Skipping collect phase because no nodes were triggered successfully.'
        }
        foreach ($collectEntry in $(if ($triggeredNodes.Count -gt 0) { Invoke-NodeCollectBatch -Config $config -Nodes @($triggeredNodes.ToArray()) -RunId $RunId -RawDir $runInfo.RawDir } else { @() })) {
            $collectIndex++
            $node = $collectEntry.Node
            $collect = $collectEntry.CollectResult
            Update-ConsoleStatus -Message ("Collect {0}/{1}: {2}" -f $collectIndex, $collectTotal, $node.IP)
            Write-Progress -Id 2 -Activity 'Collecting node results' -Status ("{0}/{1}" -f $collectIndex, $collectTotal) -PercentComplete ([int](($collectIndex / [double]$collectTotal) * 100))
            try {
                Write-NodeActionLog -Node $node -Action 'collect_start' -Detail 'attempting result collection'
                $collectedAtUtc = (Get-Date).ToUniversalTime().ToString('o')

                if (-not $collect.Success) {
                    $failedDeliveryNodeCount++
                    Add-NodeFailureRecord -Failures $failedNodes -Node $node -Stage 'collect' -Detail $collect.ErrorOutput
                    Add-NodeJobRecord -Config $config -RunId $RunId -Node $node -Status 'collect_failed' -CollectedAtUtc $collectedAtUtc -ErrorMessage $collect.ErrorOutput
                    Write-NodeActionLog -Node $node -Action 'collect_failed' -Detail $collect.ErrorOutput -Level WARN
                    continue
                }

                $collectedFiles = if ($null -ne $collect.PSObject.Properties['Files']) { @($collect.Files) } else { @() }
                $diagnosticFiles = if ($null -ne $collect.PSObject.Properties['DiagnosticFiles']) { @($collect.DiagnosticFiles) } else { @() }
                $pendingFiles = if ($null -ne $collect.PSObject.Properties['PendingFiles']) { @($collect.PendingFiles) } else { @() }
                $diagnosticsKeepThreshold = if ($config.ContainsKey('NodeDiagnosticsKeepThresholdMbit')) { [double]$config.NodeDiagnosticsKeepThresholdMbit } else { 10.0 }

                if (@($pendingFiles).Count -gt 0) {
                    $pendingSummary = 'pending_files=' + @($pendingFiles).Count + '; first_file=' + $pendingFiles[0].LocalPath + '; raw_size=' + $pendingFiles[0].RawSize
                    Add-NodeFailureRecord -Failures $failedNodes -Node $node -Stage 'collect' -Detail $pendingSummary
                    Add-NodeJobRecord -Config $config -RunId $RunId -Node $node -Status 'collect_pending' -CollectedAtUtc $collectedAtUtc -ResultFile ((@($pendingFiles | ForEach-Object { $_.LocalPath }) -join ';' ) ) -ErrorMessage $pendingSummary
                    Write-NodeActionLog -Node $node -Action 'collect_pending' -Detail $pendingSummary -Level WARN
                }

                if (@($collectedFiles).Count -eq 0) {
                    if (@($diagnosticFiles).Count -gt 0) {
                        foreach ($diagnosticFile in $diagnosticFiles) {
                            $diagnosticRecord = [pscustomobject]@{
                                NodeId                 = $diagnosticFile.ParsedDiagnostic.NodeId
                                TargetHost             = $diagnosticFile.ParsedDiagnostic.TargetHost
                                SpeedtestDelaySeconds  = $diagnosticFile.ParsedDiagnostic.SpeedtestDelaySeconds
                                DiagnosticDelaySeconds = $diagnosticFile.ParsedDiagnostic.DiagnosticDelaySeconds
                                TimestampNs            = $diagnosticFile.ParsedDiagnostic.TimestampNs
                                GatewayProbe           = $diagnosticFile.ParsedDiagnostic.GatewayProbe
                                GatewayProbeKind       = $diagnosticFile.ParsedDiagnostic.GatewayProbeKind
                                PingGatewayLossPct     = $diagnosticFile.ParsedDiagnostic.PingGatewayLossPct
                                PingTargetLossPct      = $diagnosticFile.ParsedDiagnostic.PingTargetLossPct
                                Load1                  = $diagnosticFile.ParsedDiagnostic.Load1
                                Load5                  = $diagnosticFile.ParsedDiagnostic.Load5
                                Load15                 = $diagnosticFile.ParsedDiagnostic.Load15
                                LocalPath              = $diagnosticFile.LocalPath
                                RawOutput              = $diagnosticFile.RawOutput
                            }
                            Save-NodeDiagnostic -Config $config -Node $node -RunId $RunId -Diagnostic $diagnosticRecord
                        }
                        Write-NodeActionLog -Node $node -Action 'diagnostic_kept' -Detail ('reason=no_measurement; files=' + @($diagnosticFiles).Count + '; first_file=' + $diagnosticFiles[0].LocalPath) -Level WARN
                    }

                    if (@($pendingFiles).Count -gt 0) {
                        $failedDeliveryNodeCount++
                        continue
                    }

                    $emptyMessage = 'no files found in remote harvester dir'
                    $failedDeliveryNodeCount++
                    Add-NodeFailureRecord -Failures $failedNodes -Node $node -Stage 'collect' -Detail $emptyMessage
                    Add-NodeJobRecord -Config $config -RunId $RunId -Node $node -Status 'collected_empty' -CollectedAtUtc $collectedAtUtc -ErrorMessage $emptyMessage
                    Write-NodeActionLog -Node $node -Action 'collect_empty' -Detail $emptyMessage -Level WARN
                    continue
                }

                $collectedCount++
                $collectedFileCount += @($collectedFiles).Count

                $successfulFiles = @($collectedFiles | Where-Object { $_.ParsedMeasurement.ResultType -eq 'success' })
                $failedFiles = @($collectedFiles | Where-Object { $_.ParsedMeasurement.ResultType -eq 'final_failed' })
                $nodeStatus = if (@($successfulFiles).Count -gt 0 -and @($failedFiles).Count -gt 0) { 'collected_mixed' } elseif (@($successfulFiles).Count -gt 0) { 'collected' } else { 'collected_failed_result' }

                if (@($successfulFiles).Count -gt 0) {
                    $successfulDeliveryNodeCount++
                }
                else {
                    $failedDeliveryNodeCount++
                    $failureReason = if (@($failedFiles).Count -gt 0) { @($failedFiles | ForEach-Object { $_.ParsedMeasurement.FailureReason } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Select-Object -First 1)[0] } else { '' }
                    Add-NodeFailureRecord -Failures $failedNodes -Node $node -Stage 'final' -Detail $failureReason
                }

                $resultFiles = (@($collectedFiles | ForEach-Object { $_.LocalPath }) -join ';' )
                Add-NodeJobRecord -Config $config -RunId $RunId -Node $node -Status $nodeStatus -CollectedAtUtc $collectedAtUtc -ResultFile $resultFiles -ErrorFile '' -ErrorMessage $collect.ErrorOutput
                Write-NodeActionLog -Node $node -Action 'collect_success' -Detail ('files=' + @($collectedFiles).Count + '; success=' + @($successfulFiles).Count + '; final_failed=' + @($failedFiles).Count + '; first_file=' + $collectedFiles[0].LocalPath)

                foreach ($file in $collectedFiles) {
                    $parsed = $file.ParsedMeasurement
                    $downloadDuration = if ($null -ne $parsed.PSObject.Properties['DownloadDurationSeconds']) { $parsed.DownloadDurationSeconds } else { '' }
                    $downloadedBytes = if ($null -ne $parsed.PSObject.Properties['DownloadedBytes']) { $parsed.DownloadedBytes } else { '' }
                    $expectedBytes = if ($null -ne $parsed.PSObject.Properties['ExpectedBytes']) { $parsed.ExpectedBytes } else { '' }
                    $timeoutSeconds = if ($null -ne $parsed.PSObject.Properties['TimeoutSeconds']) { $parsed.TimeoutSeconds } else { '' }
                    $wgetExitCode = if ($null -ne $parsed.PSObject.Properties['WgetExitCode']) { $parsed.WgetExitCode } else { '' }

                    if ($parsed.ResultType -eq 'success') {
                        $parsedCount++
                        Write-NodeActionLog -Node $node -Action 'parse_success' -Detail ('nodeid=' + $parsed.NodeId + '; throughput_mbit=' + $parsed.ThroughputMbit + '; duration_seconds=' + $downloadDuration + '; bytes=' + $downloadedBytes + '; timeout_seconds=' + $timeoutSeconds + '; source_file=' + $file.LocalPath)
                    }
                    elseif ($parsed.ResultType -eq 'final_failed') {
                        $parsedCount++
                        Write-NodeActionLog -Node $node -Action 'parse_final_failed' -Detail ('nodeid=' + $parsed.NodeId + '; failure_reason=' + $parsed.FailureReason + '; throughput_mbit=0; duration_seconds=' + $downloadDuration + '; bytes=' + $downloadedBytes + '; expected_bytes=' + $expectedBytes + '; timeout_seconds=' + $timeoutSeconds + '; wget_exit_code=' + $wgetExitCode + '; source_file=' + $file.LocalPath) -Level WARN
                    }
                    else {
                        Write-NodeActionLog -Node $node -Action 'parse_failed' -Detail ('raw output stored, parser did not match; source_file=' + $file.LocalPath) -Level WARN
                    }

                    Save-Measurement -Config $config -Node $node -RunId $RunId -RawOutput $file.RawOutput -ParsedMeasurement $parsed
                }

                if (@($diagnosticFiles).Count -gt 0) {
                    $keepDiagnostics = Test-ShouldKeepNodeDiagnostics -MeasurementFiles $collectedFiles -KeepThresholdMbit $diagnosticsKeepThreshold
                    if ($keepDiagnostics) {
                        foreach ($diagnosticFile in $diagnosticFiles) {
                            $diagnosticRecord = [pscustomobject]@{
                                NodeId                 = $diagnosticFile.ParsedDiagnostic.NodeId
                                TargetHost             = $diagnosticFile.ParsedDiagnostic.TargetHost
                                SpeedtestDelaySeconds  = $diagnosticFile.ParsedDiagnostic.SpeedtestDelaySeconds
                                DiagnosticDelaySeconds = $diagnosticFile.ParsedDiagnostic.DiagnosticDelaySeconds
                                TimestampNs            = $diagnosticFile.ParsedDiagnostic.TimestampNs
                                GatewayProbe           = $diagnosticFile.ParsedDiagnostic.GatewayProbe
                                GatewayProbeKind       = $diagnosticFile.ParsedDiagnostic.GatewayProbeKind
                                PingGatewayLossPct     = $diagnosticFile.ParsedDiagnostic.PingGatewayLossPct
                                PingTargetLossPct      = $diagnosticFile.ParsedDiagnostic.PingTargetLossPct
                                Load1                  = $diagnosticFile.ParsedDiagnostic.Load1
                                Load5                  = $diagnosticFile.ParsedDiagnostic.Load5
                                Load15                 = $diagnosticFile.ParsedDiagnostic.Load15
                                LocalPath              = $diagnosticFile.LocalPath
                                RawOutput              = $diagnosticFile.RawOutput
                            }
                            Save-NodeDiagnostic -Config $config -Node $node -RunId $RunId -Diagnostic $diagnosticRecord
                        }

                        $keepReason = if (@($failedFiles).Count -gt 0) { 'final_failed' } else { 'throughput_le_threshold' }
                        Write-NodeActionLog -Node $node -Action 'diagnostic_kept' -Detail ('reason=' + $keepReason + '; threshold_mbit=' + $diagnosticsKeepThreshold + '; files=' + @($diagnosticFiles).Count + '; first_file=' + $diagnosticFiles[0].LocalPath) -Level WARN
                    }
                    else {
                        Remove-NodeDiagnosticArtifacts -DiagnosticFiles $diagnosticFiles
                        Write-NodeActionLog -Node $node -Action 'diagnostic_discarded' -Detail ('reason=throughput_gt_threshold; threshold_mbit=' + $diagnosticsKeepThreshold + '; files=' + @($diagnosticFiles).Count)
                    }
                }
            }
            catch {
                $failedDeliveryNodeCount++
                Add-NodeFailureRecord -Failures $failedNodes -Node $node -Stage 'collect' -Detail $_.Exception.Message
                Add-NodeJobRecord -Config $config -RunId $RunId -Node $node -Status 'collect_exception' -CollectedAtUtc ((Get-Date).ToUniversalTime().ToString('o')) -ErrorMessage $_.Exception.Message
                Write-NodeActionLog -Node $node -Action 'collect_exception' -Detail $_.Exception.Message -Level ERROR
            }
        }

        Write-Progress -Id 2 -Activity 'Collecting node results' -Completed
        Update-ConsoleStatus -Complete
        Complete-MeasurementRun -Config $config -RunId $RunId -ReachableNodes $reachableCount -CollectedNodes $collectedCount -ParsedNodes $parsedCount -Status 'completed'
        Write-Log -Message "Run summary: total=$($nodes.Count), reachable=$reachableCount, collected_nodes=$collectedCount, collected_files=$collectedFileCount, parsed=$parsedCount, successful_nodes=$successfulDeliveryNodeCount, failed_nodes=$failedDeliveryNodeCount"
        Write-Host ("Node delivery summary: successful={0}, failed={1}" -f $successfulDeliveryNodeCount, $failedDeliveryNodeCount)
        if ($failedNodes.Count -gt 0) {
            foreach ($failureLine in Format-NodeFailureSummary -Failures @($failedNodes.ToArray())) {
                Write-Host $failureLine
            }
        }
    }
    catch {
        Update-ConsoleStatus -Complete
        if ($script:CurrentConfig -and $RunId) {
            try {
                Complete-MeasurementRun -Config $script:CurrentConfig -RunId $RunId -ReachableNodes 0 -CollectedNodes 0 -ParsedNodes 0 -Status 'failed' -Notes $_.Exception.Message
            }
            catch {
                # Ignore secondary failure while reporting primary error.
            }
        }

        Write-Log -Level ERROR -Message "Fatal error: $($_.Exception.Message)"
        throw
    }
}
