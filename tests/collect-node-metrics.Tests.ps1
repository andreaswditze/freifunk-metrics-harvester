BeforeAll {
    Remove-Module FreifunkMetrics -ErrorAction SilentlyContinue
    Import-Module "$PSScriptRoot/../src/FreifunkMetrics.psm1" -Force
}

AfterAll {
    Remove-Module FreifunkMetrics -ErrorAction SilentlyContinue
}

Describe 'Module entry points' {
    It 'exports collector functions from the module' {
        (Get-Command ConvertFrom-MeasurementOutput -ErrorAction Stop).Source | Should -Be 'FreifunkMetrics'
        (Get-Command Invoke-CollectNodeMetricsMain -ErrorAction Stop).Source | Should -Be 'FreifunkMetrics'
    }

    It 'wrapper script loads the module in no-run mode' {
        Remove-Module FreifunkMetrics -ErrorAction SilentlyContinue
        . "$PSScriptRoot/../src/collect-node-metrics.ps1" -NoRun

        (Get-Command ConvertFrom-MeasurementOutput -ErrorAction Stop).Source | Should -Be 'FreifunkMetrics'
    }
}

Describe 'Invoke-CollectNodeMetricsMain' {
    It 'prints a final delivery summary for successful and failed nodes' {
        InModuleScope FreifunkMetrics {
            $baseDir = Join-Path $TestDrive 'runner-main'
            $nodeSuccess = [pscustomobject]@{ DeviceID = 'node-001'; Name = 'Node 1'; IP = '2a03:2260::1'; Domain = 'dom-a' }
            $nodeFailedResult = [pscustomobject]@{ DeviceID = 'node-002'; Name = 'Node 2'; IP = '2a03:2260::2'; Domain = 'dom-b' }
            $nodeTriggerFailed = [pscustomobject]@{ DeviceID = 'node-003'; Name = 'Node 3'; IP = '2a03:2260::3'; Domain = 'dom-c' }

            $config = @{
                ConfigPath = 'test-config.ps1'
                ScriptBaseDir = $baseDir
                RawResultBaseDir = (Join-Path $baseDir 'raw')
                LogDir = (Join-Path $baseDir 'log')
                TempDir = (Join-Path $baseDir 'temp')
                DatabasePath = (Join-Path $baseDir 'metrics.db')
                LogFilePrefix = 'collect-node-metrics'
                ExcelInputFiles = @()
                ExcelInputDirectories = @()
                ExcelSearchRecurse = $false
                UseTestNodeIPs = $false
                TestNodeIPs = @()
                TriggerParallelism = 2
                CollectParallelism = 2
                TriggerRandomDelayMaxSeconds = 0
                SpeedtestTargetBytes = 104857600
            }

            $successFile = [pscustomobject]@{
                LocalPath = 'success.txt'
                RawOutput = 'speedtest,nodeid=aa download_mbit=10.5,target="https://example.invalid/test.bin" 1772839860'
                ParsedMeasurement = [pscustomobject]@{ ResultType = 'success'; NodeId = 'aa'; ThroughputMbit = 10.5 }
            }
            $failedFile = [pscustomobject]@{
                LocalPath = 'failed.txt'
                RawOutput = 'wget_failed,nodeid=bb exit=4 bytes=0 expected_bytes=104857600 target="https://example.invalid/test.bin" 1772839860'
                ParsedMeasurement = [pscustomobject]@{ ResultType = 'final_failed'; NodeId = 'bb'; FailureReason = 'wget_failed'; ThroughputMbit = 0 }
            }

            Mock Show-StartupBanner {}
            Mock Update-ConsoleStatus {}
            Mock Write-Progress {}
            Mock Write-Host {}
            Mock Write-Log {}
            Mock Initialize-Database {}
            Mock Get-EnvironmentConfig { $config }
            Mock Import-NodeListFromExcel { [pscustomobject]@{ Nodes = @($nodeSuccess, $nodeFailedResult, $nodeTriggerFailed); SourceFiles = @('nodes.csv') } }
            Mock Start-MeasurementRun { [pscustomobject]@{ RawDir = (Join-Path $TestDrive 'raw-run') } }
            Mock Write-NodeActionLog {}
            Mock Add-NodeJobRecord {}
            Mock Wait-WithProgress {}
            Mock Save-Measurement {}
            Mock Complete-MeasurementRun {}
            Mock Invoke-NodeTriggerBatch {
                @(
                    [pscustomobject]@{ Node = $nodeSuccess; TriggerResult = [pscustomobject]@{ Triggered = $true; RemoteResultFile = '/tmp/node-001.txt'; RemoteErrorFile = '/tmp/node-001.err'; AssignedDelaySeconds = 0 } }
                    [pscustomobject]@{ Node = $nodeFailedResult; TriggerResult = [pscustomobject]@{ Triggered = $true; RemoteResultFile = '/tmp/node-002.txt'; RemoteErrorFile = '/tmp/node-002.err'; AssignedDelaySeconds = 0 } }
                    [pscustomobject]@{ Node = $nodeTriggerFailed; TriggerResult = [pscustomobject]@{ Triggered = $false; Error = 'ssh failed' } }
                )
            }
            Mock Invoke-NodeCollectBatch {
                @(
                    [pscustomobject]@{ Node = $nodeSuccess; CollectResult = [pscustomobject]@{ Success = $true; ErrorOutput = ''; Files = @($successFile); PendingFiles = @() } }
                    [pscustomobject]@{ Node = $nodeFailedResult; CollectResult = [pscustomobject]@{ Success = $true; ErrorOutput = ''; Files = @($failedFile); PendingFiles = @() } }
                )
            }

            try {
                Invoke-CollectNodeMetricsMain -RunId 'run-summary'

                Assert-MockCalled Complete-MeasurementRun -Times 1 -Exactly -ParameterFilter { $ReachableNodes -eq 2 -and $CollectedNodes -eq 2 -and $ParsedNodes -eq 2 -and $Status -eq 'completed' }
                Assert-MockCalled Write-Log -Times 1 -ParameterFilter { $Message -eq 'Run summary: total=3, reachable=2, collected_nodes=2, collected_files=2, parsed=2, successful_nodes=1, failed_nodes=2' }
                Assert-MockCalled Write-Host -Times 1 -Exactly -ParameterFilter { $Object -eq 'Node delivery summary: successful=1, failed=2' }
            }
            finally {
                $script:CurrentConfig = $null
                $script:LogFilePath = $null
                $script:DailyLogDir = $null
                $script:DailyLogFilePath = $null
                $script:ConsoleStatusLength = 0
                $script:ConsoleBannerShown = $false
            }
        }
    }
}

Describe 'ConvertFrom-MeasurementOutput' {
    It 'parses valid line protocol' {
        $raw = 'speedtest,nodeid=001122334455 download_mbit=87.32,target="https://fsn1-speed.hetzner.com/100MB.bin" 1731000000000000000'
        $parsed = ConvertFrom-MeasurementOutput -RawOutput $raw

        $parsed | Should -Not -BeNullOrEmpty
        $parsed.NodeId | Should -Be '001122334455'
        $parsed.ThroughputMbit | Should -Be 87.32
        $parsed.Target | Should -Be 'https://fsn1-speed.hetzner.com/100MB.bin'
        $parsed.TimestampNs | Should -Be '1731000000000000000'
    }


    It 'parses valid line protocol when banner text is present' {
        $raw = @(
            'Freifunk Nordhessen e.V.'
            'Hostname: Test-Node'
            'speedtest,nodeid=aabbccddeeff download_mbit=10.75,target="https://fsn1-speed.hetzner.com/100MB.bin" 1772839860'
        ) -join "`n"
        $parsed = ConvertFrom-MeasurementOutput -RawOutput $raw

        $parsed | Should -Not -BeNullOrEmpty
        $parsed.NodeId | Should -Be 'aabbccddeeff'
        $parsed.ThroughputMbit | Should -Be 10.75
        $parsed.TimestampNs | Should -Be '1772839860'
    }
    It 'returns null for invalid payload' {
        $parsed = ConvertFrom-MeasurementOutput -RawOutput 'invalid payload'
        $parsed | Should -BeNullOrEmpty
    }


    It 'parses final failed speedtest markers with zero throughput' {
        $failed = ConvertFrom-MeasurementOutput -RawOutput 'wget_failed,nodeid=001122334455 exit=4 bytes=0 expected_bytes=104857600 target="https://fsn1-speed.hetzner.com/100MB.bin" 1772839860'
        $invalid = ConvertFrom-MeasurementOutput -RawOutput 'speedtest_invalid,nodeid=001122334455 bytes=0 sec=0 expected_bytes=104857600 target="https://fsn1-speed.hetzner.com/100MB.bin" 1772839860'
        $mismatch = ConvertFrom-MeasurementOutput -RawOutput 'speedtest_size_mismatch,nodeid=001122334455 bytes=104857599 expected_bytes=104857600 target="https://fsn1-speed.hetzner.com/100MB.bin" 1772839860'

        $failed.ResultType | Should -Be 'final_failed'
        $failed.FailureReason | Should -Be 'wget_failed'
        $failed.ThroughputMbit | Should -Be 0
        $invalid.FailureReason | Should -Be 'speedtest_invalid'
        $mismatch.FailureReason | Should -Be 'speedtest_size_mismatch'
    }
    It 'returns null for empty payload' {
        $parsed = ConvertFrom-MeasurementOutput -RawOutput ''
        $parsed | Should -BeNullOrEmpty
    }
}

Describe 'Resolve-NodeSourceFiles' {
    It 'finds Excel and CSV files recursively' {
        $nested = Join-Path $TestDrive 'a/b/c'
        New-Item -ItemType Directory -Path $nested -Force | Out-Null

        $xlsx = Join-Path $nested 'node_routerliste.xlsx'
        $csv = Join-Path $nested 'node_routerliste.csv'
        $txt = Join-Path $nested 'ignore.txt'

        Set-Content -Path $xlsx -Value 'x' -NoNewline
        Set-Content -Path $csv -Value 'x' -NoNewline
        Set-Content -Path $txt -Value 'x' -NoNewline

        $config = @{
            ExcelInputFiles       = @()
            ExcelInputDirectories = @($TestDrive)
            ExcelSearchRecurse    = $true
        }

        $result = @(Resolve-NodeSourceFiles -Config $config)
        $result | Should -Contain $xlsx
        $result | Should -Contain $csv
        $result | Should -Not -Contain $txt
    }
}

Describe 'Get-TestNodesFromConfig' {
    It 'builds deterministic test nodes from configured IPs' {
        $config = @{
            TestNodeIPs = @(
                '2a03:2260:3013:200:7a8a:20ff:fed0:747a'
                '2a03:2260:3013:200:1ae8:29ff:fe5c:1ff8'
                '[2a03:2260:3013:200:1ae8:29ff:fe5c:1ff8]'
            )
        }

        $result = Get-TestNodesFromConfig -Config $config
        $nodes = @($result.Nodes)

        $nodes.Count | Should -Be 2
        $nodes[0].DeviceID | Should -Be 'test-001'
        $nodes[1].DeviceID | Should -Be 'test-002'
        $nodes[0].Domain | Should -Be 'testing'
        $result.SourceFiles[0] | Should -Be '<test-node-ips>'
    }
}

Describe 'Convert-NodeTimestampToUtc' {
    It 'handles epoch seconds from nodes' {
        $utc = Convert-NodeTimestampToUtc -Timestamp '1772839860'
        $utc | Should -Be '2026-03-06T23:31:00.0000000Z'
    }

    It 'handles nanoseconds' {
        $utc = Convert-NodeTimestampToUtc -Timestamp '1772839860123456789'
        $utc | Should -Match '^2026-03-06T23:31:00\.123'
    }
}

Describe 'Wait-WithProgress' {
    It 'stops early when all nodes are finished' {
        InModuleScope FreifunkMetrics {
            $nodes = @(
                [pscustomobject]@{ DeviceID = 'node-001'; IP = '2a03:2260::1' }
                [pscustomobject]@{ DeviceID = 'node-002'; IP = '2a03:2260::2' }
            )

            Mock Get-Date { [datetime]'2026-03-08T12:00:00Z' }
            Mock Start-NodeResultCountPoll { [pscustomobject]@{ State = 'Completed' } }
            Mock Receive-NodeResultCountPoll { [pscustomobject]@{ PendingNodeKeys = @() } }
            Mock Stop-NodeResultCountPoll {}
            Mock Start-Sleep {}
            Mock Write-Progress {}
            Mock Update-ConsoleStatus {}

            Wait-WithProgress -Seconds 30 -Config @{} -RunId 'run-a' -Nodes $nodes -PollIntervalSeconds 1

            Assert-MockCalled Start-NodeResultCountPoll -Times 1 -Exactly
            Assert-MockCalled Receive-NodeResultCountPoll -Times 1 -Exactly
            Assert-MockCalled Start-Sleep -Times 0 -Exactly
        }
    }

    It 'keeps updating elapsed time while the poll is still running' {
        InModuleScope FreifunkMetrics {
            $nodes = @(
                [pscustomobject]@{ DeviceID = 'node-001'; IP = '2a03:2260::1' }
            )
            $timestamps = @(
                [datetime]'2026-03-08T12:00:00Z'
                [datetime]'2026-03-08T12:00:00Z'
                [datetime]'2026-03-08T12:00:01Z'
                [datetime]'2026-03-08T12:00:02Z'
                [datetime]'2026-03-08T12:00:03Z'
            )
            $script:dateIndex = 0

            Mock Get-Date {
                $current = $timestamps[[Math]::Min($script:dateIndex, $timestamps.Count - 1)]
                $script:dateIndex++
                $current
            }
            Mock Start-NodeResultCountPoll { [pscustomobject]@{ State = 'Running' } }
            Mock Receive-NodeResultCountPoll { throw 'should not be called' }
            Mock Stop-NodeResultCountPoll {}
            Mock Start-Sleep {}
            Mock Write-Progress {}
            Mock Update-ConsoleStatus {}

            Wait-WithProgress -Seconds 3 -Config @{} -RunId 'run-a' -Nodes $nodes -PollIntervalSeconds 1

            Assert-MockCalled Start-NodeResultCountPoll -Times 1 -Exactly
            Assert-MockCalled Start-Sleep -Times 3 -Exactly
            Assert-MockCalled Stop-NodeResultCountPoll -Times 1 -Exactly
            Assert-MockCalled Update-ConsoleStatus -Times 4 -ParameterFilter { $Message -match 'finished pending/1 nodes' }
            Assert-MockCalled Update-ConsoleStatus -Times 4
        }
    }

    It 'polls only the remaining nodes after a successful update' {
        InModuleScope FreifunkMetrics {
            $nodes = @(
                [pscustomobject]@{ DeviceID = 'node-001'; IP = '2a03:2260::1' }
                [pscustomobject]@{ DeviceID = 'node-002'; IP = '2a03:2260::2' }
            )
            $timestamps = @(
                [datetime]'2026-03-08T12:00:00Z'
                [datetime]'2026-03-08T12:00:00Z'
                [datetime]'2026-03-08T12:00:01Z'
                [datetime]'2026-03-08T12:00:02Z'
            )
            $script:dateIndex = 0
            $script:pollIndex = 0

            Mock Get-Date {
                $current = $timestamps[[Math]::Min($script:dateIndex, $timestamps.Count - 1)]
                $script:dateIndex++
                $current
            }
            Mock Start-NodeResultCountPoll {
                if ($script:pollIndex -eq 0) {
                    $script:pollIndex++
                    [pscustomobject]@{ State = 'Completed'; Id = 'first' }
                }
                else {
                    [pscustomobject]@{ State = 'Running'; Id = 'second' }
                }
            }
            Mock Receive-NodeResultCountPoll {
                [pscustomobject]@{ PendingNodeKeys = @((Get-NodeProgressKey -Node $nodes[1])) }
            }
            Mock Stop-NodeResultCountPoll {}
            Mock Start-Sleep {}
            Mock Write-Progress {}
            Mock Update-ConsoleStatus {}

            Wait-WithProgress -Seconds 2 -Config @{} -RunId 'run-a' -Nodes $nodes -PollIntervalSeconds 1

            Assert-MockCalled Start-NodeResultCountPoll -Times 1 -Exactly -ParameterFilter { @($Nodes).Count -eq 2 }
            Assert-MockCalled Start-NodeResultCountPoll -Times 1 -Exactly -ParameterFilter { @($Nodes).Count -eq 1 }
        }
    }

    It 'keeps the last finished count when a later poll fails' {
        InModuleScope FreifunkMetrics {
            $nodes = @(
                [pscustomobject]@{ DeviceID = 'node-001'; IP = '2a03:2260::1' }
                [pscustomobject]@{ DeviceID = 'node-002'; IP = '2a03:2260::2' }
            )
            $timestamps = @(
                [datetime]'2026-03-08T12:00:00Z'
                [datetime]'2026-03-08T12:00:00Z'
                [datetime]'2026-03-08T12:00:01Z'
                [datetime]'2026-03-08T12:00:02Z'
            )
            $script:dateIndex = 0
            $script:pollIndex = 0

            Mock Get-Date {
                $current = $timestamps[[Math]::Min($script:dateIndex, $timestamps.Count - 1)]
                $script:dateIndex++
                $current
            }
            Mock Start-NodeResultCountPoll {
                if ($script:pollIndex -eq 0) {
                    $script:pollIndex++
                    [pscustomobject]@{ State = 'Completed'; Id = 'first' }
                }
                else {
                    [pscustomobject]@{ State = 'Completed'; Id = 'second' }
                }
            }
            Mock Receive-NodeResultCountPoll {
                param($Job)

                if ($Job.Id -eq 'first') {
                    return [pscustomobject]@{ PendingNodeKeys = @((Get-NodeProgressKey -Node $nodes[1])) }
                }

                return $null
            }
            Mock Stop-NodeResultCountPoll {}
            Mock Start-Sleep {}
            Mock Write-Progress {}
            Mock Update-ConsoleStatus {}

            Wait-WithProgress -Seconds 2 -Config @{} -RunId 'run-a' -Nodes $nodes -PollIntervalSeconds 2

            Assert-MockCalled Receive-NodeResultCountPoll -Times 2 -Exactly
            Assert-MockCalled Update-ConsoleStatus -Times 3 -ParameterFilter { $Message -match 'finished 1/2 nodes' }
        }
    }
}
Describe 'Receive-NodeResultCountPoll' {
    It 'returns poll results and removes the finished job' {
        InModuleScope FreifunkMetrics {
            $job = [pscustomobject]@{ Id = 42 }
            $expected = [pscustomobject]@{ PendingNodeKeys = @('node-002|2a03:2260::2||') }

            Mock Receive-Job {
                param($Job, [switch]$AutoRemoveJob)

                if ($AutoRemoveJob) {
                    throw 'AutoRemoveJob must not be used here'
                }

                return $expected
            }
            Mock Remove-Job {}

            $result = Receive-NodeResultCountPoll -Job $job

            @($result.PendingNodeKeys) | Should -Be @('node-002|2a03:2260::2||')
            Assert-MockCalled Receive-Job -Times 1 -Exactly
            Assert-MockCalled Remove-Job -Times 1 -Exactly
        }
    }
}

Describe 'Test-NodeReleaseSupported' {
    It 'accepts 1.5.0 and newer releases' {
        Test-NodeReleaseSupported -Release '1.5.0' | Should -BeTrue
        Test-NodeReleaseSupported -Release '1.5.1' | Should -BeTrue
        Test-NodeReleaseSupported -Release 'v1.6.0' | Should -BeTrue
    }

    It 'rejects empty, invalid, and older releases' {
        Test-NodeReleaseSupported -Release '' | Should -BeFalse
        Test-NodeReleaseSupported -Release '1.4.9' | Should -BeFalse
        Test-NodeReleaseSupported -Release 'snapshot' | Should -BeFalse
    }
}

Describe 'Import-NodeListFromExcel' {
    It 'imports only nodes with IP and release 1.5.0 or newer from csv' {
        $csvPath = Join-Path $TestDrive 'node_routerliste.csv'
        @(
            'DeviceID,Type,Owner,District,Location,LocalContactName,LocalContactPhone,LocalContactMail,Notes,Name,MapLink,IP,Outdoor,Domain,VPNMesh,Speedlimit,Branch,Autoupdater,SSHKeys,Release,VLAN,Backup'
            'node-001,,,,,,,,,Node 1,,2a03:2260::1,,dom-a,,,,,,1.5.0,,'
            'node-002,,,,,,,,,Node 2,,2a03:2260::2,,dom-a,,,,,,1.4.9,,'
            'node-003,,,,,,,,,Node 3,,,,dom-a,,,,,,1.5.2,,'
            'node-004,,,,,,,,,Node 4,,2a03:2260::4,,dom-b,,,,,,,,'
            'node-005,,,,,,,,,Node 5,,2a03:2260::5,,dom-b,,,,,,v1.6.0,,'
        ) | Set-Content -Path $csvPath

        $config = @{
            ExcelInputFiles       = @($csvPath)
            ExcelInputDirectories = @()
            ExcelSearchRecurse    = $false
        }

        $result = Import-NodeListFromExcel -Config $config
        $nodes = @($result.Nodes)

        $nodes.Count | Should -Be 2
        $nodes.DeviceID | Should -Contain 'node-001'
        $nodes.DeviceID | Should -Contain 'node-005'
        $nodes.DeviceID | Should -Not -Contain 'node-002'
        $nodes.DeviceID | Should -Not -Contain 'node-003'
        $nodes.DeviceID | Should -Not -Contain 'node-004'
    }
}
Describe 'Get-NodeTriggerAssignments' {
    It 'spreads unknown and slow nodes across the beginning of the delay window' {
        InModuleScope FreifunkMetrics {
            $config = @{ TriggerRandomDelayMaxSeconds = 10 }
            $nodes = @(
                [pscustomobject]@{ DeviceID = 'node-001'; Name = 'Node 1'; IP = '2a03:2260::1'; Domain = 'dom-a' }
                [pscustomobject]@{ DeviceID = 'node-002'; Name = 'Node 2'; IP = '2a03:2260::2'; Domain = 'dom-a' }
                [pscustomobject]@{ DeviceID = 'node-003'; Name = 'Node 3'; IP = '2a03:2260::3'; Domain = 'dom-a' }
                [pscustomobject]@{ DeviceID = 'node-004'; Name = 'Node 4'; IP = '2a03:2260::4'; Domain = 'dom-a' }
            )

            Mock Get-LatestThroughputByIp {
                @{
                    '2a03:2260::2' = 0.0
                    '2a03:2260::3' = 20.0
                    '2a03:2260::4' = 80.0
                }
            }

            $assigned = @(Get-NodeTriggerAssignments -Config $config -RunId 'run-a' -Nodes $nodes)
            $delayByDeviceId = @{}
            foreach ($item in $assigned) {
                $delayByDeviceId[$item.Node.DeviceID] = $item.AssignedDelaySeconds
            }

            $delayByDeviceId['node-001'] | Should -Be 0
            $delayByDeviceId['node-002'] | Should -Be 3
            $delayByDeviceId['node-003'] | Should -Be 7
            $delayByDeviceId['node-004'] | Should -Be 10
        }
    }

    It 'does not assign all-unknown nodes to delay 0' {
        InModuleScope FreifunkMetrics {
            $config = @{ TriggerRandomDelayMaxSeconds = 10 }
            $nodes = @(
                [pscustomobject]@{ DeviceID = 'node-001'; Name = 'Node 1'; IP = '2a03:2260::1'; Domain = 'dom-a' }
                [pscustomobject]@{ DeviceID = 'node-002'; Name = 'Node 2'; IP = '2a03:2260::2'; Domain = 'dom-a' }
                [pscustomobject]@{ DeviceID = 'node-003'; Name = 'Node 3'; IP = '2a03:2260::3'; Domain = 'dom-a' }
                [pscustomobject]@{ DeviceID = 'node-004'; Name = 'Node 4'; IP = '2a03:2260::4'; Domain = 'dom-a' }
            )

            Mock Get-LatestThroughputByIp { @{} }

            $assignedDelays = @(Get-NodeTriggerAssignments -Config $config -RunId 'run-a' -Nodes $nodes | ForEach-Object { $_.AssignedDelaySeconds } | Sort-Object)

            $assignedDelays | Should -Be @(0, 3, 7, 10)
        }
    }
}
Describe 'Invoke-NodeTriggerBatch' {
    It 'triggers lower assigned delays first' {
        InModuleScope FreifunkMetrics {
            $config = @{ TriggerParallelism = 1 }
            $nodes = @(
                [pscustomobject]@{ DeviceID = 'node-001'; Name = 'Node 1'; IP = '2a03:2260::1'; Domain = 'dom-a' }
                [pscustomobject]@{ DeviceID = 'node-002'; Name = 'Node 2'; IP = '2a03:2260::2'; Domain = 'dom-a' }
                [pscustomobject]@{ DeviceID = 'node-003'; Name = 'Node 3'; IP = '2a03:2260::3'; Domain = 'dom-a' }
            )

            Mock Get-NodeTriggerAssignments {
                @(
                    [pscustomobject]@{ Index = 2; Node = $nodes[2]; AssignedDelaySeconds = 10 }
                    [pscustomobject]@{ Index = 0; Node = $nodes[0]; AssignedDelaySeconds = 0 }
                    [pscustomobject]@{ Index = 1; Node = $nodes[1]; AssignedDelaySeconds = 5 }
                )
            }

            Mock Get-NodeTriggerCommandInfo {
                param($Config, $RunId, $AssignedDelaySeconds)

                [pscustomobject]@{
                    RemoteResultFile     = ''
                    RemoteErrorFile      = ''
                    TriggerCommand       = ''
                    AssignedDelaySeconds = $AssignedDelaySeconds
                }
            }

            Mock Invoke-NodeTriggerCommand {
                param($Config, $Node, $RunId, $AssignedDelaySeconds)

                [pscustomobject]@{
                    Reachable            = $true
                    Triggered            = $true
                    RemoteResultFile     = ''
                    RemoteErrorFile      = ''
                    AssignedDelaySeconds = $AssignedDelaySeconds
                    Error                = ''
                }
            }

            $result = @(Invoke-NodeTriggerBatch -Config $config -Nodes $nodes -RunId 'run-a')

            @($result | ForEach-Object { $_.Node.DeviceID }) | Should -Be @('node-001', 'node-002', 'node-003')
            @($result | ForEach-Object { $_.TriggerResult.AssignedDelaySeconds }) | Should -Be @(0, 5, 10)
        }
    }
}
Describe 'Get-NodeTriggerCommandInfo' {
    It 'uses controller-assigned delay and target settings' {
        $config = @{
            RemoteResultDir = '/tmp/harvester'
            TriggerRandomDelayMaxSeconds = 42
            SpeedtestTargetUrl = 'https://example.invalid/testfile.bin'
            SpeedtestTargetBytes = 123456789
        }

        $info = Get-NodeTriggerCommandInfo -Config $config -RunId 'run-trigger' -AssignedDelaySeconds 17
        $info.RemoteResultFile | Should -Be '/tmp/harvester/run-trigger/*.txt'
        $info.AssignedDelaySeconds | Should -Be 17

        $info.TriggerCommand | Should -Match 'delay_seconds=17'
        $info.TriggerCommand | Should -Match 'sleep "\$delay_seconds"'
        $info.TriggerCommand | Should -Not -Match 'run_id='
        $info.TriggerCommand | Should -Not -Match 'delay_seed='
        $info.TriggerCommand | Should -Not -Match 'cksum'
        $info.TriggerCommand | Should -Not -Match 'srand\('
        $info.TriggerCommand | Should -Match 'target_url=''https://example\.invalid/testfile\.bin'''
        $info.TriggerCommand | Should -Match 'wget_exit_file='
        $info.TriggerCommand | Should -Match 'wc -c'
        $info.TriggerCommand | Should -Match 'expected_bytes="?123456789"?'
        $info.TriggerCommand | Should -Match 'speedtest_size_mismatch,nodeid='
    }
}



Describe 'SSH streaming integration' -Tag 'ssh-streaming' {
    It 'streams a remote measurement file from one configured test node over SSH' {
        $configPath = $env:FFMH_TEST_CONFIG_PATH
        if ([string]::IsNullOrWhiteSpace($configPath)) {
            throw 'FFMH_TEST_CONFIG_PATH is required for ssh-streaming tests.'
        }

        $config = Get-EnvironmentConfig -RequestedPath $configPath
        $nodeResult = Get-TestNodesFromConfig -Config $config
        $nodes = @($nodeResult.Nodes)
        if ($nodes.Count -eq 0) {
            throw 'No TestNodeIPs configured for ssh-streaming test.'
        }

        $rawDir = Join-Path $TestDrive 'ssh-streaming'
        New-Item -ItemType Directory -Path $rawDir -Force | Out-Null

        $success = $false
        $failures = New-Object System.Collections.Generic.List[string]

        foreach ($node in $nodes) {
            $collectConfig = @{} + $config
            $collectConfig.RemoteResultDir = ('{0}/pester-ssh-streaming-base-{1}' -f $config.RemoteResultDir.TrimEnd('/'), ([guid]::NewGuid().ToString('N')))
            $remoteRunDir = Get-RemoteRunResultDir -Config $collectConfig -RunId 'run-ssh-streaming'
            $remoteFile = ('{0}/result.txt' -f $remoteRunDir)
            $remoteDirEscaped = Convert-ToShellSingleQuoted -Value $remoteRunDir
            $remoteFileEscaped = Convert-ToShellSingleQuoted -Value $remoteFile
            $payload = 'speedtest,nodeid=pester download_mbit=12.34,target="https://example.invalid/test.bin" 1772839860'
            $payloadEscaped = Convert-ToShellSingleQuoted -Value $payload
            $sshArgs = New-SshArgs -Config $collectConfig -NodeIp $node.IP

            try {
                $prepareCmd = "mkdir -p '$remoteDirEscaped'; printf '%s\n' '$payloadEscaped' > '$remoteFileEscaped'"
                $prepareOutput = & $collectConfig.SshBinary @sshArgs $prepareCmd 2>&1
                if ($LASTEXITCODE -ne 0) {
                    throw "prepare failed: $($prepareOutput -join ' ')"
                }

                $result = Receive-NodeResults -Config $collectConfig -Node $node -RunId 'run-ssh-streaming' -RawDir $rawDir
                $files = @($result.Files)
                if (-not $result.Success) {
                    throw "collect failed: $($result.ErrorOutput)"
                }

                $files.Count | Should -Be 1
                $files[0].RemotePath | Should -Be $remoteFile
                $files[0].ParsedMeasurement.ResultType | Should -Be 'success'
                $files[0].ParsedMeasurement.NodeId | Should -Be 'pester'
                $files[0].RawOutput | Should -Match '^speedtest,nodeid=pester'
                $success = $true
                break
            }
            catch {
                $failures.Add(($node.IP + ': ' + $_.Exception.Message))
            }
            finally {
                $cleanupCmd = "rm -f '$remoteFileEscaped'; rmdir '$remoteDirEscaped' >/dev/null 2>&1 || true"
                & $collectConfig.SshBinary @sshArgs $cleanupCmd 2>$null | Out-Null
            }
        }

        if (-not $success) {
            throw ('No configured test node completed the ssh-streaming test. ' + ($failures -join ' | '))
        }
    }
}
Describe 'Speedtest target integration' {
    It 'downloads the configured speedtest file with the expected length' -Tag 'integration' {
        $config = @{
            SpeedtestTargetUrl   = 'https://fsn1-speed.hetzner.com/100MB.bin'
            SpeedtestTargetBytes = 104857600
        }

        $downloadPath = Join-Path $TestDrive 'speedtest-target.bin'

        try {
            Invoke-WebRequest `
                -Uri $config.SpeedtestTargetUrl `
                -OutFile $downloadPath `
                -MaximumRetryCount 2 `
                -RetryIntervalSec 5 `
                -TimeoutSec 300 | Out-Null

            (Get-Item -Path $downloadPath).Length | Should -Be $config.SpeedtestTargetBytes
        }
        finally {
            if (Test-Path -Path $downloadPath) {
                Remove-Item -Path $downloadPath -Force
            }
        }
    }
}

Describe 'Assert-ValidConfig' {
    It 'normalizes valid config values' {
        $config = @{
            SshUser = ' root '
            SshBinary = ' ssh '
            SQLiteBinary = ' sqlite3 '
            RemoteResultDir = ' /tmp/harvester '
            LogFilePrefix = ' collect-node-metrics '
            SpeedtestTargetUrl = ' https://example.invalid/testfile.bin '
            SshConnectTimeoutSeconds = '8'
            TriggerParallelism = '10'
            CollectParallelism = '4'
            TriggerRandomDelayMaxSeconds = '600'
            SpeedtestTargetBytes = '104857600'
            ExcelInputFiles = @('nodes.csv', '', $null)
            ExcelInputDirectories = @('/tmp/nodes', ' ')
            ExcelSearchRecurse = $true
            UseTestNodeIPs = $false
            TestNodeIPs = @('2a03:2260::1', '')
        }

        Assert-ValidConfig -Config $config

        $config.RemoteResultDir | Should -Be '/tmp/harvester'
        $config.LogFilePrefix | Should -Be 'collect-node-metrics'
        $config.SpeedtestTargetUrl | Should -Be 'https://example.invalid/testfile.bin'
        $config.TriggerParallelism | Should -Be 10
        $config.CollectParallelism | Should -Be 4
        $config.TriggerRandomDelayMaxSeconds | Should -Be 600
        $config.SpeedtestTargetBytes | Should -Be 104857600
        $config.ExcelInputFiles | Should -Be @('nodes.csv')
        $config.ExcelInputDirectories | Should -Be @('/tmp/nodes')
        $config.TestNodeIPs | Should -Be @('2a03:2260::1')
    }

    It 'rejects invalid speedtest url schemes' {
        $config = @{
            SshUser = 'root'
            SshBinary = 'ssh'
            SQLiteBinary = 'sqlite3'
            RemoteResultDir = '/tmp/harvester'
            LogFilePrefix = 'collect-node-metrics'
            SpeedtestTargetUrl = 'ftp://example.invalid/testfile.bin'
            SshConnectTimeoutSeconds = 8
            TriggerParallelism = 10
            CollectParallelism = 4
            TriggerRandomDelayMaxSeconds = 600
            SpeedtestTargetBytes = 104857600
            ExcelInputFiles = @()
            ExcelInputDirectories = @()
            ExcelSearchRecurse = $true
            UseTestNodeIPs = $false
            TestNodeIPs = @()
        }

        { Assert-ValidConfig -Config $config } | Should -Throw 'Config value SpeedtestTargetUrl must use http or https.'
    }

    It 'rejects non-positive collect parallelism' {
        $config = @{
            SshUser = 'root'
            SshBinary = 'ssh'
            SQLiteBinary = 'sqlite3'
            RemoteResultDir = '/tmp/harvester'
            LogFilePrefix = 'collect-node-metrics'
            SpeedtestTargetUrl = 'https://example.invalid/testfile.bin'
            SshConnectTimeoutSeconds = 8
            TriggerParallelism = 10
            CollectParallelism = 0
            TriggerRandomDelayMaxSeconds = 600
            SpeedtestTargetBytes = 104857600
            ExcelInputFiles = @()
            ExcelInputDirectories = @()
            ExcelSearchRecurse = $true
            UseTestNodeIPs = $false
            TestNodeIPs = @()
        }

        { Assert-ValidConfig -Config $config } | Should -Throw 'Config value CollectParallelism must be at least 1.'
    }
}

Describe 'Convert-CollectStreamToFiles' {
    It 'parses multiple streamed remote files' {
        $stream = @(
            '__FFMH_FILE_BEGIN__/tmp/harvester/1.txt'
            'speedtest,nodeid=aabbcc download_mbit=12.34,target="https://example.invalid/test.bin" 123'
            '__FFMH_FILE_END__/tmp/harvester/1.txt'
            '__FFMH_FILE_BEGIN__/tmp/harvester/2.txt'
            'pending output'
            '__FFMH_FILE_END__/tmp/harvester/2.txt'
        ) -join "`n"

        $files = @(Convert-CollectStreamToFiles -RawOutput $stream)

        $files.Count | Should -Be 2
        $files[0].RemotePath | Should -Be '/tmp/harvester/1.txt'
        $files[0].RawOutput | Should -Match '^speedtest,nodeid=aabbcc'
        $files[1].RemotePath | Should -Be '/tmp/harvester/2.txt'
        $files[1].RawOutput | Should -Be 'pending output'
    }
}

Describe 'Receive-NodeResults' {
    It 'keeps downloaded measurements when remote delete fails' {
        $mockSsh = Join-Path $TestDrive 'mock-ssh-delete-fail.ps1'
        @(
            '$command = $args[-1]'
            'if ($command -like ''find*'') {'
            '    @(' 
            '        ''__FFMH_FILE_BEGIN__/tmp/harvester/1700000000.txt'''
            '        ''speedtest,nodeid=aabbccddeeff download_mbit=48.25,target="https://example.invalid/test.bin" 1700000000000000000'''
            '        ''__FFMH_FILE_END__/tmp/harvester/1700000000.txt'''
            '    ) | ForEach-Object { Write-Output $_ }'
            '    exit 0'
            '}'
            'if ($command -like ''rm -f*'') {'
            '    Write-Output ''simulated delete failure'''
            '    exit 7'
            '}'
            'Write-Output ("unexpected command: {0}" -f $command)'
            'exit 9'
        ) | Set-Content -Path $mockSsh

        $rawDir = Join-Path $TestDrive 'raw-delete-fail'
        New-Item -ItemType Directory -Path $rawDir -Force | Out-Null

        $config = @{
            SshBinary = $mockSsh
            SshKeyPath = 'ignore'
            SshUser = 'root'
            SshConnectTimeoutSeconds = 1
            RemoteResultDir = '/tmp/harvester'
            CollectParallelism = 2
        }
        $node = [pscustomobject]@{ DeviceID = 'node-001'; Name = 'Node 1'; IP = '2a03:2260::1'; Domain = 'dom-a' }

        $result = Receive-NodeResults -Config $config -Node $node -RunId 'run-test' -RawDir $rawDir
        $files = @($result.Files)

        $result.Success | Should -BeTrue
        $files.Count | Should -Be 1
        $result.ErrorOutput | Should -Match 'delete failed'
        (Test-Path -Path $files[0].LocalPath -PathType Leaf) | Should -BeTrue
        $files[0].RawOutput | Should -Match '^speedtest,nodeid=aabbccddeeff'
    }

    It 'treats final failed results as collected measurements' {
        $mockSsh = Join-Path $TestDrive 'mock-ssh-final-failed.ps1'
        @(
            '$command = $args[-1]'
            'if ($command -like ''find*'') {'
            '    @('
            '        ''__FFMH_FILE_BEGIN__/tmp/harvester/1700000000.txt'''
            '        ''wget_failed,nodeid=aabbccddeeff exit=4 bytes=0 expected_bytes=104857600 target="https://example.invalid/test.bin" 1700000000000000000'''
            '        ''__FFMH_FILE_END__/tmp/harvester/1700000000.txt'''
            '    ) | ForEach-Object { Write-Output $_ }'
            '    exit 0'
            '}'
            'if ($command -like ''rm -f*'') {'
            '    exit 0'
            '}'
            'Write-Output ("unexpected command: {0}" -f $command)'
            'exit 9'
        ) | Set-Content -Path $mockSsh

        $rawDir = Join-Path $TestDrive 'raw-final-failed'
        New-Item -ItemType Directory -Path $rawDir -Force | Out-Null

        $config = @{
            SshBinary = $mockSsh
            SshKeyPath = 'ignore'
            SshUser = 'root'
            SshConnectTimeoutSeconds = 1
            RemoteResultDir = '/tmp/harvester'
            CollectParallelism = 2
        }
        $node = [pscustomobject]@{ DeviceID = 'node-001'; Name = 'Node 1'; IP = '2a03:2260::1'; Domain = 'dom-a' }

        $result = Receive-NodeResults -Config $config -Node $node -RunId 'run-test' -RawDir $rawDir
        $files = @($result.Files)

        $result.Success | Should -BeTrue
        $files.Count | Should -Be 1
        $files[0].ParsedMeasurement.ResultType | Should -Be 'final_failed'
        $files[0].ParsedMeasurement.ThroughputMbit | Should -Be 0
    }
}


Describe 'Get-RemoteRunResultDir' {
    It 'builds a run-specific remote directory' {
        $config = @{ RemoteResultDir = '/tmp/harvester' }

        Get-RemoteRunResultDir -Config $config -RunId 'run-20260307-123456' | Should -Be '/tmp/harvester/run-20260307-123456'
    }
}
Describe 'Get-FinishedNodeResultCountBatch' {
    It 'counts nodes with finished remote result files' {
        $mockSsh = Join-Path $TestDrive 'mock-ssh-ready.ps1'
        @(
            '$command = $args[-1]'
            '$nodeHost = $args[-2]'
            'if ($command -like ''find*'') {'
            '    if ($nodeHost -like ''*::1'') { Write-Output ''/tmp/harvester/run-ready/1700000001.txt'' }'
            '    exit 0'
            '}'
            'Write-Output (''unexpected command: {0}'' -f $command)'
            'exit 9'
        ) | Set-Content -Path $mockSsh

        $config = @{
            SshBinary = $mockSsh
            SshKeyPath = 'ignore'
            SshUser = 'root'
            SshConnectTimeoutSeconds = 1
            RemoteResultDir = '/tmp/harvester'
            CollectParallelism = 2
        }
        $nodes = @(
            [pscustomobject]@{ DeviceID = 'node-001'; Name = 'Node 1'; IP = '2a03:2260::1'; Domain = 'dom-a' }
            [pscustomobject]@{ DeviceID = 'node-002'; Name = 'Node 2'; IP = '2a03:2260::2'; Domain = 'dom-b' }
        )

        Get-FinishedNodeResultCountBatch -Config $config -RunId 'run-ready' -Nodes $nodes | Should -Be 1
    }
}

Describe 'Invoke-NodeCollectBatch' {
    It 'collects multiple nodes in parallel' {
        $mockSsh = Join-Path $TestDrive 'mock-ssh-batch.ps1'
        @(
            '$command = $args[-1]'
            '$nodeHost = $args[-2]'
            'if ($command -like ''find*'') {'
            '    $suffix = if ($nodeHost -like ''*::1'') { ''1'' } else { ''2'' }'
            '    @('
            '        "__FFMH_FILE_BEGIN__/tmp/harvester/170000000$suffix.txt"'
            '        "speedtest,nodeid=node$suffix download_mbit=40.$suffix,target=`"https://example.invalid/test.bin`" 170000000000000000$suffix"'
            '        "__FFMH_FILE_END__/tmp/harvester/170000000$suffix.txt"'
            '    ) | ForEach-Object { Write-Output $_ }'
            '    exit 0'
            '}'
            'if ($command -like ''rm -f*'') {'
            '    exit 0'
            '}'
            'Write-Output ("unexpected command: {0}" -f $command)'
            'exit 9'
        ) | Set-Content -Path $mockSsh

        $rawDir = Join-Path $TestDrive 'raw-batch'
        New-Item -ItemType Directory -Path $rawDir -Force | Out-Null

        $config = @{
            SshBinary = $mockSsh
            SshKeyPath = 'ignore'
            SshUser = 'root'
            SshConnectTimeoutSeconds = 1
            RemoteResultDir = '/tmp/harvester'
            CollectParallelism = 2
        }
        $nodes = @(
            [pscustomobject]@{ DeviceID = 'node-001'; Name = 'Node 1'; IP = '2a03:2260::1'; Domain = 'dom-a' }
            [pscustomobject]@{ DeviceID = 'node-002'; Name = 'Node 2'; IP = '2a03:2260::2'; Domain = 'dom-b' }
        )

        $results = @(Invoke-NodeCollectBatch -Config $config -Nodes $nodes -RunId 'run-batch' -RawDir $rawDir)
        $sorted = @($results | Sort-Object { $_.Node.DeviceID })

        $sorted.Count | Should -Be 2
        $sorted[0].Node.DeviceID | Should -Be 'node-001'
        $sorted[0].CollectResult.Success | Should -BeTrue
        @($sorted[0].CollectResult.Files).Count | Should -Be 1
        $sorted[1].Node.DeviceID | Should -Be 'node-002'
        $sorted[1].CollectResult.Success | Should -BeTrue
        @($sorted[1].CollectResult.Files).Count | Should -Be 1
    }
}

