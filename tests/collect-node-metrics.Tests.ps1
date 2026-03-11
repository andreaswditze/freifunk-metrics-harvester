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
                RawOutput = 'speedtest,nodeid=aa download_mbit=10.5 bytes=104857600 sec=79.891561 timeout_seconds=180,target="https://example.invalid/test.bin" 1772839860'
                ParsedMeasurement = [pscustomobject]@{ ResultType = 'success'; NodeId = 'aa'; ThroughputMbit = 10.5; DownloadDurationSeconds = 79.891561; DownloadedBytes = 104857600; TimeoutSeconds = 180 }
            }
            $failedFile = [pscustomobject]@{
                LocalPath = 'failed.txt'
                RawOutput = 'wget_failed,nodeid=bb exit=4 bytes=0 sec=12.500000 expected_bytes=104857600 timeout_seconds=180 target="https://example.invalid/test.bin" 1772839860'
                ParsedMeasurement = [pscustomobject]@{ ResultType = 'final_failed'; NodeId = 'bb'; FailureReason = 'wget_failed'; ThroughputMbit = 0; DownloadDurationSeconds = 12.5; DownloadedBytes = 0; ExpectedBytes = 104857600; TimeoutSeconds = 180; WgetExitCode = 4 }
            }

            Mock Show-StartupBanner {}
            Mock Update-ConsoleStatus {}
            Mock Write-Progress {}
            $script:HostMessages = New-Object System.Collections.Generic.List[string]
            Mock Write-Host {
                param($Object)

                $script:HostMessages.Add([string]$Object)
            }
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
                @($script:HostMessages) | Should -Contain 'Node delivery summary: successful=1, failed=2'
                @($script:HostMessages | Where-Object { $_ -like 'Failed node reasons:*' }) | Should -HaveCount 1
                @($script:HostMessages | Where-Object { $_ -like 'Failed node reasons:*' })[0] | Should -Match 'download_failed=1'
                @($script:HostMessages | Where-Object { $_ -like 'Failed node reasons:*' })[0] | Should -Match 'not_reachable=1'
                @($script:HostMessages) | Should -Contain ' - Node 2 (2a03:2260::2): download_failed [final; wget_failed]'
                @($script:HostMessages) | Should -Contain ' - Node 3 (2a03:2260::3): not_reachable [trigger; ssh failed]'
            }
            finally {
                $script:CurrentConfig = $null
                $script:LogFilePath = $null
                $script:DailyLogDir = $null
                $script:DailyLogFilePath = $null
                $script:ConsoleStatusLength = 0
                $script:ConsoleBannerShown = $false
                $script:HostMessages = $null
            }
        }
    }

    It 'does not print failure reasons when all nodes succeeded' {
        InModuleScope FreifunkMetrics {
            $baseDir = Join-Path $TestDrive 'runner-main-success-only'
            $nodeA = [pscustomobject]@{ DeviceID = 'node-010'; Name = 'Node 10'; IP = '2a03:2260::10'; Domain = 'dom-a' }
            $nodeB = [pscustomobject]@{ DeviceID = 'node-011'; Name = 'Node 11'; IP = '2a03:2260::11'; Domain = 'dom-b' }

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

            $fileA = [pscustomobject]@{
                LocalPath = 'success-a.txt'
                RawOutput = 'speedtest,nodeid=aa download_mbit=20.1 bytes=104857600 sec=41.734866 timeout_seconds=180,target="https://example.invalid/test.bin" 1772839860'
                ParsedMeasurement = [pscustomobject]@{ ResultType = 'success'; NodeId = 'aa'; ThroughputMbit = 20.1; DownloadDurationSeconds = 41.734866; DownloadedBytes = 104857600; TimeoutSeconds = 180 }
            }
            $fileB = [pscustomobject]@{
                LocalPath = 'success-b.txt'
                RawOutput = 'speedtest,nodeid=bb download_mbit=22.3 bytes=104857600 sec=37.616179 timeout_seconds=180,target="https://example.invalid/test.bin" 1772839860'
                ParsedMeasurement = [pscustomobject]@{ ResultType = 'success'; NodeId = 'bb'; ThroughputMbit = 22.3; DownloadDurationSeconds = 37.616179; DownloadedBytes = 104857600; TimeoutSeconds = 180 }
            }

            Mock Show-StartupBanner {}
            Mock Update-ConsoleStatus {}
            Mock Write-Progress {}
            $script:HostMessages = New-Object System.Collections.Generic.List[string]
            Mock Write-Host {
                param($Object)

                $script:HostMessages.Add([string]$Object)
            }
            Mock Write-Log {}
            Mock Initialize-Database {}
            Mock Get-EnvironmentConfig { $config }
            Mock Import-NodeListFromExcel { [pscustomobject]@{ Nodes = @($nodeA, $nodeB); SourceFiles = @('nodes.csv') } }
            Mock Start-MeasurementRun { [pscustomobject]@{ RawDir = (Join-Path $TestDrive 'raw-run-success-only') } }
            Mock Write-NodeActionLog {}
            Mock Add-NodeJobRecord {}
            Mock Wait-WithProgress {}
            Mock Save-Measurement {}
            Mock Complete-MeasurementRun {}
            Mock Invoke-NodeTriggerBatch {
                @(
                    [pscustomobject]@{ Node = $nodeA; TriggerResult = [pscustomobject]@{ Triggered = $true; RemoteResultFile = '/tmp/node-010.txt'; RemoteErrorFile = '/tmp/node-010.err'; AssignedDelaySeconds = 0 } }
                    [pscustomobject]@{ Node = $nodeB; TriggerResult = [pscustomobject]@{ Triggered = $true; RemoteResultFile = '/tmp/node-011.txt'; RemoteErrorFile = '/tmp/node-011.err'; AssignedDelaySeconds = 0 } }
                )
            }
            Mock Invoke-NodeCollectBatch {
                @(
                    [pscustomobject]@{ Node = $nodeA; CollectResult = [pscustomobject]@{ Success = $true; ErrorOutput = ''; Files = @($fileA); PendingFiles = @() } }
                    [pscustomobject]@{ Node = $nodeB; CollectResult = [pscustomobject]@{ Success = $true; ErrorOutput = ''; Files = @($fileB); PendingFiles = @() } }
                )
            }

            try {
                Invoke-CollectNodeMetricsMain -RunId 'run-success-only'

                @($script:HostMessages) | Should -Contain 'Node delivery summary: successful=2, failed=0'
                @($script:HostMessages | Where-Object { $_ -like 'Failed node reasons:*' }) | Should -HaveCount 0
            }
            finally {
                $script:CurrentConfig = $null
                $script:LogFilePath = $null
                $script:DailyLogDir = $null
                $script:DailyLogFilePath = $null
                $script:ConsoleStatusLength = 0
                $script:ConsoleBannerShown = $false
                $script:HostMessages = $null
            }
        }
    }

    It 'stores diagnostics for successful high-throughput nodes' {
        InModuleScope FreifunkMetrics {
            $baseDir = Join-Path $TestDrive 'runner-main-diagnostics'
            $node = [pscustomobject]@{ DeviceID = 'node-020'; Name = 'Node 20'; IP = '2a03:2260::20'; Domain = 'dom-a' }

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
                TriggerParallelism = 1
                CollectParallelism = 1
                TriggerRandomDelayMaxSeconds = 0
                SpeedtestTargetBytes = 104857600
            }

            $measurementFile = [pscustomobject]@{
                LocalPath = 'success-fast.txt'
                RawOutput = 'speedtest,nodeid=cc download_mbit=48.7 bytes=104857600 sec=17.208000 timeout_seconds=180,target="https://example.invalid/test.bin" 1772839860'
                ParsedMeasurement = [pscustomobject]@{ ResultType = 'success'; NodeId = 'cc'; ThroughputMbit = 48.7; DownloadDurationSeconds = 17.208; DownloadedBytes = 104857600; TimeoutSeconds = 180 }
            }
            $diagnosticFile = [pscustomobject]@{
                LocalPath = 'diag-fast.txt'
                RawOutput = @(
                    'diagnostic,nodeid=cc target_host="example.invalid" speedtest_delay_seconds=30 diagnostic_delay_seconds=90 timestamp=1772839800'
                    'diag_summary,load1=0.10 load5=0.20 load15=0.30 gateway_probe="fe80::1" gateway_probe_kind="ipv6" ping_gateway_loss=0 ping_target_loss=0 target_ipv4="192.0.2.10" target_ipv6="2001:db8::10" route_get_ipv4="192.0.2.10 via 192.0.2.1 dev eth0 src 192.0.2.20" route_get_ipv6="2001:db8::10 from 2001:db8::20 via fe80::1 dev br-client" wget_stderr="wget: connection reset by peer" tcp_gateway_probe_port=53 tcp_gateway_probe_result="success" tcp_target_probe_port=443 tcp_target_probe_result="exit_1"'
                    'diag_section,name=target_resolution'
                    'Name: example.invalid'
                    'Address 1: 192.0.2.10'
                    'diag_section_end,name=target_resolution'
                    'diag_section,name=route_get'
                    '192.0.2.10 via 192.0.2.1 dev eth0 src 192.0.2.20'
                    'diag_section_end,name=route_get'
                    'diag_section,name=tcp_gateway_probe'
                    'target=fe80::1%br-client port=53 result=success'
                    'Connection to fe80::1%br-client 53 port [tcp/domain] succeeded!'
                    'diag_section_end,name=tcp_gateway_probe'
                    'diag_section,name=tcp_target_probe'
                    'target=2001:db8::10 port=443 result=exit_1'
                    'nc: connection timed out'
                    'diag_section_end,name=tcp_target_probe'
                    'diag_section,name=ip_rule'
                    '0: from all lookup local'
                    '1000: from all lookup main'
                    'diag_section_end,name=ip_rule'
                    'diag_section,name=batctl_if'
                    'wlan0: active'
                    'mesh0: active'
                    'diag_section_end,name=batctl_if'
                    'diag_section,name=batctl_n'
                    '[B.A.T.M.A.N. adv 2024.0, MainIF/MAC: wlan0/02:00:00:00:00:01 (mesh0 BATMAN_V)]'
                    'IF             Neighbor              last-seen'
                    'wlan0          02:11:22:33:44:55    0.420s'
                    'diag_section_end,name=batctl_n'
                    'diag_section,name=ubus_network_dump'
                    '{"interface":[]}'
                    'diag_section_end,name=ubus_network_dump'
                    'diag_section,name=ubus_ifstatus_wan'
                    '{"up":true}'
                    'diag_section_end,name=ubus_ifstatus_wan'
                    'diag_section,name=ubus_ifstatus_wan6'
                    '{"up":false}'
                    'diag_section_end,name=ubus_ifstatus_wan6'
                ) -join "`n"
                ParsedDiagnostic = [pscustomobject]@{
                    NodeId = 'cc'
                    TargetHost = 'example.invalid'
                    SpeedtestDelaySeconds = 30
                    DiagnosticDelaySeconds = 90
                    TimestampNs = '1772839800'
                    GatewayProbe = 'fe80::1'
                    GatewayProbeKind = 'ipv6'
                    PingGatewayLossPct = 0
                    PingTargetLossPct = 0
                    Load1 = 0.10
                    Load5 = 0.20
                    Load15 = 0.30
                    TargetIPv4 = '192.0.2.10'
                    TargetIPv6 = '2001:db8::10'
                    RouteGetIPv4 = '192.0.2.10 via 192.0.2.1 dev eth0 src 192.0.2.20'
                    RouteGetIPv6 = '2001:db8::10 from 2001:db8::20 via fe80::1 dev br-client'
                    WgetStderr = 'wget: connection reset by peer'
                    TcpGatewayProbePort = 53
                    TcpGatewayProbeResult = 'success'
                    TcpTargetProbePort = 443
                    TcpTargetProbeResult = 'exit_1'
                    TargetResolution = "Name: example.invalid`nAddress 1: 192.0.2.10"
                    RouteGet = '192.0.2.10 via 192.0.2.1 dev eth0 src 192.0.2.20'
                    TcpGatewayProbe = "target=fe80::1%br-client port=53 result=success`nConnection to fe80::1%br-client 53 port [tcp/domain] succeeded!"
                    TcpTargetProbe = "target=2001:db8::10 port=443 result=exit_1`nnc: connection timed out"
                    IpRule = "0: from all lookup local`n1000: from all lookup main"
                    BatctlIf = "wlan0: active`nmesh0: active"
                    BatctlN = "[B.A.T.M.A.N. adv 2024.0, MainIF/MAC: wlan0/02:00:00:00:00:01 (mesh0 BATMAN_V)]`nIF             Neighbor              last-seen`nwlan0          02:11:22:33:44:55    0.420s"
                    UbusNetworkDump = '{"interface":[]}'
                    UbusIfstatusWan = '{"up":true}'
                    UbusIfstatusWan6 = '{"up":false}'
                }
            }

            Mock Show-StartupBanner {}
            Mock Update-ConsoleStatus {}
            Mock Write-Progress {}
            Mock Write-Host {}
            Mock Write-Log {}
            Mock Initialize-Database {}
            Mock Get-EnvironmentConfig { $config }
            Mock Import-NodeListFromExcel { [pscustomobject]@{ Nodes = @($node); SourceFiles = @('nodes.csv') } }
            Mock Start-MeasurementRun { [pscustomobject]@{ RawDir = (Join-Path $TestDrive 'raw-run-diagnostics') } }
            Mock Write-NodeActionLog {}
            Mock Add-NodeJobRecord {}
            Mock Wait-WithProgress {}
            Mock Save-Measurement {}
            Mock Save-NodeDiagnostic {}
            Mock Complete-MeasurementRun {}
            Mock Invoke-NodeTriggerBatch {
                @([pscustomobject]@{ Node = $node; TriggerResult = [pscustomobject]@{ Triggered = $true; RemoteResultFile = '/tmp/node-020.txt'; RemoteErrorFile = '/tmp/node-020.err'; AssignedDelaySeconds = 0 } })
            }
            Mock Invoke-NodeCollectBatch {
                @([pscustomobject]@{
                        Node = $node
                        CollectResult = [pscustomobject]@{
                            Success = $true
                            ErrorOutput = ''
                            Files = @($measurementFile)
                            DiagnosticFiles = @($diagnosticFile)
                            PendingFiles = @()
                        }
                    })
            }

            try {
                Invoke-CollectNodeMetricsMain -RunId 'run-diagnostics'

                Assert-MockCalled Save-NodeDiagnostic -Times 1 -Exactly -ParameterFilter {
                    $RunId -eq 'run-diagnostics' -and
                    $Node.DeviceID -eq 'node-020' -and
                    $Diagnostic.NodeId -eq 'cc' -and
                    $Diagnostic.TargetHost -eq 'example.invalid' -and
                    $Diagnostic.TargetIPv4 -eq '192.0.2.10' -and
                    $Diagnostic.WgetStderr -eq 'wget: connection reset by peer' -and
                    $Diagnostic.TcpGatewayProbeResult -eq 'success' -and
                    $Diagnostic.TcpTargetProbeResult -eq 'exit_1' -and
                    $Diagnostic.IpRule -eq "0: from all lookup local`n1000: from all lookup main" -and
                    $Diagnostic.BatctlN -like '*02:11:22:33:44:55*'
                }
                Assert-MockCalled Write-NodeActionLog -Times 1 -ParameterFilter {
                    $Node.DeviceID -eq 'node-020' -and
                    $Action -eq 'diagnostic_saved' -and
                    $Detail -like 'reason=measurement_present; files=1*'
                }
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
        $raw = 'speedtest,nodeid=001122334455 download_mbit=87.32 bytes=104857600 sec=9.608123 timeout_seconds=480,target="https://fsn1-speed.hetzner.com/100MB.bin" 1731000000000000000'
        $parsed = ConvertFrom-MeasurementOutput -RawOutput $raw

        $parsed | Should -Not -BeNullOrEmpty
        $parsed.NodeId | Should -Be '001122334455'
        $parsed.ThroughputMbit | Should -Be 87.32
        $parsed.Target | Should -Be 'https://fsn1-speed.hetzner.com/100MB.bin'
        $parsed.TimestampNs | Should -Be '1731000000000000000'
        $parsed.DownloadedBytes | Should -Be 104857600
        $parsed.ExpectedBytes | Should -Be 104857600
        $parsed.TimeoutSeconds | Should -Be 480
        $parsed.WgetExitCode | Should -Be 0
    }


    It 'parses valid line protocol when banner text is present' {
        $raw = @(
            'Freifunk Nordhessen e.V.'
            'Hostname: Test-Node'
            'speedtest,nodeid=aabbccddeeff download_mbit=10.75 bytes=104857600 sec=78.033210 timeout_seconds=180,target="https://fsn1-speed.hetzner.com/100MB.bin" 1772839860'
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
        $failed = ConvertFrom-MeasurementOutput -RawOutput 'wget_failed,nodeid=001122334455 exit=4 bytes=0 sec=12.500000 expected_bytes=104857600 timeout_seconds=180 target="https://fsn1-speed.hetzner.com/100MB.bin" 1772839860'
        $invalid = ConvertFrom-MeasurementOutput -RawOutput 'speedtest_invalid,nodeid=001122334455 bytes=0 sec=0.000000 expected_bytes=104857600 timeout_seconds=180 target="https://fsn1-speed.hetzner.com/100MB.bin" 1772839860'
        $mismatch = ConvertFrom-MeasurementOutput -RawOutput 'speedtest_size_mismatch,nodeid=001122334455 bytes=104857599 sec=179.500000 expected_bytes=104857600 timeout_seconds=180 target="https://fsn1-speed.hetzner.com/100MB.bin" 1772839860'
        $timeout = ConvertFrom-MeasurementOutput -RawOutput 'speedtest_timeout,nodeid=001122334455 exit=124 bytes=104857600 sec=180.000321 expected_bytes=104857600 timeout_seconds=180 target="https://fsn1-speed.hetzner.com/100MB.bin" 1772839860'

        $failed.ResultType | Should -Be 'final_failed'
        $failed.FailureReason | Should -Be 'wget_failed'
        $failed.ThroughputMbit | Should -Be 0
        $failed.WgetExitCode | Should -Be 4
        $invalid.FailureReason | Should -Be 'speedtest_invalid'
        $mismatch.FailureReason | Should -Be 'speedtest_size_mismatch'
        $timeout.FailureReason | Should -Be 'speedtest_timeout'
        $timeout.ThroughputMbit | Should -Be 0
        $timeout.TimeoutSeconds | Should -Be 180
        $timeout.DownloadDurationSeconds | Should -BeGreaterThan 180
    }

    It 'parses wget metadata sections for failed results' {
        $raw = @(
            'wget_failed,nodeid=001122334455 exit=4 bytes=0 sec=12.500000 expected_bytes=104857600 timeout_seconds=180 target="https://fsn1-speed.hetzner.com/100MB.bin" 1772839860'
            'measurement_meta,wget_exit_reason="network_failure" wget_exit_code=4'
            'measurement_section,name=wget_stderr'
            'wget: connection reset by peer'
            'operation aborted'
            'measurement_section_end,name=wget_stderr'
        ) -join "`n"

        $parsed = ConvertFrom-MeasurementOutput -RawOutput $raw

        $parsed.WgetExitCode | Should -Be 4
        $parsed.WgetExitReason | Should -Be 'network_failure'
        $parsed.WgetStderr | Should -Be "wget: connection reset by peer`noperation aborted"
    }
    It 'parses a complete download at the exact timeout as success' {
        $parsed = ConvertFrom-MeasurementOutput -RawOutput 'speedtest,nodeid=001122334455 download_mbit=1.75 bytes=104857600 sec=480.000000 timeout_seconds=480,target="https://fsn1-speed.hetzner.com/100MB.bin" 1772839860'

        $parsed.ResultType | Should -Be 'success'
        $parsed.DownloadedBytes | Should -Be 104857600
        $parsed.ExpectedBytes | Should -Be 104857600
        $parsed.TimeoutSeconds | Should -Be 480
        $parsed.DownloadDurationSeconds | Should -Be 480
    }

    It 'returns null for empty payload' {
        $parsed = ConvertFrom-MeasurementOutput -RawOutput ''
        $parsed | Should -BeNullOrEmpty
    }
}

Describe 'Initialize-Database and Save-Measurement' {
    It 'stores parsed transfer metadata for failed timeout measurements' {
        InModuleScope FreifunkMetrics {
            $baseDir = Join-Path $TestDrive 'db-metadata'
            New-Item -ItemType Directory -Path $baseDir -Force | Out-Null

            $config = @{
                DatabasePath = (Join-Path $baseDir 'metrics.db')
                SQLiteBinary = 'sqlite3'
                LogDir = $baseDir
            }
            $node = [pscustomobject]@{
                DeviceID = 'node-timeout'
                Name = 'Timeout Node'
                IP = '2a03:2260::dead'
                Domain = 'dom-timeout'
            }
            $parsed = [pscustomobject]@{
                NodeId = '001122334455'
                ThroughputMbit = 0.0
                Target = 'https://fsn1-speed.hetzner.com/100MB.bin'
                TimestampNs = '1772839860'
                ResultType = 'final_failed'
                FailureReason = 'speedtest_timeout'
                DownloadedBytes = 104857600
                ExpectedBytes = 104857600
                DownloadDurationSeconds = 180.000321
                TimeoutSeconds = 180
                WgetExitCode = 124
                WgetExitReason = 'watchdog_timeout'
                WgetStderr = "wget: TLS handshake failed`nretrying..."
            }

            Mock Write-Log {}

            Initialize-Database -Config $config
            Save-Measurement -Config $config -Node $node -RunId 'run-timeout' -RawOutput (@(
                'speedtest_timeout,nodeid=001122334455 exit=124 bytes=104857600 sec=180.000321 expected_bytes=104857600 timeout_seconds=180 target="https://fsn1-speed.hetzner.com/100MB.bin" 1772839860'
                'measurement_meta,wget_exit_reason="watchdog_timeout" wget_exit_code=124'
                'measurement_section,name=wget_stderr'
                'wget: TLS handshake failed'
                'retrying...'
                'measurement_section_end,name=wget_stderr'
            ) -join "`n") -ParsedMeasurement $parsed

            $row = & sqlite3 $config.DatabasePath "select result_type || '|' || failure_reason || '|' || downloaded_bytes || '|' || expected_bytes || '|' || round(download_duration_seconds,6) || '|' || timeout_seconds || '|' || wget_exit_code || '|' || wget_exit_reason || '|' || replace(wget_stderr, char(10), '<n>') from measurements where run_id='run-timeout';"
            $LASTEXITCODE | Should -Be 0
            $row | Should -Be 'final_failed|speedtest_timeout|104857600|104857600|180.000321|180|124|watchdog_timeout|wget: TLS handshake failed<n>retrying...'
        }
    }

    It 'stores extended node diagnostic metadata' {
        InModuleScope FreifunkMetrics {
            $baseDir = Join-Path $TestDrive 'db-diagnostic-metadata'
            New-Item -ItemType Directory -Path $baseDir -Force | Out-Null

            $config = @{
                DatabasePath = (Join-Path $baseDir 'metrics.db')
                SQLiteBinary = 'sqlite3'
                LogDir = $baseDir
            }
            $node = [pscustomobject]@{
                DeviceID = 'node-diagnostic'
                Name = 'Diagnostic Node'
                IP = '2a03:2260::beef'
                Domain = 'dom-diagnostic'
            }
            $diagnostic = [pscustomobject]@{
                NodeId = 'aabbccddeeff'
                TargetHost = 'ash-speed.hetzner.com'
                SpeedtestDelaySeconds = 30
                DiagnosticDelaySeconds = 90
                TimestampNs = '1772839860'
                GatewayProbe = 'fe80::1'
                GatewayProbeKind = 'ipv6'
                PingGatewayLossPct = 0
                PingTargetLossPct = 25
                TcpGatewayProbePort = 53
                TcpGatewayProbeResult = 'success'
                TcpTargetProbePort = 443
                TcpTargetProbeResult = 'exit_1'
                Load1 = 0.12
                Load5 = 0.34
                Load15 = 0.56
                TargetIPv4 = '192.0.2.10'
                TargetIPv6 = '2001:db8::10'
                RouteGetIPv4 = '192.0.2.10 via 192.0.2.1 dev eth0 src 192.0.2.20'
                RouteGetIPv6 = '2001:db8::10 from 2001:db8::20 via fe80::1 dev br-client'
                WgetStderr = 'wget: connection reset by peer'
                TargetResolution = 'Name: ash-speed.hetzner.com Address 1: 192.0.2.10'
                RouteGet = '192.0.2.10 via 192.0.2.1 dev eth0 src 192.0.2.20'
                TcpGatewayProbe = 'target=fe80::1%br-client port=53 result=success'
                TcpTargetProbe = 'target=2001:db8::10 port=443 result=exit_1'
                IpRule = '0: from all lookup local'
                BatctlIf = 'wlan0: active'
                BatctlN = 'wlan0 02:11:22:33:44:55 0.420s'
                UbusNetworkDump = '{"interface":[]}'
                UbusIfstatusWan = '{"up":true}'
                UbusIfstatusWan6 = '{"up":false}'
                LocalPath = 'diag-aabb.txt'
                RawOutput = 'diagnostic raw'
            }

            Mock Write-Log {}

            Initialize-Database -Config $config
            Save-NodeDiagnostic -Config $config -Node $node -RunId 'run-diagnostic' -Diagnostic $diagnostic

            $row = & sqlite3 $config.DatabasePath "select target_ipv4 || '|' || target_ipv6 || '|' || route_get_ipv4 || '|' || wget_stderr || '|' || tcp_gateway_probe_port || '|' || tcp_gateway_probe_result || '|' || tcp_target_probe_port || '|' || tcp_target_probe_result || '|' || target_resolution || '|' || ip_rule || '|' || batctl_if || '|' || batctl_n || '|' || ubus_ifstatus_wan6 from node_diagnostics where run_id='run-diagnostic';"
            $LASTEXITCODE | Should -Be 0
            $row | Should -Be '192.0.2.10|2001:db8::10|192.0.2.10 via 192.0.2.1 dev eth0 src 192.0.2.20|wget: connection reset by peer|53|success|443|exit_1|Name: ash-speed.hetzner.com Address 1: 192.0.2.10|0: from all lookup local|wlan0: active|wlan0 02:11:22:33:44:55 0.420s|{"up":false}'
        }
    }
}

Describe 'ConvertFrom-NodeDiagnosticOutput' {
    It 'parses diagnostic summary payloads' {
        $raw = @(
            'diagnostic,nodeid=aabbccddeeff target_host="ash-speed.hetzner.com" speedtest_delay_seconds=30 diagnostic_delay_seconds=90 timestamp=1772839860'
            'diag_summary,load1=0.12 load5=0.34 load15=0.56 gateway_probe="fe80::1" gateway_probe_kind="ipv6" ping_gateway_loss=0 ping_target_loss=25 target_ipv4="192.0.2.10" target_ipv6="2001:db8::10" route_get_ipv4="192.0.2.10 via 192.0.2.1 dev eth0 src 192.0.2.20" route_get_ipv6="2001:db8::10 from 2001:db8::20 via fe80::1 dev br-client" wget_stderr="wget: connection reset by peer" tcp_gateway_probe_port=53 tcp_gateway_probe_result="success" tcp_target_probe_port=443 tcp_target_probe_result="exit_1"'
            'diag_section,name=ip_route'
            'default via 192.0.2.1 dev eth0'
            'diag_section_end,name=ip_route'
            'diag_section,name=target_resolution'
            'Name: ash-speed.hetzner.com'
            'Address 1: 192.0.2.10'
            'diag_section_end,name=target_resolution'
            'diag_section,name=route_get'
            '192.0.2.10 via 192.0.2.1 dev eth0 src 192.0.2.20'
            'diag_section_end,name=route_get'
            'diag_section,name=tcp_gateway_probe'
            'target=fe80::1%br-client port=53 result=success'
            'Connection to fe80::1%br-client 53 port [tcp/domain] succeeded!'
            'diag_section_end,name=tcp_gateway_probe'
            'diag_section,name=tcp_target_probe'
            'target=2001:db8::10 port=443 result=exit_1'
            'nc: connection timed out'
            'diag_section_end,name=tcp_target_probe'
            'diag_section,name=ip_rule'
            '0: from all lookup local'
            '1000: from all lookup main'
            'diag_section_end,name=ip_rule'
            'diag_section,name=batctl_if'
            'wlan0: active'
            'mesh0: active'
            'diag_section_end,name=batctl_if'
            'diag_section,name=batctl_n'
            '[B.A.T.M.A.N. adv 2024.0, MainIF/MAC: wlan0/02:00:00:00:00:01 (mesh0 BATMAN_V)]'
            'IF             Neighbor              last-seen'
            'wlan0          02:11:22:33:44:55    0.420s'
            'diag_section_end,name=batctl_n'
            'diag_section,name=ubus_network_dump'
            '{"interface":[]}'
            'diag_section_end,name=ubus_network_dump'
            'diag_section,name=ubus_ifstatus_wan'
            '{"up":true}'
            'diag_section_end,name=ubus_ifstatus_wan'
            'diag_section,name=ubus_ifstatus_wan6'
            '{"up":false}'
            'diag_section_end,name=ubus_ifstatus_wan6'
        ) -join "`n"

        $parsed = ConvertFrom-NodeDiagnosticOutput -RawOutput $raw

        $parsed | Should -Not -BeNullOrEmpty
        $parsed.NodeId | Should -Be 'aabbccddeeff'
        $parsed.TargetHost | Should -Be 'ash-speed.hetzner.com'
        $parsed.SpeedtestDelaySeconds | Should -Be 30
        $parsed.DiagnosticDelaySeconds | Should -Be 90
        $parsed.GatewayProbeKind | Should -Be 'ipv6'
        $parsed.PingTargetLossPct | Should -Be 25
        $parsed.Load15 | Should -Be 0.56
        $parsed.TargetIPv4 | Should -Be '192.0.2.10'
        $parsed.TargetIPv6 | Should -Be '2001:db8::10'
        $parsed.RouteGetIPv4 | Should -Be '192.0.2.10 via 192.0.2.1 dev eth0 src 192.0.2.20'
        $parsed.WgetStderr | Should -Be 'wget: connection reset by peer'
        $parsed.TcpGatewayProbePort | Should -Be 53
        $parsed.TcpGatewayProbeResult | Should -Be 'success'
        $parsed.TcpTargetProbePort | Should -Be 443
        $parsed.TcpTargetProbeResult | Should -Be 'exit_1'
        $parsed.TargetResolution | Should -Be "Name: ash-speed.hetzner.com`nAddress 1: 192.0.2.10"
        $parsed.TcpGatewayProbe | Should -Match 'result=success'
        $parsed.TcpTargetProbe | Should -Match 'result=exit_1'
        $parsed.IpRule | Should -Be "0: from all lookup local`n1000: from all lookup main"
        $parsed.BatctlIf | Should -Be "wlan0: active`nmesh0: active"
        $parsed.BatctlN | Should -Match '02:11:22:33:44:55'
        $parsed.UbusIfstatusWan6 | Should -Be '{"up":false}'
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
            $job = Start-Job -ScriptBlock { [pscustomobject]@{ PendingNodeKeys = @('node-002|2a03:2260::2||') } }
            Wait-Job -Job $job | Out-Null

            $result = Receive-NodeResultCountPoll -Job $job

            @($result.PendingNodeKeys) | Should -Be @('node-002|2a03:2260::2||')
            Get-Job -Id $job.Id -ErrorAction SilentlyContinue | Should -BeNullOrEmpty
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
    It 'uses throughput ordering on odd ISO weekdays' {
        InModuleScope FreifunkMetrics {
            $config = @{ TriggerRandomDelayMaxSeconds = 10 }
            $nodes = @(
                [pscustomobject]@{ DeviceID = 'node-001'; Name = 'Node 1'; IP = '2a03:2260::1'; Domain = 'dom-a' }
                [pscustomobject]@{ DeviceID = 'node-002'; Name = 'Node 2'; IP = '2a03:2260::2'; Domain = 'dom-a' }
                [pscustomobject]@{ DeviceID = 'node-003'; Name = 'Node 3'; IP = '2a03:2260::3'; Domain = 'dom-a' }
                [pscustomobject]@{ DeviceID = 'node-004'; Name = 'Node 4'; IP = '2a03:2260::4'; Domain = 'dom-a' }
            )

            Mock Get-Date { [datetime]'2026-03-11T12:00:00' }
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

    It 'uses ascending node ids on even ISO weekdays' {
        InModuleScope FreifunkMetrics {
            $config = @{ TriggerRandomDelayMaxSeconds = 10 }
            $nodes = @(
                [pscustomobject]@{ DeviceID = 'node-020'; Name = 'Node 20'; IP = '2a03:2260::20'; Domain = 'dom-a' }
                [pscustomobject]@{ DeviceID = 'node-003'; Name = 'Node 3'; IP = '2a03:2260::3'; Domain = 'dom-a' }
                [pscustomobject]@{ DeviceID = 'node-100'; Name = 'Node 100'; IP = '2a03:2260::100'; Domain = 'dom-a' }
                [pscustomobject]@{ DeviceID = 'node-010'; Name = 'Node 10'; IP = '2a03:2260::10'; Domain = 'dom-a' }
            )

            Mock Get-Date { [datetime]'2026-03-12T12:00:00' }
            Mock Get-LatestThroughputByIp {
                @{
                    '2a03:2260::20' = 5.0
                    '2a03:2260::3' = 90.0
                    '2a03:2260::100' = 1.0
                    '2a03:2260::10' = 50.0
                }
            }

            $assigned = @(Get-NodeTriggerAssignments -Config $config -RunId 'run-a' -Nodes $nodes)
            $delayByDeviceId = @{}
            foreach ($item in $assigned) {
                $delayByDeviceId[$item.Node.DeviceID] = $item.AssignedDelaySeconds
            }

            $delayByDeviceId['node-003'] | Should -Be 0
            $delayByDeviceId['node-010'] | Should -Be 3
            $delayByDeviceId['node-020'] | Should -Be 7
            $delayByDeviceId['node-100'] | Should -Be 10
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
Describe 'Invoke-NodeTriggerBatch' {
    It 'streams trigger payloads over stdin in parallel mode' {
        $captureDir = Join-Path $TestDrive 'trigger-batch-capture'
        New-Item -ItemType Directory -Path $captureDir -Force | Out-Null

        $mockSsh = Join-Path $TestDrive 'mock-ssh-trigger-batch.ps1'
        @(
            '$payload = @($input) -join ""'
            '$nodeHost = $args[-2]'
            '$safeName = ($nodeHost -replace ''[^A-Za-z0-9_.-]'', ''_'')'
            '@('
            '    "LASTARG=$($args[-1])"'
            '    $payload'
            ') | Set-Content -Path (Join-Path $env:FFMH_TRIGGER_CAPTURE_DIR ($safeName + ''.txt''))'
            'exit 0'
        ) | Set-Content -Path $mockSsh

        $config = @{
            SshBinary = $mockSsh
            SshKeyPath = 'ignore'
            SshUser = 'root'
            SshConnectTimeoutSeconds = 1
            RemoteResultDir = '/tmp/harvester'
            TriggerParallelism = 2
            TriggerRandomDelayMaxSeconds = 0
            SpeedtestTargetUrl = 'https://example.invalid/test.bin'
            SpeedtestTargetBytes = 104857600
            EnableNodeDiagnostics = $true
            NodeDiagnosticsDelaySeconds = 60
        }
        $nodes = @(
            [pscustomobject]@{ DeviceID = 'node-001'; Name = 'Node 1'; IP = '2a03:2260::1'; Domain = 'dom-a' }
            [pscustomobject]@{ DeviceID = 'node-002'; Name = 'Node 2'; IP = '2a03:2260::2'; Domain = 'dom-b' }
        )

        $env:FFMH_TRIGGER_CAPTURE_DIR = $captureDir
        try {
            $results = @(Invoke-NodeTriggerBatch -Config $config -Nodes $nodes -RunId 'run-batch')
        }
        finally {
            Remove-Item Env:FFMH_TRIGGER_CAPTURE_DIR -ErrorAction SilentlyContinue
        }

        $sorted = @($results | Sort-Object { $_.Node.DeviceID })
        $sorted.Count | Should -Be 2
        @($sorted | ForEach-Object { $_.TriggerResult.Triggered }) | Should -Be @($true, $true)

        $captures = @(Get-ChildItem -Path $captureDir -File | Sort-Object Name)
        $captures.Count | Should -Be 2
        foreach ($capture in $captures) {
            $content = Get-Content -Raw -Path $capture.FullName
            $content | Should -Match '^LASTARG=sh -s'
            $content | Should -Match "target_url='https://example\.invalid/test\.bin'"
            $content | Should -Match 'delay_seconds=0'
        }
    }
}

Describe 'Invoke-NodeTriggerCommand' {
    It 'streams trigger payloads over stdin so SSH command arguments stay short' {
        $capturePath = Join-Path $TestDrive 'trigger-stdin.txt'
        $mockSsh = Join-Path $TestDrive 'mock-ssh-trigger-stdin.ps1'
        @(
            '$payload = @($input) -join ""'
            '@('
            '    "LASTARG=$($args[-1])"'
            '    "HOST=$($args[-2])"'
            '    "PAYLOAD_BEGIN"'
            '    $payload'
            '    "PAYLOAD_END"'
            ') | Set-Content -Path $env:FFMH_TRIGGER_CAPTURE_PATH'
            'exit 0'
        ) | Set-Content -Path $mockSsh

        $config = @{
            SshBinary = $mockSsh
            SshKeyPath = 'ignore'
            SshUser = 'root'
            SshConnectTimeoutSeconds = 1
            RemoteResultDir = '/tmp/harvester'
            SpeedtestTargetUrl = 'https://example.invalid/test.bin'
            SpeedtestTargetBytes = 104857600
            EnableNodeDiagnostics = $true
            NodeDiagnosticsDelaySeconds = 60
        }
        $node = [pscustomobject]@{ DeviceID = 'node-001'; Name = 'Node 1'; IP = '2a03:2260::1'; Domain = 'dom-a' }

        $env:FFMH_TRIGGER_CAPTURE_PATH = $capturePath
        try {
            $result = Invoke-NodeTriggerCommand -Config $config -Node $node -RunId 'run-stdin' -AssignedDelaySeconds 17
        }
        finally {
            Remove-Item Env:FFMH_TRIGGER_CAPTURE_PATH -ErrorAction SilentlyContinue
        }

        $capture = Get-Content -Raw -Path $capturePath
        $result.Reachable | Should -BeTrue
        $result.Triggered | Should -BeTrue
        $capture | Should -Match '^LASTARG=sh -s'
        $capture | Should -Match 'HOST=root@2a03:2260::1'
        $capture | Should -Match 'delay_seconds=17'
        $capture | Should -Match "diag_out='/tmp/harvester/run-stdin/diag-'"
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
        $info.TriggerCommand | Should -Match 'wget -O /dev/null -q -T 480'
        $info.TriggerCommand | Should -Match 'wget_pid=\$!'
        $info.TriggerCommand | Should -Match "printf '%s' '124' > "
        $info.TriggerCommand | Should -Match 'expected_bytes="?123456789"?'
        $info.TriggerCommand | Should -Match 'measurement_meta,wget_exit_reason='
        $info.TriggerCommand | Should -Match 'measurement_section,name=wget_stderr'
        $info.TriggerCommand | Should -Match 'ip -6 route get'
        $info.TriggerCommand | Should -Match 'tcp_gateway_probe_port=53'
        $info.TriggerCommand | Should -Match 'tcp_gateway_probe_result='
        $info.TriggerCommand | Should -Match 'tcp_target_probe_port=443'
        $info.TriggerCommand | Should -Match 'tcp_target_probe_result='
        $info.TriggerCommand | Should -Match 'diag_section,name=tcp_gateway_probe'
        $info.TriggerCommand | Should -Match 'diag_section,name=tcp_target_probe'
        $info.TriggerCommand | Should -Match 'nc -6 -z -w 5'
        $info.TriggerCommand | Should -Match 'ip rule'
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
            $payload = 'speedtest,nodeid=pester download_mbit=12.34 bytes=104857600 sec=67.979000 timeout_seconds=180,target="https://example.invalid/test.bin" 1772839860'
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
            CollectWaitTimeoutSeconds = '300'
            SpeedtestDownloadTimeoutSeconds = '480'
            SpeedtestTargetBytes = '104857600'
            NodeDiagnosticsGatewayTcpProbePort = '53'
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
        $config.CollectWaitTimeoutSeconds | Should -Be 300
        $config.SpeedtestDownloadTimeoutSeconds | Should -Be 480
        $config.SpeedtestTargetBytes | Should -Be 104857600
        $config.NodeDiagnosticsGatewayTcpProbePort | Should -Be 53
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

    It 'rejects non-positive collect wait timeout' {
        $config = @{
            SshUser = 'root'
            SshBinary = 'ssh'
            SQLiteBinary = 'sqlite3'
            RemoteResultDir = '/tmp/harvester'
            LogFilePrefix = 'collect-node-metrics'
            SpeedtestTargetUrl = 'https://example.invalid/testfile.bin'
            SshConnectTimeoutSeconds = 8
            TriggerParallelism = 10
            CollectParallelism = 4
            TriggerRandomDelayMaxSeconds = 600
            CollectWaitTimeoutSeconds = 0
            SpeedtestTargetBytes = 104857600
            ExcelInputFiles = @()
            ExcelInputDirectories = @()
            ExcelSearchRecurse = $true
            UseTestNodeIPs = $false
            TestNodeIPs = @()
        }

        { Assert-ValidConfig -Config $config } | Should -Throw 'Config value CollectWaitTimeoutSeconds must be at least 1.'
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
            'speedtest,nodeid=aabbcc download_mbit=12.34 bytes=104857600 sec=67.979000 timeout_seconds=180,target="https://example.invalid/test.bin" 123'
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
            '        ''speedtest,nodeid=aabbccddeeff download_mbit=48.25 bytes=104857600 sec=17.385922 timeout_seconds=180,target="https://example.invalid/test.bin" 1700000000000000000'''
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
            '        ''wget_failed,nodeid=aabbccddeeff exit=4 bytes=0 sec=12.500000 expected_bytes=104857600 timeout_seconds=180 target="https://example.invalid/test.bin" 1700000000000000000'''
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

    It 'collects diagnostic files separately from measurements' {
        $mockSsh = Join-Path $TestDrive 'mock-ssh-diagnostic.ps1'
        @(
            '$command = $args[-1]'
            'if ($command -like ''find*'') {'
            '    @('
            '        ''__FFMH_FILE_BEGIN__/tmp/harvester/1700000000.txt'''
            '        ''speedtest,nodeid=aabbccddeeff download_mbit=48.25 bytes=104857600 sec=17.385922 timeout_seconds=180,target="https://example.invalid/test.bin" 1700000000000000000'''
            '        ''__FFMH_FILE_END__/tmp/harvester/1700000000.txt'''
            '        ''__FFMH_FILE_BEGIN__/tmp/harvester/diag-1700000001.txt'''
            '        ''diagnostic,nodeid=aabbccddeeff target_host="example.invalid" speedtest_delay_seconds=10 diagnostic_delay_seconds=70 timestamp=1700000001000000000'''
            '        ''diag_summary,load1=0.12 load5=0.23 load15=0.34 gateway_probe="192.0.2.1" gateway_probe_kind="ipv4" ping_gateway_loss=0 ping_target_loss=100 target_ipv4="192.0.2.10" target_ipv6="2001:db8::10" route_get_ipv4="192.0.2.10 via 192.0.2.1 dev eth0 src 192.0.2.20" route_get_ipv6="2001:db8::10 from 2001:db8::20 via fe80::1 dev br-client" wget_stderr="wget: connection reset by peer" tcp_gateway_probe_port=53 tcp_gateway_probe_result="success" tcp_target_probe_port=443 tcp_target_probe_result="exit_1"'''
            '        ''diag_section,name=target_resolution'''
            '        ''Name: example.invalid'''
            '        ''Address 1: 192.0.2.10'''
            '        ''diag_section_end,name=target_resolution'''
            '        ''diag_section,name=route_get'''
            '        ''192.0.2.10 via 192.0.2.1 dev eth0 src 192.0.2.20'''
            '        ''diag_section_end,name=route_get'''
            '        ''diag_section,name=tcp_gateway_probe'''
            '        ''target=192.0.2.1 port=53 result=success'''
            '        ''Connection to 192.0.2.1 53 port [tcp/domain] succeeded!'''
            '        ''diag_section_end,name=tcp_gateway_probe'''
            '        ''diag_section,name=tcp_target_probe'''
            '        ''target=192.0.2.10 port=443 result=exit_1'''
            '        ''nc: connection timed out'''
            '        ''diag_section_end,name=tcp_target_probe'''
            '        ''diag_section,name=ip_rule'''
            '        ''0: from all lookup local'''
            '        ''diag_section_end,name=ip_rule'''
            '        ''diag_section,name=batctl_if'''
            '        ''wlan0: active'''
            '        ''diag_section_end,name=batctl_if'''
            '        ''diag_section,name=batctl_n'''
            '        ''wlan0 02:11:22:33:44:55 0.420s'''
            '        ''diag_section_end,name=batctl_n'''
            '        ''diag_section,name=ubus_network_dump'''
            '        ''{"interface":[]}'''
            '        ''diag_section_end,name=ubus_network_dump'''
            '        ''diag_section,name=ubus_ifstatus_wan'''
            '        ''{"up":true}'''
            '        ''diag_section_end,name=ubus_ifstatus_wan'''
            '        ''diag_section,name=ubus_ifstatus_wan6'''
            '        ''{"up":false}'''
            '        ''diag_section_end,name=ubus_ifstatus_wan6'''
            '        ''__FFMH_FILE_END__/tmp/harvester/diag-1700000001.txt'''
            '    ) | ForEach-Object { Write-Output $_ }'
            '    exit 0'
            '}'
            'if ($command -like ''rm -f*'') {'
            '    exit 0'
            '}'
            'Write-Output ("unexpected command: {0}" -f $command)'
            'exit 9'
        ) | Set-Content -Path $mockSsh

        $rawDir = Join-Path $TestDrive 'raw-diagnostic'
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
        $diagnostics = @($result.DiagnosticFiles)

        $result.Success | Should -BeTrue
        $files.Count | Should -Be 1
        $diagnostics.Count | Should -Be 1
        $diagnostics[0].ParsedDiagnostic.TargetHost | Should -Be 'example.invalid'
        $diagnostics[0].ParsedDiagnostic.PingTargetLossPct | Should -Be 100
        $diagnostics[0].ParsedDiagnostic.TargetIPv4 | Should -Be '192.0.2.10'
        $diagnostics[0].ParsedDiagnostic.WgetStderr | Should -Be 'wget: connection reset by peer'
        $diagnostics[0].ParsedDiagnostic.TcpGatewayProbeResult | Should -Be 'success'
        $diagnostics[0].ParsedDiagnostic.TcpTargetProbeResult | Should -Be 'exit_1'
        $diagnostics[0].ParsedDiagnostic.IpRule | Should -Be '0: from all lookup local'
        $diagnostics[0].ParsedDiagnostic.BatctlIf | Should -Be 'wlan0: active'
        $diagnostics[0].ParsedDiagnostic.BatctlN | Should -Be 'wlan0 02:11:22:33:44:55 0.420s'
        $diagnostics[0].ParsedDiagnostic.UbusIfstatusWan | Should -Be '{"up":true}'
    }
}


Describe 'Get-NodeTriggerCommandInfo' {
    It 'does not join background commands with invalid ampersand semicolons' {
        $config = @{
            SpeedtestTargetUrl = 'https://example.invalid/test.bin'
            SpeedtestTargetBytes = 104857600
            RemoteResultDir = '/tmp/harvester'
            EnableNodeDiagnostics = $true
            NodeDiagnosticsDelaySeconds = 60
        }

        $info = Get-NodeTriggerCommandInfo -Config $config -RunId 'run-test' -AssignedDelaySeconds 30

        $info.TriggerCommand | Should -Not -Match '&;'
        $info.TriggerCommand | Should -Match "diag_out='/tmp/harvester/run-test/diag-'"
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
            '        "speedtest,nodeid=node$suffix download_mbit=40.$suffix bytes=104857600 sec=20.000000 timeout_seconds=180,target=`"https://example.invalid/test.bin`" 170000000000000000$suffix"'
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









Describe 'Section parsers' {
    It 'accepts empty diagnostic lines without throwing' {
        InModuleScope FreifunkMetrics {
            { Get-NodeDiagnosticSections -Lines @('') } | Should -Not -Throw
            (Get-NodeDiagnosticSections -Lines @('')).Count | Should -Be 0
        }
    }

    It 'accepts empty measurement lines without throwing' {
        InModuleScope FreifunkMetrics {
            { Get-MeasurementSections -Lines @('') } | Should -Not -Throw
            (Get-MeasurementSections -Lines @('')).Count | Should -Be 0
        }
    }
}
