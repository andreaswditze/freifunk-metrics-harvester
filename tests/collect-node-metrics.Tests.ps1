BeforeAll {
    . "$PSScriptRoot/../src/collect-node-metrics.ps1" -NoRun
}

Describe 'Parse-MeasurementOutput' {
    It 'parses valid line protocol' {
        $raw = 'speedtest,nodeid=001122334455 download_mbit=87.32,target="https://fsn1-speed.hetzner.com/100MB.bin" 1731000000000000000'
        $parsed = Parse-MeasurementOutput -RawOutput $raw

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
        $parsed = Parse-MeasurementOutput -RawOutput $raw

        $parsed | Should -Not -BeNullOrEmpty
        $parsed.NodeId | Should -Be 'aabbccddeeff'
        $parsed.ThroughputMbit | Should -Be 10.75
        $parsed.TimestampNs | Should -Be '1772839860'
    }
    It 'returns null for invalid payload' {
        $parsed = Parse-MeasurementOutput -RawOutput 'invalid payload'
        $parsed | Should -BeNullOrEmpty
    }

    It 'returns null for empty payload' {
        $parsed = Parse-MeasurementOutput -RawOutput ''
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
