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

    It 'returns null for invalid payload' {
        $parsed = Parse-MeasurementOutput -RawOutput 'invalid payload'
        $parsed | Should -BeNullOrEmpty
    }
}

    It 'returns null for empty payload' {
        $parsed = Parse-MeasurementOutput -RawOutput ''
        $parsed | Should -BeNullOrEmpty
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

