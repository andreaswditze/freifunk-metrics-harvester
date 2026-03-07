# Development example for local testing.
# Keep this file in git. Use config.production.ps1 for real server values.

return @{
    ScriptBaseDir            = '/tmp/freifunk-metrics-harvester'
    DatabasePath             = '/tmp/freifunk-metrics-harvester/data/metrics.db'
    RawResultBaseDir         = '/tmp/freifunk-metrics-harvester/data/raw'
    LogDir                   = '/tmp/freifunk-metrics-harvester/log'
    TempDir                  = '/tmp/freifunk-metrics-harvester/temp'

    SshKeyPath               = '/home/dev/.ssh/id_rsa'
    SshUser                  = 'root'
    SshBinary                = 'ssh'
    SQLiteBinary             = 'sqlite3'

    RemoteResultDir          = '/tmp/harvester'
    SshConnectTimeoutSeconds = 4
    TriggerParallelism       = 10
    CollectParallelism       = 10
    TriggerRandomDelayMaxSeconds = 600
    SpeedtestTargetUrl         = 'https://fsn1-speed.hetzner.com/100MB.bin'
    SpeedtestTargetBytes       = 104857600
    LogFilePrefix            = 'collect-node-metrics-dev'

    ExcelInputFiles          = @(
        '/tmp/freifunk-metrics-harvester/temp/sample-nodes.csv'
    )

    ExcelInputDirectories    = @()
    ExcelSearchRecurse       = $true
    # Testing mode: bypass Excel import and run only against these IPs.
    UseTestNodeIPs          = $false
    TestNodeIPs             = @(
        '2a03:2260:3013:200:7a8a:20ff:fed0:747a'
        '2a03:2260:3013:200:1ae8:29ff:fe5c:1ff8'
    )
}

