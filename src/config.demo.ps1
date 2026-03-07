# Demo configuration for production-like usage on host "mars".
# Copy to config.production.ps1 for local server operation (do not commit production file).

return @{
    ScriptBaseDir            = '/home/ffuser/skripte/freifunk-metrics-harvester'
    DatabasePath             = '/home/ffuser/skripte/freifunk-metrics-harvester/data/metrics.db'
    RawResultBaseDir         = '/home/ffuser/skripte/freifunk-metrics-harvester/data/raw'
    LogDir                   = '/home/ffuser/skripte/freifunk-metrics-harvester/log'
    TempDir                  = '/home/ffuser/skripte/freifunk-metrics-harvester/temp'

    SshKeyPath               = '/home/ffuser/.ssh/burgwald_freifunk_id_rsa_nopw'
    SshUser                  = 'root'
    SshBinary                = 'ssh'
    SQLiteBinary             = 'sqlite3'

    RemoteResultDir          = '/tmp/harvester'
    SshConnectTimeoutSeconds = 8
    TriggerParallelism       = 10
    CollectParallelism       = 10
    TriggerRandomDelayMaxSeconds = 600
    SpeedtestTargetUrl         = 'https://fsn1-speed.hetzner.com/100MB.bin'
    SpeedtestTargetBytes       = 104857600
    LogFilePrefix            = 'collect-node-metrics'

    # Optional explicit files.
    ExcelInputFiles          = @()

    # Recommended for FNDG/FCBT style trees.
    ExcelInputDirectories    = @(
        '/home/ffuser/skripte/fndg/twodrive'
    )

    # Search directories recursively for *.xlsx, *.xlsm, *.xls, *.csv.
    ExcelSearchRecurse       = $true
    # Testing mode: bypass Excel import and run only against these IPs.
    UseTestNodeIPs          = $false
    TestNodeIPs             = @(
        '2a03:2260:3013:200:7a8a:20ff:fed0:747a'
        '2a03:2260:3013:200:1ae8:29ff:fe5c:1ff8'
    )
}

