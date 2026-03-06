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
    ScpBinary                = 'scp'
    SQLiteBinary             = 'sqlite3'

    RemoteResultDir          = '/tmp/harvester'
    SshConnectTimeoutSeconds = 4
    CollectWaitSeconds       = 15
    LogFilePrefix            = 'collect-node-metrics-dev'

    ExcelInputFiles          = @(
        '/tmp/freifunk-metrics-harvester/temp/sample-nodes.csv'
    )
}
