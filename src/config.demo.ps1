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
    ScpBinary                = 'scp'
    SQLiteBinary             = 'sqlite3'

    RemoteResultDir          = '/tmp/harvester'
    SshConnectTimeoutSeconds = 8
    CollectWaitSeconds       = 90
    LogFilePrefix            = 'collect-node-metrics'

    # One or more node list files. Supports .xlsx via ImportExcel module and .csv via built-in Import-Csv.
    ExcelInputFiles          = @(
        '/home/ffuser/skripte/freifunk-metrics-harvester/temp/nodes.xlsx'
    )
}
