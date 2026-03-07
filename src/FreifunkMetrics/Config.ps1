# Functions for this concern are loaded by FreifunkMetrics.psm1.



function Get-EnvironmentConfig {
    [CmdletBinding()]
    param(
        [string]$RequestedPath
    )

    $scriptRoot = $script:ModuleBaseDir
    $configCandidates = @()

    if ($RequestedPath) {
        $configCandidates += $RequestedPath
    }

    $configCandidates += @(
        (Join-Path $scriptRoot 'config.production.ps1'),
        (Join-Path $scriptRoot 'config.development.example.ps1'),
        (Join-Path $scriptRoot 'config.demo.ps1')
    )

    $chosenConfig = $null
    foreach ($candidate in $configCandidates) {
        if ($candidate -and (Test-Path -Path $candidate -PathType Leaf)) {
            $chosenConfig = (Resolve-Path -Path $candidate).Path
            break
        }
    }

    if (-not $chosenConfig) {
        throw 'No config file found. Provide -ConfigPath or create config.production.ps1 from config.demo.ps1.'
    }

    $configData = & $chosenConfig
    if (-not $configData -or $configData -isnot [hashtable]) {
        throw "Config file must return a hashtable: ${chosenConfig}"
    }

    $defaults = @{
        SshUser                   = 'root'
        SshBinary                 = 'ssh'
        SQLiteBinary              = 'sqlite3'
        RemoteResultDir           = '/tmp/harvester'
        SshConnectTimeoutSeconds  = 8
        TriggerParallelism        = 10
        CollectParallelism        = 10
        TriggerRandomDelayMaxSeconds = 600
        SpeedtestTargetUrl        = 'https://fsn1-speed.hetzner.com/100MB.bin'
        SpeedtestTargetBytes      = 104857600
        LogFilePrefix             = 'collect-node-metrics'
        ExcelInputFiles           = @()
        ExcelInputDirectories     = @()
        ExcelSearchRecurse        = $true
        UseTestNodeIPs            = $false
        TestNodeIPs               = @('2a03:2260:3013:200:7a8a:20ff:fed0:747a','2a03:2260:3013:200:1ae8:29ff:fe5c:1ff8')
    }

    foreach ($key in $defaults.Keys) {
        if (-not $configData.ContainsKey($key) -or [string]::IsNullOrWhiteSpace([string]$configData[$key])) {
            $configData[$key] = $defaults[$key]
        }
    }

    $requiredKeys = @(
        'ScriptBaseDir',
        'DatabasePath',
        'RawResultBaseDir',
        'LogDir',
        'TempDir',
        'SshKeyPath'
    )

    foreach ($key in $requiredKeys) {
        if (-not $configData.ContainsKey($key) -or [string]::IsNullOrWhiteSpace([string]$configData[$key])) {
            throw "Missing required config key: ${key}"
        }
    }

    Assert-ValidConfig -Config $configData
    $configData.ConfigPath = $chosenConfig
    return $configData
}

function Assert-ValidConfig {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Config
    )

    $Config.SshUser = Convert-ToTrimmedString -Value $Config.SshUser
    $Config.SshBinary = Convert-ToTrimmedString -Value $Config.SshBinary
    $Config.SQLiteBinary = Convert-ToTrimmedString -Value $Config.SQLiteBinary
    $Config.RemoteResultDir = Convert-ToTrimmedString -Value $Config.RemoteResultDir
    $Config.LogFilePrefix = Convert-ToTrimmedString -Value $Config.LogFilePrefix
    $Config.SpeedtestTargetUrl = Convert-ToTrimmedString -Value $Config.SpeedtestTargetUrl

    if ([string]::IsNullOrWhiteSpace($Config.SshUser)) {
        throw 'Config value SshUser must not be empty.'
    }

    if ([string]::IsNullOrWhiteSpace($Config.SshBinary)) {
        throw 'Config value SshBinary must not be empty.'
    }

    if ([string]::IsNullOrWhiteSpace($Config.SQLiteBinary)) {
        throw 'Config value SQLiteBinary must not be empty.'
    }

    if ([string]::IsNullOrWhiteSpace($Config.RemoteResultDir)) {
        throw 'Config value RemoteResultDir must not be empty.'
    }

    if ([string]::IsNullOrWhiteSpace($Config.LogFilePrefix)) {
        throw 'Config value LogFilePrefix must not be empty.'
    }

    $speedtestUri = $null
    if (-not [uri]::TryCreate($Config.SpeedtestTargetUrl, [System.UriKind]::Absolute, [ref]$speedtestUri)) {
        throw 'Config value SpeedtestTargetUrl must be an absolute URL.'
    }

    if ($speedtestUri.Scheme -notin @('http', 'https')) {
        throw 'Config value SpeedtestTargetUrl must use http or https.'
    }

    try {
        $Config.SshConnectTimeoutSeconds = [int]$Config.SshConnectTimeoutSeconds
    }
    catch {
        throw 'Config value SshConnectTimeoutSeconds must be an integer.'
    }

    if ($Config.SshConnectTimeoutSeconds -lt 1) {
        throw 'Config value SshConnectTimeoutSeconds must be at least 1.'
    }

    try {
        $Config.TriggerParallelism = [int]$Config.TriggerParallelism
    }
    catch {
        throw 'Config value TriggerParallelism must be an integer.'
    }

    if ($Config.TriggerParallelism -lt 1) {
        throw 'Config value TriggerParallelism must be at least 1.'
    }

    try {
        $Config.CollectParallelism = [int]$Config.CollectParallelism
    }
    catch {
        throw 'Config value CollectParallelism must be an integer.'
    }

    if ($Config.CollectParallelism -lt 1) {
        throw 'Config value CollectParallelism must be at least 1.'
    }

    try {
        $Config.TriggerRandomDelayMaxSeconds = [int]$Config.TriggerRandomDelayMaxSeconds
    }
    catch {
        throw 'Config value TriggerRandomDelayMaxSeconds must be an integer.'
    }

    if ($Config.TriggerRandomDelayMaxSeconds -lt 0) {
        throw 'Config value TriggerRandomDelayMaxSeconds must be zero or greater.'
    }

    try {
        $Config.SpeedtestTargetBytes = [int64]$Config.SpeedtestTargetBytes
    }
    catch {
        throw 'Config value SpeedtestTargetBytes must be an integer.'
    }

    if ($Config.SpeedtestTargetBytes -lt 1) {
        throw 'Config value SpeedtestTargetBytes must be at least 1.'
    }

    $Config.ExcelInputFiles = @($Config.ExcelInputFiles | Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) })
    $Config.ExcelInputDirectories = @($Config.ExcelInputDirectories | Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) })
    $Config.ExcelSearchRecurse = [bool]$Config.ExcelSearchRecurse
    $Config.UseTestNodeIPs = [bool]$Config.UseTestNodeIPs
    $Config.TestNodeIPs = @($Config.TestNodeIPs | Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) })
}
