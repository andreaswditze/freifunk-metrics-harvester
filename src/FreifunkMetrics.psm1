<#
.SYNOPSIS
Collects distributed Freifunk node performance measurements in two phases.

.DESCRIPTION
Loads the FreifunkMetrics function groups and exports them as a single module.

.NOTES
Designed for PowerShell 7 on Linux and non-interactive execution via cron/systemd.
#>
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$script:CurrentConfig = $null
$script:LogFilePath = $null
$script:DailyLogDir = $null
$script:DailyLogFilePath = $null
$script:ConsoleStatusLength = 0
$script:ConsoleBannerShown = $false
$script:ModuleFilePath = $PSCommandPath
$script:ModuleBaseDir = Split-Path -Parent $PSCommandPath

$privateScriptDir = Join-Path $script:ModuleBaseDir 'FreifunkMetrics'
foreach ($scriptName in @(
    'Common.ps1',
    'Config.ps1',
    'NodeSource.ps1',
    'Storage.ps1',
    'Transport.ps1',
    'Runner.ps1'
)) {
    . (Join-Path $privateScriptDir $scriptName)
}

Export-ModuleMember -Function *
