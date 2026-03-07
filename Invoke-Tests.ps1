[CmdletBinding()]
param(
    [ValidateSet('None', 'NUnitXml', 'JUnitXml')]
    [string]$OutputFormat = 'None',
    [string]$OutputPath = '',
    [switch]$InstallModules,
    [switch]$IncludeIntegration,
    [switch]$RunSshStreaming,
    [string]$ConfigPath = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$params = @{
    OutputFormat = $OutputFormat
    OutputPath = $OutputPath
    InstallModules = $InstallModules
    IncludeIntegration = $IncludeIntegration
    RunSshStreaming = $RunSshStreaming
    ConfigPath = $ConfigPath
}

& (Join-Path $PSScriptRoot 'tests/Invoke-Tests.ps1') @params
