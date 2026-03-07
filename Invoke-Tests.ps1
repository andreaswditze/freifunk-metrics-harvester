[CmdletBinding()]
param(
    [ValidateSet('None', 'NUnitXml', 'JUnitXml')]
    [string]$OutputFormat = 'None',
    [string]$OutputPath = '',
    [switch]$InstallModules
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$params = @{
    OutputFormat = $OutputFormat
    OutputPath = $OutputPath
    InstallModules = $InstallModules
}

& (Join-Path $PSScriptRoot 'tests/Invoke-Tests.ps1') @params
