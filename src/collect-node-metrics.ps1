<#
.SYNOPSIS
Collects distributed Freifunk node performance measurements in two phases.

.DESCRIPTION
Thin entry point that imports the FreifunkMetrics module and runs the collector.
#>
[CmdletBinding()]
param(
    [string]$ConfigPath,
    [string]$RunId,
    [switch]$VerboseLogging,
    [switch]$NoRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

Import-Module (Join-Path $PSScriptRoot 'FreifunkMetrics.psm1') -Force | Out-Null

if (-not $NoRun) {
    try {
        Invoke-CollectNodeMetricsMain -ConfigPath $ConfigPath -RunId $RunId -VerboseLogging:$VerboseLogging
    }
    catch {
        Write-Error $_
        exit 1
    }
}
