<#
.SYNOPSIS
Shows SQLite content for the Freifunk metrics harvester.

.DESCRIPTION
Loads the configured database path and prints either all core tables,
a single table, or a custom SQLite query.
#>
[CmdletBinding()]
param(
    [string]$ConfigPath,
    [string]$Table,
    [string]$Query
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

. "$PSScriptRoot/collect-node-metrics.ps1" -NoRun

$config = Get-EnvironmentConfig -RequestedPath $ConfigPath

if (-not (Test-Path -Path $config.DatabasePath -PathType Leaf)) {
    throw "Database not found: $($config.DatabasePath)"
}

$tables = @('nodes', 'runs', 'node_jobs', 'measurements')

if ($Query) {
    & $config.SQLiteBinary -header -column $config.DatabasePath $Query
    exit $LASTEXITCODE
}

if ($Table) {
    if ($tables -notcontains $Table) {
        throw "Unsupported table: $Table"
    }

    Write-Host ("=== {0} ===" -f $Table)
    & $config.SQLiteBinary -header -column $config.DatabasePath ("SELECT * FROM {0};" -f $Table)
    exit $LASTEXITCODE
}

foreach ($currentTable in $tables) {
    Write-Host ("=== {0} ===" -f $currentTable)
    & $config.SQLiteBinary -header -column $config.DatabasePath ("SELECT * FROM {0};" -f $currentTable)
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }

    Write-Host ''
}