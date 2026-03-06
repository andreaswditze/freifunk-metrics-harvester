[CmdletBinding()]
param(
    [ValidateSet('None', 'NUnitXml', 'JUnitXml')]
    [string]$OutputFormat = 'None',
    [string]$OutputPath = '',
    [switch]$InstallModules
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
Set-Location -Path $projectRoot

if ($InstallModules -and -not (Get-Module -ListAvailable -Name Pester)) {
    Install-Module Pester -Scope CurrentUser -Force
}

if (-not (Get-Module -ListAvailable -Name Pester)) {
    throw 'Pester module is not installed. Run: Install-Module Pester -Scope CurrentUser -Force'
}

$invokeParams = @{
    Path   = (Join-Path $projectRoot 'tests')
    CI     = $true
}

if ($OutputFormat -ne 'None') {
    if ([string]::IsNullOrWhiteSpace($OutputPath)) {
        throw 'When OutputFormat is set, OutputPath is required.'
    }

    $outDir = Split-Path -Parent $OutputPath
    if ($outDir -and -not (Test-Path -Path $outDir -PathType Container)) {
        New-Item -ItemType Directory -Path $outDir -Force | Out-Null
    }

    $invokeParams['OutputFormat'] = $OutputFormat
    $invokeParams['OutputFile'] = $OutputPath
}

Invoke-Pester @invokeParams
