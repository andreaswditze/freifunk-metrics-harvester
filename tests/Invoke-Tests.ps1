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

function Resolve-TestConfigPath {
    [CmdletBinding()]
    param(
        [string]$ProjectRoot,
        [string]$RequestedPath
    )

    $candidates = @()
    if (-not [string]::IsNullOrWhiteSpace($RequestedPath)) {
        $candidates += $RequestedPath
    }

    $candidates += @(
        (Join-Path $ProjectRoot 'src/config.production.ps1'),
        (Join-Path $ProjectRoot 'src/config.development.ps1'),
        (Join-Path $ProjectRoot 'src/config.development.example.ps1'),
        (Join-Path $ProjectRoot 'src/config.demo.ps1')
    )

    foreach ($candidate in $candidates) {
        if (-not [string]::IsNullOrWhiteSpace($candidate) -and (Test-Path -Path $candidate -PathType Leaf)) {
            return (Resolve-Path -Path $candidate).Path
        }
    }

    throw 'No config file found for tests requiring TestNodeIPs. Provide -ConfigPath or create src/config.production.ps1.'
}

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
Set-Location -Path $projectRoot

if ($InstallModules -and -not (Get-Module -ListAvailable -Name Pester)) {
    Install-Module Pester -Scope CurrentUser -Force
}

if (-not (Get-Module -ListAvailable -Name Pester)) {
    throw 'Pester module is not installed. Run: Install-Module Pester -Scope CurrentUser -Force'
}

$invokeParams = @{
    Path = (Join-Path $projectRoot 'tests')
    CI   = $true
}

if ($RunSshStreaming) {
    $invokeParams['TagFilter'] = @('ssh-streaming')
}
elseif (-not $IncludeIntegration) {
    $invokeParams['ExcludeTagFilter'] = @('integration', 'ssh-streaming')
}
else {
    $invokeParams['ExcludeTagFilter'] = @('ssh-streaming')
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

$previousConfigPath = $env:FFMH_TEST_CONFIG_PATH
try {
    if ($RunSshStreaming) {
        $env:FFMH_TEST_CONFIG_PATH = Resolve-TestConfigPath -ProjectRoot $projectRoot -RequestedPath $ConfigPath
    }

    Invoke-Pester @invokeParams
}
finally {
    if ($null -eq $previousConfigPath) {
        Remove-Item Env:FFMH_TEST_CONFIG_PATH -ErrorAction SilentlyContinue
    }
    else {
        $env:FFMH_TEST_CONFIG_PATH = $previousConfigPath
    }
}
