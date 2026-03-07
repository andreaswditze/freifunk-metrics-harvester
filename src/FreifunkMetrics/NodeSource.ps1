# Functions for this concern are loaded by FreifunkMetrics.psm1.



function Test-ValidNodeRow {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [pscustomobject]$Row
    )

    if ([string]::IsNullOrWhiteSpace([string]$Row.DeviceID)) { return $false }
    if ([string]::IsNullOrWhiteSpace([string]$Row.Name)) { return $false }
    if ([string]::IsNullOrWhiteSpace([string]$Row.IP)) { return $false }
    if ([string]::IsNullOrWhiteSpace([string]$Row.Domain)) { return $false }

    return $true
}

function Test-NodeReleaseSupported {
    [CmdletBinding()]
    param(
        [AllowNull()]
        [string]$Release,
        [version]$MinimumRelease = [version]'1.5.0'
    )

    $trimmed = Convert-ToTrimmedString -Value $Release
    if ([string]::IsNullOrWhiteSpace($trimmed)) {
        return $false
    }

    $normalized = $trimmed
    if ($normalized.StartsWith('v', [System.StringComparison]::OrdinalIgnoreCase)) {
        $normalized = $normalized.Substring(1)
    }

    $parsedVersion = $null
    if (-not [version]::TryParse($normalized, [ref]$parsedVersion)) {
        return $false
    }

    return $parsedVersion -ge $MinimumRelease
}

function Get-NormalizedIP {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$IP
    )

    $trimmed = $IP.Trim()
    $sanitized = $trimmed.Trim('[', ']')
    return $sanitized
}

function Get-RowValue {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object]$Row,
        [Parameter(Mandatory = $true)]
        [string[]]$Candidates
    )

    $propertyMap = @{}
    foreach ($prop in $Row.PSObject.Properties) {
        $propertyMap[$prop.Name.ToLowerInvariant()] = $prop.Value
    }

    foreach ($candidate in $Candidates) {
        $lookup = $candidate.ToLowerInvariant()
        if ($propertyMap.ContainsKey($lookup)) {
            return [string]$propertyMap[$lookup]
        }
    }

    return ''
}

function Resolve-NodeSourceFiles {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Config
    )

    $resolved = New-Object System.Collections.Generic.List[string]

    foreach ($path in @($Config.ExcelInputFiles)) {
        if ([string]::IsNullOrWhiteSpace([string]$path)) { continue }
        if (Test-Path -Path $path -PathType Leaf) {
            $resolved.Add((Resolve-Path -Path $path).Path)
        }
        else {
            Write-Log -Level WARN -Message "Excel file path missing: ${path}"
        }
    }
    $extensions = @('*.xlsx', '*.xlsm', '*.xls', '*.csv')
    foreach ($dir in @($Config.ExcelInputDirectories)) {
        if ([string]::IsNullOrWhiteSpace([string]$dir)) { continue }
        if (-not (Test-Path -Path $dir -PathType Container)) {
            Write-Log -Level WARN -Message "Excel input directory missing: ${dir}"
            continue
        }

        foreach ($pattern in $extensions) {
            $items = Get-ChildItem -Path $dir -Filter $pattern -File -Recurse:$([bool]$Config.ExcelSearchRecurse) -ErrorAction SilentlyContinue
            foreach ($item in $items) {
                $resolved.Add($item.FullName)
            }
        }
    }

    return @($resolved | Sort-Object -Unique)
}

function Get-TestNodesFromConfig {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Config
    )

    $normalized = @(
        @($Config.TestNodeIPs) |
            ForEach-Object { Get-NormalizedIP -IP ([string]$_) } |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
            Select-Object -Unique
    )

    $nodes = New-Object System.Collections.Generic.List[object]
    $index = 1
    foreach ($ip in $normalized) {
        $nodes.Add([pscustomobject]@{
            DeviceID = 'test-{0:d3}' -f $index
            Name     = 'TestNode-{0:d3}' -f $index
            IP       = $ip
            Domain   = 'testing'
        })
        $index++
    }

    return [pscustomobject]@{
        Nodes       = $nodes.ToArray()
        SourceFiles = @('<test-node-ips>')
    }
}

function Import-NodeListFromExcel {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Config
    )

    $allNodes = New-Object System.Collections.Generic.List[object]
    $sourceFiles = @(Resolve-NodeSourceFiles -Config $Config)

    if (-not $sourceFiles -or $sourceFiles.Count -eq 0) {
        throw 'No node source files found. Use ExcelInputFiles and/or ExcelInputDirectories in config.'
    }

    $importExcelAvailable = $false
    if (Get-Module -ListAvailable -Name ImportExcel) {
        $importExcelAvailable = $true
        $previousWarningPreference = $WarningPreference
        try {
            $WarningPreference = 'SilentlyContinue'
            Import-Module ImportExcel -ErrorAction Stop 3>$null | Out-Null
        }
        finally {
            $WarningPreference = $previousWarningPreference
        }
    }

    foreach ($filePath in $sourceFiles) {
        if (-not (Test-Path -Path $filePath -PathType Leaf)) {
            Write-Log -Level WARN -Message "Excel source missing: ${filePath}"
            continue
        }

        $extension = [IO.Path]::GetExtension($filePath).ToLowerInvariant()
        $baseName = [IO.Path]::GetFileName($filePath)
        $rows = @()

        if ($baseName -match '(?i)^vorlage_.*\\.xlsx$') {
            Write-Log -Level WARN -Message "Skipping template workbook by naming rule: ${filePath}"
            continue
        }

        if ($extension -eq '.csv') {
            $rows = Import-Csv -Path $filePath
        }
        elseif ($extension -in @('.xlsx', '.xlsm', '.xls')) {
            if (-not $importExcelAvailable) {
                throw 'Module ImportExcel is required for .xlsx imports. Install with: Install-Module ImportExcel -Scope CurrentUser'
            }

            try {
                $rows = Import-Excel -Path $filePath -WarningAction SilentlyContinue 3>$null
            }
            catch {
                $message = $_.Exception.Message
                if ($message -match 'No column headers found on top row') {
                    Write-Log -Level WARN -Message "Skipping workbook without header row: ${filePath}"
                    continue
                }

                throw
            }
        }
        else {
            Write-Log -Level WARN -Message "Skipping unsupported file extension: ${filePath}"
            continue
        }

        $imported = 0
        $skipped = 0

        foreach ($row in $rows) {
            $node = [pscustomobject]@{
                DeviceID = (Get-RowValue -Row $row -Candidates @('DeviceID', 'DeviceId', 'ID', 'NodeID', 'NodeId'))
                Name     = (Get-RowValue -Row $row -Candidates @('Name', 'Hostname', 'NodeName'))
                IP       = (Get-RowValue -Row $row -Candidates @('IP', 'IPv4', 'Address', 'NodeIP'))
                Domain   = (Get-RowValue -Row $row -Candidates @('Domain', 'Segment', 'Community'))
                Release  = (Get-RowValue -Row $row -Candidates @('Release'))
            }

            if (-not (Test-ValidNodeRow -Row $node)) {
                $skipped++
                continue
            }

            if (-not (Test-NodeReleaseSupported -Release $node.Release)) {
                $skipped++
                continue
            }

            $node.IP = Get-NormalizedIP -IP $node.IP
            $allNodes.Add($node)
            $imported++
        }

        Write-Log -Message "Excel import done for ${filePath}: imported=${imported}, skipped=${skipped}"
    }

    $uniqueNodes = $allNodes |
        Group-Object -Property DeviceID, IP |
        ForEach-Object { $_.Group[0] }

    return [pscustomobject]@{
        Nodes       = @($uniqueNodes)
        SourceFiles = $sourceFiles
    }
}

