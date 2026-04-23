# Reads sheetUrl from google-sheet-config.json, validates, writes URL to stdout (single line). Exit 0 or 1.
$ErrorActionPreference = 'Stop'
$configPath = Join-Path $PSScriptRoot 'google-sheet-config.json'
if (-not (Test-Path -LiteralPath $configPath)) {
    Write-Error "Config not found: $configPath" -ErrorAction Stop
}
$json = Get-Content -LiteralPath $configPath -Raw -Encoding UTF8 | ConvertFrom-Json
$url = $json.sheetUrl
if ([string]::IsNullOrWhiteSpace($url) -or ($url -match 'PLACEHOLDER')) {
    Write-Error 'Set sheetUrl in .vscode\google-sheet-config.json to your Google Sheet URL (not the PLACEHOLDER).'
}
Write-Output $url
