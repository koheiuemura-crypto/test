# Copies the validated Google Sheet URL from google-sheet-config.json into workspace settings
# (googleSheets.sheetUrl) so task variables like ${config:googleSheets.sheetUrl} can open Simple Browser.
$ErrorActionPreference = 'Stop'
$settingsPath = Join-Path $PSScriptRoot 'settings.json'
if (-not (Test-Path -LiteralPath $settingsPath)) {
    Write-Error "settings.json not found: $settingsPath"
}
$url = & (Join-Path $PSScriptRoot 'get-google-sheet-url.ps1') | Select-Object -Last 1
$raw = Get-Content -LiteralPath $settingsPath -Raw -Encoding UTF8
$j = $raw | ConvertFrom-Json
if (-not $j.PSObject.Properties['googleSheets']) {
    $j | Add-Member -NotePropertyName 'googleSheets' -Value ([pscustomobject]@{})
}
$gs = $j.googleSheets
if ($null -eq $gs) {
    $j.googleSheets = [pscustomobject]@{}
    $gs = $j.googleSheets
}
$gs | Add-Member -NotePropertyName 'sheetUrl' -Value $url -Force
$text = $j | ConvertTo-Json -Depth 30
[System.IO.File]::WriteAllText(
    (Resolve-Path -LiteralPath $settingsPath).Path,
    $text,
    (New-Object System.Text.UTF8Encoding -ArgumentList $true)
)
Write-Output "Updated googleSheets.sheetUrl in settings.json (length: $($url.Length) chars)."
