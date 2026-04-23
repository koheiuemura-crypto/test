# Opens the Google Sheet URL in the default external browser.
$ErrorActionPreference = 'Stop'
$url = & (Join-Path $PSScriptRoot 'get-google-sheet-url.ps1') | Select-Object -Last 1
Start-Process $url
