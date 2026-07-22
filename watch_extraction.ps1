# watch_extraction.ps1
# Runs vcom_monitor.py locally with a visible browser window so you can watch
# the scraping live. Stop any other running extraction (e.g. via run_monitor.py)
# first to avoid two concurrent Playwright sessions.
#
# Usage:
#   .\watch_extraction.ps1
#
# To stop: Ctrl+C in this terminal.

$ROOT = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ROOT

Write-Host ""
Write-Host "=================================================" -ForegroundColor Cyan
Write-Host "  VCOM Extraction — Visible Browser Mode" -ForegroundColor Cyan
Write-Host "=================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Watch the Chromium window open and navigate to VCOM." -ForegroundColor White
Write-Host "Press Ctrl+C to stop." -ForegroundColor White
Write-Host ""

# Force headless OFF for this session
$env:VCOM_HEADLESS = "false"

python "$ROOT\vcom_monitor.py"
