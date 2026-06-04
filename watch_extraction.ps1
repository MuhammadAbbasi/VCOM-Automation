# watch_extraction.ps1
# Stops the Docker extraction container and runs vcom_monitor.py locally
# with a visible browser window so you can watch the scraping live.
#
# Usage:
#   .\watch_extraction.ps1
#
# To stop: Ctrl+C in this terminal. Docker extraction resumes automatically.

$ROOT = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ROOT

Write-Host ""
Write-Host "=================================================" -ForegroundColor Cyan
Write-Host "  VCOM Extraction — Visible Browser Mode" -ForegroundColor Cyan
Write-Host "=================================================" -ForegroundColor Cyan
Write-Host ""

# Check Docker is running
$dockerRunning = docker info 2>&1 | Select-String "Containers"
if (-not $dockerRunning) {
    Write-Host "WARNING: Docker doesn't seem to be running. Continuing anyway..." -ForegroundColor Yellow
}

# Pause the extraction container so there's no conflict
$containerExists = docker ps -q -f name=scada_extraction
if ($containerExists) {
    Write-Host "Pausing Docker extraction container..." -ForegroundColor Yellow
    docker stop scada_extraction | Out-Null
    Write-Host "  Docker extraction stopped." -ForegroundColor Green
} else {
    Write-Host "  Docker extraction container not running — OK." -ForegroundColor Gray
}

Write-Host ""
Write-Host "Starting local extraction with visible browser..." -ForegroundColor Green
Write-Host "Watch the Chromium window open and navigate to VCOM." -ForegroundColor White
Write-Host "Press Ctrl+C to stop and restart Docker extraction." -ForegroundColor White
Write-Host ""

# Load .env variables from the Docker env file
$envFile = Join-Path $ROOT "VCOM Automation Docker\.env"
if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object {
        if ($_ -match '^\s*([^#][^=]+)=(.*)$') {
            $name  = $matches[1].Trim()
            $value = $matches[2].Trim()
            [System.Environment]::SetEnvironmentVariable($name, $value, "Process")
        }
    }
    Write-Host "  Loaded credentials from .env" -ForegroundColor Gray
}

# Force headless OFF for this session
$env:VCOM_HEADLESS = "false"

try {
    python "$ROOT\vcom_monitor.py"
} finally {
    Write-Host ""
    Write-Host "Local extraction stopped. Restarting Docker extraction..." -ForegroundColor Yellow
    $compose = Join-Path $ROOT "VCOM Automation Docker"
    Set-Location $compose
    docker compose start extraction | Out-Null
    Write-Host "  Docker extraction restarted." -ForegroundColor Green
    Set-Location $ROOT
}
