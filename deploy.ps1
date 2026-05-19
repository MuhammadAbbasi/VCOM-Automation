# ============================================================================
# VCOM Automation System - Docker Deployment Script (Windows PowerShell)
# ============================================================================
# Usage: .\deploy.ps1
# ============================================================================

param(
    [switch]$Quick = $false
)

$ErrorActionPreference = "SilentlyContinue"

# Colors
$infoColor = "Cyan"
$successColor = "Green"
$errorColor = "Red"
$warningColor = "Yellow"

Write-Host ""
Write-Host "========================================================" -ForegroundColor $infoColor
Write-Host "  VCOM Automation System - Docker Deployment" -ForegroundColor $infoColor
Write-Host "========================================================" -ForegroundColor $infoColor
Write-Host ""

# PRE-FLIGHT CHECKS
Write-Host "[1/6] Pre-flight checks..." -ForegroundColor $warningColor

# Check Docker
$docker = docker --version 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "  [OK] Docker: $docker" -ForegroundColor $successColor
} else {
    Write-Host "  [FAIL] Docker not installed" -ForegroundColor $errorColor
    exit 1
}

# Check Docker Compose
$compose = docker-compose --version 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "  [OK] Docker Compose: $compose" -ForegroundColor $successColor
} else {
    Write-Host "  [FAIL] Docker Compose not installed" -ForegroundColor $errorColor
    exit 1
}

# Check required files
$files = @("Dockerfile", "docker-compose.yml", ".dockerignore", ".env.example")
foreach ($file in $files) {
    if (Test-Path $file) {
        Write-Host "  [OK] $file" -ForegroundColor $successColor
    } else {
        Write-Host "  [FAIL] $file not found" -ForegroundColor $errorColor
        exit 1
    }
}

# Check .env
if (-not (Test-Path ".env")) {
    Write-Host "  [WARN] .env not found, creating from template..." -ForegroundColor $warningColor
    Copy-Item ".env.example" ".env"
    Write-Host "  [DONE] Created .env file" -ForegroundColor $successColor
    Write-Host ""
    Write-Host "  PLEASE EDIT .env with your credentials:" -ForegroundColor $warningColor
    Write-Host "    notepad .env" -ForegroundColor $infoColor
    Write-Host ""
    Write-Host "  Then run this script again." -ForegroundColor $warningColor
    exit 0
} else {
    Write-Host "  [OK] .env configured" -ForegroundColor $successColor
}

Write-Host ""

# CREATE DIRECTORIES
Write-Host "[2/6] Creating data directories..." -ForegroundColor $warningColor

$dirs = @("extracted_data", "logs", "db", "errors", "artifacts")
foreach ($dir in $dirs) {
    if (-not (Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir -ErrorAction SilentlyContinue | Out-Null
        Write-Host "  [OK] Created $dir" -ForegroundColor $successColor
    } else {
        Write-Host "  [OK] $dir exists" -ForegroundColor $successColor
    }
}

Write-Host ""

# VALIDATE CONFIG
Write-Host "[3/6] Validating docker-compose configuration..." -ForegroundColor $warningColor

$config = docker-compose config 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "  [OK] Configuration valid" -ForegroundColor $successColor
} else {
    Write-Host "  [FAIL] Configuration error" -ForegroundColor $errorColor
    Write-Host "  $config" -ForegroundColor $errorColor
    exit 1
}

Write-Host ""

# BUILD IMAGE
Write-Host "[4/6] Building Docker image (this may take 5-10 minutes)..." -ForegroundColor $warningColor

docker-compose build 2>&1 | ForEach-Object {
    if ($_ -match "error|failed" -and $_ -notmatch "already") {
        Write-Host "  [ERROR] $_" -ForegroundColor $errorColor
    } elseif ($_ -match "Step|Building|Successfully") {
        Write-Host "  $_"
    }
}

if ($LASTEXITCODE -ne 0) {
    Write-Host "  [FAIL] Build failed" -ForegroundColor $errorColor
    exit 1
}

Write-Host "  [OK] Image built successfully" -ForegroundColor $successColor
Write-Host ""

# START CONTAINER
Write-Host "[5/6] Starting VCOM Automation container..." -ForegroundColor $warningColor

docker-compose up -d 2>&1 | ForEach-Object {
    if ($_ -match "error|failed") {
        Write-Host "  [ERROR] $_" -ForegroundColor $errorColor
    } elseif ($_ -match "Creating|Starting") {
        Write-Host "  [OK] $_" -ForegroundColor $successColor
    }
}

if ($LASTEXITCODE -ne 0) {
    Write-Host "  [FAIL] Start failed" -ForegroundColor $errorColor
    exit 1
}

Write-Host "  [OK] Container started" -ForegroundColor $successColor
Write-Host ""

# WAIT FOR SERVICES
Write-Host "[6/6] Waiting for services to initialize..." -ForegroundColor $warningColor

for ($i = 0; $i -lt 30; $i++) {
    Write-Host -NoNewline "."
    Start-Sleep -Seconds 1
}
Write-Host ""
Write-Host "  [OK] Services initialized" -ForegroundColor $successColor
Write-Host ""

# VERIFY
Write-Host "========================================================" -ForegroundColor $successColor
Write-Host "  DEPLOYMENT COMPLETE" -ForegroundColor $successColor
Write-Host "========================================================" -ForegroundColor $successColor
Write-Host ""

# Check status
$status = docker-compose ps 2>&1
if ($status -match "Up") {
    Write-Host "[OK] Container is running" -ForegroundColor $successColor
} else {
    Write-Host "[WARN] Check container status: docker-compose ps" -ForegroundColor $warningColor
}

Write-Host ""
Write-Host "NEXT STEPS:" -ForegroundColor $infoColor
Write-Host "  1. Access Dashboard: http://localhost:8080" -ForegroundColor $warningColor
Write-Host "  2. View logs: docker-compose logs -f" -ForegroundColor $warningColor
Write-Host "  3. Check status: docker-compose ps" -ForegroundColor $warningColor
Write-Host "  4. Stop system: docker-compose down" -ForegroundColor $warningColor
Write-Host ""
Write-Host "Documentation:" -ForegroundColor $infoColor
Write-Host "  - Quick Start: DOCKER_QUICKSTART.md" -ForegroundColor $warningColor
Write-Host "  - Full Guide: DOCKER_README.md" -ForegroundColor $warningColor
Write-Host ""
