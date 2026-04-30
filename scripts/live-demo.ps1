$ErrorActionPreference = "Stop"

$ResetDb = $false
$Cleanup = $false
$CleanupAll = $false

foreach ($argument in $args) {
    switch ($argument) {
        { $_ -in @("--reset-db", "-ResetDb") } {
            $ResetDb = $true
            break
        }
        { $_ -in @("--cleanup", "-Cleanup") } {
            $Cleanup = $true
            break
        }
        { $_ -in @("--cleanup-all", "-CleanupAll") } {
            $CleanupAll = $true
            break
        }
        { $_ -in @("-h", "--help") } {
            Write-Host "Usage: .\scripts\live-demo.ps1 [--reset-db] [--cleanup] [--cleanup-all]"
            exit 0
        }
        default {
            Write-Host "Unknown option: $argument" -ForegroundColor Red
            exit 2
        }
    }
}

$RepoRoot = Split-Path -Parent $PSScriptRoot
$VenvPython = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$DatabaseUrl = "postgresql://auction_user:localdevpassword@127.0.0.1:5433/auction_etl"

function Write-Step {
    param([string]$Message)
    Write-Host ""
    Write-Host "==> $Message"
}

function Invoke-Checked {
    param(
        [string]$Command,
        [string[]]$Arguments = @()
    )

    & $Command @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code ${LASTEXITCODE}: $Command $($Arguments -join ' ')"
    }
}

function Get-Python {
    $candidates = @(
        @{ Command = "py"; Arguments = @("-3.11") },
        @{ Command = "python"; Arguments = @() },
        @{ Command = "python3"; Arguments = @() }
    )

    foreach ($candidate in $candidates) {
        $command = $candidate.Command
        $arguments = [string[]]$candidate.Arguments

        try {
            & $command @arguments -c "import sys; raise SystemExit(sys.version_info < (3, 11))" *> $null
            if ($LASTEXITCODE -eq 0) {
                return @{
                    Command = $command
                    Arguments = $arguments
                }
            }
        }
        catch {
            continue
        }
    }

    throw "Python 3.11 or newer was not found."
}

function Invoke-Python {
    param([string[]]$Arguments)
    Invoke-Checked -Command $VenvPython -Arguments $Arguments
}

function Invoke-AuctionEtl {
    param([string[]]$Arguments)
    Invoke-Python -Arguments (@("-m", "app.cli") + $Arguments)
}

function Invoke-DockerCompose {
    param([string[]]$Arguments)
    Invoke-Checked -Command "docker" -Arguments (@("compose") + $Arguments)
}

function Stop-DemoPostgres {
    Write-Step "Stopping Postgres container"
    try {
        & docker compose stop postgres
        if ($LASTEXITCODE -ne 0) {
            Write-Warning "Could not stop Postgres container."
        }
    }
    catch {
        Write-Warning "Could not stop Postgres container: $_"
    }
}

function Remove-DemoEnvironment {
    Write-Step "Removing demo database volume and virtual environment"
    try {
        & docker compose down -v
        if ($LASTEXITCODE -ne 0) {
            Write-Warning "Could not remove Docker Compose resources."
        }
    }
    catch {
        Write-Warning "Could not remove Docker Compose resources: $_"
    }

    $venvPath = Join-Path $RepoRoot ".venv"
    if (Test-Path $venvPath) {
        try {
            Remove-Item -LiteralPath $venvPath -Recurse -Force
        }
        catch {
            Write-Warning "Could not remove .venv: $_"
        }
    }
}

$exitCode = 0

try {
    Set-Location $RepoRoot

    Write-Step "Checking prerequisites"
    $python = Get-Python
    Invoke-Checked -Command "docker" -Arguments @("compose", "version")

    if ($ResetDb) {
        Write-Step "Resetting local demo database volume"
        Invoke-DockerCompose -Arguments @("down", "-v")
    }

    if (-not (Test-Path $VenvPython)) {
        Write-Step "Creating .venv"
        Invoke-Checked -Command $python.Command -Arguments ([string[]]$python.Arguments + @("-m", "venv", ".venv"))
    }

    Write-Step "Installing Python dependencies"
    Invoke-Python -Arguments @("-m", "pip", "install", "-r", "requirements.txt")

    Write-Step "Installing Playwright Chromium"
    Invoke-Python -Arguments @("-m", "playwright", "install", "chromium")

    Write-Step "Starting Postgres"
    Invoke-DockerCompose -Arguments @("up", "-d", "postgres")

    $env:DATABASE_URL = $DatabaseUrl

    Write-Step "Waiting for Postgres"
    $waitForPostgres = @"
import os
import sys
import time

import psycopg

deadline = time.time() + 60
last_error = None

while time.time() < deadline:
    try:
        with psycopg.connect(os.environ["DATABASE_URL"]):
            print("Postgres is ready.")
            sys.exit(0)
    except Exception as exc:
        last_error = exc
        time.sleep(2)

print(f"Postgres did not become ready: {last_error}", file=sys.stderr)
sys.exit(1)
"@
    Invoke-Python -Arguments @("-c", $waitForPostgres)

    Write-Step "Running tests"
    Invoke-Python -Arguments @("-m", "pytest", "-q")

    Write-Step "Running Bring a Trailer discovery batch"
    Invoke-AuctionEtl -Arguments @("bat", "discover", "--max-candidates", "5")
    Invoke-AuctionEtl -Arguments @("bat", "ingest-discovered", "--batch-size", "5")
    Invoke-AuctionEtl -Arguments @("bat", "transform-discovered", "--batch-size", "5")

    Write-Step "Running Cars & Bids discovery batch"
    Invoke-AuctionEtl -Arguments @("cab", "discover", "--max-candidates", "5")
    Invoke-AuctionEtl -Arguments @("cab", "ingest-discovered", "--batch-size", "5")
    Invoke-AuctionEtl -Arguments @("cab", "transform-discovered", "--batch-size", "5")

    Write-Step "Live demo completed"
}
catch {
    $exitCode = 1
    Write-Host "ERROR: $_" -ForegroundColor Red
}
finally {
    if ($CleanupAll) {
        Remove-DemoEnvironment
    }
    elseif ($Cleanup) {
        Stop-DemoPostgres
    }
}

exit $exitCode
