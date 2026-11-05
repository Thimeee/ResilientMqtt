<#
.SYNOPSIS
  Runs the complete ResilientMqtt test campaign: unit + integration, network chaos + cross-broker + TLS,
  reconnect storm (network cut and broker restart) and soak. Writes everything to tests\test-results\<timestamp>\
  and a .zip next to it.

.EXAMPLE
  .\run-all.ps1 -Smoke                 # ~15 min sanity run (small numbers, Mosquitto only)
  .\run-all.ps1                        # full campaign: 500-client storms + 24 h soak + EMQX/HiveMQ
  .\run-all.ps1 -SoakHours 8           # shorter soak
  .\run-all.ps1 -SkipSoak              # everything except the soak
  .\run-all.ps1 -SoakOnly -SoakHours 5 # only the soak, 5 hours (then e.g. -SoakOnly -SoakHours 24)

  Needs: Windows, .NET 10 SDK, Docker Desktop (running). Works in Windows PowerShell 5.1 and PowerShell 7.
#>
param(
    [int]$Clients = 500,
    [int]$OutageSeconds = 30,
    [double]$SoakHours = 24,
    [int]$SoakMinutes = 0,
    [int]$SoakClients = 100,
    [double]$FaultEveryMinutes = 10,
    [switch]$Smoke,
    [switch]$SkipFull,          # Mosquitto only (no EMQX / HiveMQ)
    [switch]$SkipUnit,
    [switch]$SkipChaos,
    [switch]$SkipStorm,
    [switch]$SkipSoak,
    [switch]$SoakOnly,          # only the soak stage (skips unit, chaos and storms)
    [switch]$KeepEnvironment    # leave the containers running afterwards
)

# 'Continue', not 'Stop': in Windows PowerShell 5.1 a native tool writing progress to stderr (docker compose does)
# would otherwise abort the script. Failures are detected through $LASTEXITCODE instead.
$ErrorActionPreference = 'Continue'
$testsDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$chaosDir = Join-Path $testsDir 'chaos'
$certDir = Join-Path $chaosDir 'certs'
$stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$out = Join-Path $testsDir "test-results\$stamp"
New-Item -ItemType Directory -Force $out | Out-Null

if ($Smoke) {
    $Clients = 30; $OutageSeconds = 10; $SoakMinutes = 4; $SoakClients = 10; $FaultEveryMinutes = 1
    if (-not $PSBoundParameters.ContainsKey('SkipFull')) { $SkipFull = $true }
}
if ($SoakOnly) {
    $SkipUnit = $true; $SkipChaos = $true; $SkipStorm = $true
    if (-not $PSBoundParameters.ContainsKey('SkipFull')) { $SkipFull = $true }   # the soak uses Mosquitto only
}
$brokers = if ($SkipFull) { 'mosquitto' } else { 'mosquitto,emqx,hivemq' }
$soakArgs = if ($SoakMinutes -gt 0) { "--minutes $SoakMinutes" } else { "--hours $SoakHours" }

Start-Transcript -Path (Join-Path $out 'run-all.log') | Out-Null
$results = New-Object System.Collections.ArrayList

# Keep the PC awake while the campaign runs (a sleeping PC would fail the soak).
Add-Type -Namespace RMqtt -Name Power -MemberDefinition '[DllImport("kernel32.dll")] public static extern uint SetThreadExecutionState(uint esFlags);' -ErrorAction SilentlyContinue
[RMqtt.Power]::SetThreadExecutionState([uint32]'0x80000001') | Out-Null   # ES_CONTINUOUS | ES_SYSTEM_REQUIRED

function Invoke-Stage([string]$Name, [scriptblock]$Block) {
    Write-Host ""
    Write-Host "================ $Name ================" -ForegroundColor Cyan
    $sw = [Diagnostics.Stopwatch]::StartNew()
    $global:LASTEXITCODE = 0
    try { & $Block; $code = $global:LASTEXITCODE } catch { Write-Host $_ -ForegroundColor Red; $code = 99 }
    $sw.Stop()
    $verdict = if ($code -eq 0) { 'PASS' } else { "FAIL (exit $code)" }
    $color = if ($code -eq 0) { 'Green' } else { 'Red' }
    Write-Host "---- $Name : $verdict in $([math]::Round($sw.Elapsed.TotalMinutes,1)) min" -ForegroundColor $color
    [void]$results.Add([pscustomobject]@{ Stage = $Name; Result = $verdict; Minutes = [math]::Round($sw.Elapsed.TotalMinutes, 1) })
}

function Wait-Port([int]$Port, [int]$Seconds) {
    $deadline = (Get-Date).AddSeconds($Seconds)
    while ((Get-Date) -lt $deadline) {
        try { $c = New-Object Net.Sockets.TcpClient; $c.Connect('127.0.0.1', $Port); $c.Close(); return $true } catch { Start-Sleep -Milliseconds 500 }
    }
    return $false
}

try {
    # ── 0. prerequisites ────────────────────────────────────────────────
    Write-Host "Results: $out"
    $sdks = (& dotnet --list-sdks) -join "`n"
    if ($sdks -notmatch '(^|\n)10\.') { throw ".NET 10 SDK not found (dotnet --list-sdks)." }
    & docker info --format '{{.ServerVersion}}' | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Docker is not running — start Docker Desktop and try again." }

    # ── 1. build ────────────────────────────────────────────────────────
    Push-Location $testsDir
    & dotnet build ResilientMqtt.Tests -c Release --nologo -v q
    if ($LASTEXITCODE -ne 0) { throw "build failed (ResilientMqtt.Tests)" }
    & dotnet build ResilientMqtt.ChaosTests -c Release --nologo -v q
    if ($LASTEXITCODE -ne 0) { throw "build failed (ResilientMqtt.ChaosTests)" }
    & dotnet build ResilientMqtt.LoadTest -c Release --nologo -v q
    if ($LASTEXITCODE -ne 0) { throw "build failed (ResilientMqtt.LoadTest)" }
    $loadTest = Join-Path $testsDir 'ResilientMqtt.LoadTest\bin\Release\net10.0\ResilientMqtt.LoadTest.dll'

    # ── 2. test PKI ─────────────────────────────
    & dotnet $loadTest certs $certDir
    if ($LASTEXITCODE -ne 0) { throw "certificate generation failed" }
    # (the Mosquitto password file is created inside the container - see chaos/docker-compose.yml)

    # ── 3. environment ──────────────────────────────────────────────────
    $compose = Join-Path $chaosDir 'docker-compose.yml'
    & docker compose -p rmqtt-test -f $compose down -v 2>$null | Out-Null
    if ($SkipFull) { & docker compose -p rmqtt-test -f $compose up -d }
    else { & docker compose -p rmqtt-test -f $compose --profile full up -d }
    if ($LASTEXITCODE -ne 0) { throw "docker compose up failed" }
    $ports = @(18831, 18832, 18833, 18474)
    if (-not $SkipFull) { $ports += 18841, 18851 }
    foreach ($p in $ports) {
        if (-not (Wait-Port $p 180)) { throw "port $p did not open — check: docker compose -p rmqtt-test logs" }
    }
    if (-not $SkipFull) { Start-Sleep -Seconds 20 }   # EMQX / HiveMQ accept TCP before MQTT is fully up

    $env:RMQTT_CHAOS = '1'
    $env:RMQTT_BROKERS = $brokers
    $env:RMQTT_CERTS = $certDir

    # ── 4. stages ───────────────────────────────────────────────────────
    if (-not $SkipUnit) {
        Invoke-Stage 'Unit + integration (in-process broker)' {
            & dotnet test ResilientMqtt.Tests -c Release --no-build --nologo --logger "trx;LogFileName=unit.trx" --results-directory $out
        }
    }
    if (-not $SkipChaos) {
        Invoke-Stage "Chaos + cross-broker + TLS ($brokers)" {
            & dotnet test ResilientMqtt.ChaosTests -c Release --no-build --nologo --logger "trx;LogFileName=chaos.trx" --logger "console;verbosity=normal" --results-directory $out
        }
    }
    if (-not $SkipStorm) {
        Invoke-Stage "Storm: $Clients clients, network cut $OutageSeconds s" {
            & dotnet $loadTest storm --clients $Clients --outage $OutageSeconds --mode proxy --out $out
        }
        Invoke-Stage "Storm: $Clients clients, broker restart ($OutageSeconds s down)" {
            & dotnet $loadTest storm --clients $Clients --outage $OutageSeconds --mode restart --out $out
        }
    }
    if (-not $SkipSoak) {
        $label = if ($SoakMinutes -gt 0) { "$SoakMinutes min" } else { "$SoakHours h" }
        $soakSpan = if ($SoakMinutes -gt 0) { [TimeSpan]::FromMinutes($SoakMinutes) } else { [TimeSpan]::FromHours($SoakHours) }
        $endsAt = (Get-Date).Add($soakSpan).AddMinutes(8)
        Write-Host "Soak starts now; expected to finish around $($endsAt.ToString('yyyy-MM-dd HH:mm')) (+ a few minutes to settle)." -ForegroundColor Yellow
        Invoke-Stage "Soak: $SoakClients clients, $label, a fault every $FaultEveryMinutes min" {
            & dotnet $loadTest soak --clients $SoakClients ($soakArgs.Split(' ')) --fault-every $FaultEveryMinutes --out $out
        }
    }
}
catch {
    Write-Host "SETUP ERROR: $_" -ForegroundColor Red
    [void]$results.Add([pscustomobject]@{ Stage = 'Setup'; Result = "ERROR: $_"; Minutes = 0 })
}
finally {
    Pop-Location -ErrorAction SilentlyContinue
    foreach ($c in 'rmqtt-mosquitto', 'rmqtt-toxiproxy', 'rmqtt-emqx', 'rmqtt-hivemq') {
        & docker logs $c *> (Join-Path $out "$c.log")
    }
    if (-not $KeepEnvironment) { & docker compose -p rmqtt-test -f (Join-Path $chaosDir 'docker-compose.yml') --profile full down -v 2>$null | Out-Null }
    [RMqtt.Power]::SetThreadExecutionState([uint32]'0x80000000') | Out-Null

    $machine = "$env:COMPUTERNAME · $((Get-CimInstance Win32_Processor | Measure-Object NumberOfLogicalProcessors -Sum).Sum) cores · $([math]::Round((Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory/1GB)) GB"
    $lines = @("# ResilientMqtt test campaign — $stamp", "", "Machine: $machine", "", "| Stage | Result | Minutes |", "|---|---|---|")
    foreach ($r in $results) { $lines += "| $($r.Stage) | $($r.Result) | $($r.Minutes) |" }
    $lines += "", "Details: unit.trx, chaos.trx, storm-*.md, soak.md, soak-metrics.csv, *.log"
    $lines | Set-Content -Encoding UTF8 (Join-Path $out 'SUMMARY.md')
    Write-Host ""
    $lines | ForEach-Object { Write-Host $_ }
    Stop-Transcript | Out-Null
    Compress-Archive -Path (Join-Path $out '*') -DestinationPath "$out.zip" -Force
    Write-Host ""
    Write-Host "Bring back this file: $out.zip" -ForegroundColor Yellow
}
