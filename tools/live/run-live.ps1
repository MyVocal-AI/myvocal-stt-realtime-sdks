<#
Bounded real-API validation (plan v1.1 section 7, R1-R6) from the INSTALLED candidates produced by
tools\matrix\run-matrix.ps1 (-Stage install): Python wheel-only venv, Java consumer classpath (isolated
Maven repository), Go consumer with replace -> clean candidate export.

Two phases:
  -Phase prepare   no credential needed: synthetic audio, compiles the Java/Go live harnesses.
  -Phase run       needs MYVOCAL_ACCESS_KEY in THIS process environment (supplied by the sanctioned
                   secure entry; never on a command line, in a file or in a log). Optional
                   MYVOCAL_API_BASE; a non-fixture key is refused for any base other than
                   https://api.myvocal.ai. Reads the balance at the start, runs R1-R5, then
                   reconcile.py (R6: History scan, settlement, repeated finish, balance at the end).

  powershell -ExecutionPolicy Bypass -File tools\live\run-live.ps1 -Phase prepare|run -Tools <runtimes dir> -Out <evidence dir>\live [-Idle 75] [-Only python,java,go,examples,reconcile] [-PyOnly r1,r3,r4,r5,diag] [-PyRedo <regex>] [-Ffmpeg <ffmpeg.exe>]
#>
param(
  [Parameter(Mandatory = $true)][string]$Tools,
  [Parameter(Mandatory = $true)][string]$Out,
  [ValidateSet("prepare", "run")][string]$Phase = "run",
  [int]$Idle = 75,
  [string]$Only = "python,java,go,examples,reconcile",
  [string]$Jdk17 = "",
  [string]$Go125 = "",
  [string]$Ffmpeg = "",
  [string]$PyOnly = "r1,r3,r4,r5",
  [string]$PyRedo = ""
)
$ErrorActionPreference = "Continue"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
if (-not $Jdk17) { $Jdk17 = (Get-ChildItem -Directory (Join-Path $Tools "jdk17\jdk-17*") | Select-Object -First 1).FullName }
if (-not $Go125) { $Go125 = Join-Path $Tools "go1.25\go" }
$steps = $Only -split ","
New-Item -ItemType Directory -Force $Out | Out-Null
$audio = Join-Path $Out "audio"
$env:PYTHONUTF8 = "1"
$vpy = Join-Path $Tools "venvs\wheel-only-313\Scripts\python.exe"
function Log($m) { $s = (Get-Date -Format "yyyy-MM-dd HH:mm:ss") + " " + $m; $s | Tee-Object -FilePath (Join-Path $Out "live-run.log") -Append }

$cls = Join-Path $Tools "live-java-classes"
$goWork = Join-Path $Tools "live-go"
if ($Phase -eq "prepare") {
  if (-not (Test-Path (Join-Path $audio "speech_mulaw_8000.raw"))) {
    & powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot "make_audio.ps1") -Out $audio -Ffmpeg $Ffmpeg | Out-File (Join-Path $Out "make-audio.log") -Encoding utf8
  }
  $cp = (Get-Content (Join-Path $Tools "java-consumer-jdk17\cp.txt") -Raw -Encoding UTF8).Trim()
  if (Test-Path $cls) { Remove-Item -Recurse -Force $cls }
  New-Item -ItemType Directory -Force $cls | Out-Null
  & "$Jdk17\bin\javac.exe" -encoding UTF-8 -d $cls -cp $cp (Join-Path $PSScriptRoot "java\LiveJava.java")
  Log ("prepare javac exit " + $LASTEXITCODE)
  $env:GOROOT = $Go125; $env:GOTOOLCHAIN = "local"; $env:GOMODCACHE = Join-Path $Tools "gomodcache"
  $env:GOCACHE = Join-Path $Tools "gocache-live"; $env:GOPATH = Join-Path $Tools "gopath"; $env:CGO_ENABLED = "0"
  if (Test-Path $goWork) { Remove-Item -Recurse -Force $goWork }
  Copy-Item -Recurse (Join-Path $PSScriptRoot "go") $goWork
  Push-Location $goWork
  & "$Go125\bin\go.exe" mod edit ("-replace=github.com/MyVocal-AI/myvocal-stt-realtime-sdks/go=" + (Join-Path $Tools "go-candidate\go"))
  & "$Go125\bin\go.exe" build -o live-go.exe .
  Log ("prepare go build exit " + $LASTEXITCODE)
  Pop-Location
  Log "prepare end"
  exit 0
}

# ---- run phase
if (-not $env:MYVOCAL_ACCESS_KEY) { Write-Error "MYVOCAL_ACCESS_KEY is not set"; exit 2 }
$prod = "https://api.myvocal.ai"
$baseNow = if ($env:MYVOCAL_API_BASE) { $env:MYVOCAL_API_BASE.TrimEnd("/") } else { $prod }
if ($baseNow -ne $prod -and -not $env:MYVOCAL_ACCESS_KEY.StartsWith("fixture-")) { Write-Error "refusing: a non-fixture key may only be sent to $prod"; exit 2 }
$env:MYVOCAL_API_BASE = $baseNow
Log ("live start; HEAD " + (& git -C $Root rev-parse HEAD).Trim() + "; base " + $baseNow)
if (-not (Test-Path (Join-Path $Out "balance-start.json"))) {
  & $vpy (Join-Path $PSScriptRoot "balance.py") --out $Out --label start *>&1 | Tee-Object -FilePath (Join-Path $Out "balance.log") -Append
  if ($LASTEXITCODE -ne 0) { Log "balance read failed; stopping before any session is created"; exit 3 }
}

if ($steps -contains "python") {
  $pyArgs = @((Join-Path $PSScriptRoot "live_python.py"), "--audio", $audio, "--out", $Out, "--idle", $Idle, "--only", $PyOnly)
  if ($PyRedo) { $pyArgs += @("--redo", $PyRedo) }
  & $vpy @pyArgs *>&1 | Tee-Object -FilePath (Join-Path $Out "live-python.log")
  Log ("python exit " + $LASTEXITCODE)
}
if ($steps -contains "java") {
  $cp = (Get-Content (Join-Path $Tools "java-consumer-jdk17\cp.txt") -Raw -Encoding UTF8).Trim()
  & "$Jdk17\bin\java.exe" -cp ($cls + ";" + $cp) LiveJava $audio $Out $Idle *>&1 | Tee-Object -FilePath (Join-Path $Out "live-java.log")
  Log ("java exit " + $LASTEXITCODE)
}
if ($steps -contains "go") {
  & (Join-Path $goWork "live-go.exe") $audio $Out $Idle *>&1 | Tee-Object -FilePath (Join-Path $Out "live-go.log")
  Log ("go exit " + $LASTEXITCODE)
}
if ($steps -contains "examples") {
  # R5: one shipped file-stream example per language, unchanged, from the installed candidates
  $wav = Join-Path $audio "speech_pcm_s16le_16000.wav"
  $prev = Join-Path $Out "live-examples.json"
  if (Test-Path $prev) { $n = 1; while (Test-Path (Join-Path $Out "live-examples-attempt$n.json")) { $n++ }; Move-Item $prev (Join-Path $Out "live-examples-attempt$n.json") }
  $recs = @()
  $runs = @(
    @("R5-example-java-FileStream", "$Jdk17\bin\java.exe", @("-cp", ((Get-Content (Join-Path $Tools "java-consumer-jdk17\cp.txt") -Raw -Encoding UTF8).Trim() + ";" + (Join-Path $Tools "java-consumer-jdk17\target\classes")), "example.FileStream", $wav)),
    @("R5-example-go-file_stream", (Join-Path $Tools "go-consumer\consumer.exe"), @($wav))
  )
  foreach ($r in $runs) {
    $o = & $r[1] @($r[2]) 2>&1 | Out-String
    $m = [regex]::Match($o, "status: (\w+) \| transcriptionId: (stt_\w+)")
    $recs += [ordered]@{ scenario = $r[0]; status = $(if ($m.Success) { $m.Groups[1].Value } else { $null }); transcriptionId = $(if ($m.Success) { $m.Groups[2].Value } else { $null }); ok = ($m.Success -and $m.Groups[1].Value -eq "COMPLETED") }
    Log ($r[0] + " -> " + $(if ($m.Success) { $m.Groups[1].Value + " " + $m.Groups[2].Value } else { "no result" }))
  }
  @{ language = "examples"; records = $recs } | ConvertTo-Json -Depth 5 | Out-File (Join-Path $Out "live-examples.json") -Encoding utf8
}
if ($steps -contains "reconcile") {
  & $vpy (Join-Path $PSScriptRoot "reconcile.py") --out $Out *>&1 | Tee-Object -FilePath (Join-Path $Out "reconcile.log")
  Log ("reconcile exit " + $LASTEXITCODE)
}
Log "live end"
