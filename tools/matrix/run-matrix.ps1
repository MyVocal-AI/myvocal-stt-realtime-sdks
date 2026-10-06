<#
Runtime matrix + package checks for the three SDKs (plan v1.1, section 6 / 8.1). Windows PowerShell 5.1+.

Runtimes are project-isolated (never the global JDK/Go/Python/PATH). Defaults look under -Tools
  (jdk8\*, jdk11\jdk-11*, jdk13\jdk-13*, jdk17\jdk-17*, jdk21\jdk-21*, go1.22\go, go1.25\go,
  python\cpython-3.10*); pass -Jdk8/-Jdk11/-Jdk13/-Go122/-Py313/-Maven etc. to override.
  Note: JDK 13 cannot start from a directory whose path contains non-ASCII characters.
Logs go to -Out (one file per run) and a summary to -Out\matrix-summary.txt.

  powershell -ExecutionPolicy Bypass -File tools\matrix\run-matrix.ps1 -Tools <runtimes dir> -Out <log dir> [-Stage all|java|go|python|install]
#>
param(
  [Parameter(Mandatory = $true)][string]$Tools,
  [Parameter(Mandatory = $true)][string]$Out,
  [string]$Stage = "all",
  [string]$Jdk8 = "",
  [string]$Jdk11 = "",
  [string]$Jdk13 = "",
  [string]$Jdk17 = "",
  [string]$Jdk21 = "",
  [string]$Go122 = "",
  [string]$Go125 = "",
  [string]$Py310 = "",
  [string]$Py313 = "",
  [string]$Maven = "",
  [string]$Uv = "uv"
)
$ErrorActionPreference = "Continue"  # native stderr must not abort PS 5.1
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
function FirstDir($pattern) { $d = Get-ChildItem -Directory $pattern -ErrorAction SilentlyContinue | Select-Object -First 1; if ($d) { $d.FullName } else { "" } }
if (-not $Jdk8) { $Jdk8 = FirstDir (Join-Path $Tools "jdk8\*") }
if (-not $Jdk11) { $Jdk11 = FirstDir (Join-Path $Tools "jdk11\jdk-11*") }
if (-not $Go122) { $Go122 = Join-Path $Tools "go1.22\go" }
if (-not $Py313) { $Py313 = (Get-Command python -ErrorAction SilentlyContinue).Source }
if (-not $Maven) { $Maven = (Get-Command mvn.cmd -ErrorAction SilentlyContinue).Source }
if (-not $Jdk13) { $Jdk13 = FirstDir (Join-Path $Tools "jdk13\jdk-13*") }
if (-not $Jdk17) { $Jdk17 = FirstDir (Join-Path $Tools "jdk17\jdk-17*") }
if (-not $Jdk21) { $Jdk21 = FirstDir (Join-Path $Tools "jdk21\jdk-21*") }
if (-not $Go125) { $Go125 = Join-Path $Tools "go1.25\go" }
if (-not $Py310) { $Py310 = (Get-ChildItem (Join-Path $Tools "python\cpython-3.10*\python.exe") | Select-Object -First 1).FullName }

New-Item -ItemType Directory -Force $Out | Out-Null
$Summary = Join-Path $Out "matrix-summary.txt"
$env:NO_PROXY = "127.0.0.1,localhost"; $env:no_proxy = $env:NO_PROXY
$env:FIXTURE_PYTHON = $Py313
$env:STT_REPO_ROOT = $Root
$env:PYTHONUTF8 = "1"

function Log($line) { $stamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"; "$stamp $line" | Tee-Object -FilePath $Summary -Append }

function Run($name, [scriptblock]$body) {
  $log = Join-Path $Out ($name + ".log")
  $t0 = Get-Date
  $global:LASTEXITCODE = 0
  try {
    & $body *>&1 | Out-File -FilePath $log -Encoding utf8
    $code = $global:LASTEXITCODE
  } catch {
    $_ | Out-File -FilePath $log -Append -Encoding utf8
    $code = 99
  }
  $secs = [int]((Get-Date) - $t0).TotalSeconds
  $res = if ($code -eq 0) { "PASS" } else { "FAIL($code)" }
  Log ("{0,-38} {1,-9} {2,5}s  {3}" -f $name, $res, $secs, $log)
  return ($code -eq 0)
}

function JavaVersion($jdk) { (& cmd /c "`"$jdk\bin\java.exe`" -version 2>&1" | Select-Object -First 1) }

# ------------------------------------------------------------------ Java
$javaDir = Join-Path $Root "java"
$m2 = Join-Path $Tools "m2-build"
$deps = Join-Path $javaDir "target\deps"
$console = Join-Path $Tools "junit-platform-console-standalone-1.14.4.jar"

function JavaBuild {
  Run "java-build-jdk17-release8" {
    $env:JAVA_HOME = $Jdk17
    Push-Location $javaDir
    & $Maven -B -q "-Dmaven.repo.local=$m2" -DskipTests clean package dependency:copy-dependencies "-DincludeScope=runtime" "-DoutputDirectory=target/deps"
    if ($LASTEXITCODE -eq 0) { & $Maven -B -q "-Dmaven.repo.local=$m2" test-compile }
    Pop-Location
    if (-not (Test-Path $console)) {
      & $Maven -B -q "-Dmaven.repo.local=$m2" dependency:copy "-Dartifact=org.junit.platform:junit-platform-console-standalone:1.14.4" "-DoutputDirectory=$Tools"
    }
    # class file major version of the packaged JAR (52 = Java 8)
    & "$Jdk17\bin\javap.exe" -v -cp (Join-Path $javaDir "target\myvocal-stt-realtime-1.0.0.jar") ai.myvocal.stt.realtime.RealtimeSession | Select-String "major version"
  } | Out-Null
}

function JavaRun($label, $jdk, $tagArgs) {
  Run ("java-" + $label) {
    "runtime: " + (JavaVersion $jdk)
    $jar = Join-Path $javaDir "target\myvocal-stt-realtime-1.0.0.jar"
    $cp = @($jar, (Join-Path $javaDir "target\test-classes")) + (Get-ChildItem $deps -Filter *.jar | ForEach-Object FullName)
    $cpText = ($cp -join ";")
    $env:JAVA_HOME = $jdk
    & "$jdk\bin\java.exe" -jar $console execute --disable-banner --details=summary --fail-if-no-tests --class-path $cpText --scan-class-path (Join-Path $javaDir "target\test-classes") @tagArgs
  } | Out-Null
}

# ------------------------------------------------------------------ Go
$goDir = Join-Path $Root "go"
function GoRun($label, $goroot, $runArgs) {
  Run ("go-" + $label) {
    $env:GOROOT = $goroot
    $env:GOTOOLCHAIN = "local"
    $env:GOMODCACHE = Join-Path $Tools "gomodcache"
    $env:GOCACHE = Join-Path $Tools ("gocache-" + $label)
    $env:GOPATH = Join-Path $Tools "gopath"
    $env:CGO_ENABLED = "0"
    & "$goroot\bin\go.exe" version
    Push-Location $goDir
    & "$goroot\bin\go.exe" vet ./...
    if ($LASTEXITCODE -eq 0) { & "$goroot\bin\go.exe" test -count=1 @runArgs ./... }
    Pop-Location
  } | Out-Null
}

# ------------------------------------------------------------------ Python
$pyDir = Join-Path $Root "python"
$env:UV_CACHE_DIR = Join-Path $Tools "uvcache"
function PyBuild {
  Run "python-build-sdist-wheel" {
    Push-Location $pyDir
    if (Test-Path dist) { Remove-Item -Recurse -Force dist }
    # uv build: sdist first, then the wheel is built FROM the sdist (proves the sdist is complete)
    & $Uv build --python $Py313 --out-dir dist
    Pop-Location
    Get-ChildItem (Join-Path $pyDir "dist") | ForEach-Object { "{0}  {1}  sha256={2}" -f $_.Name, $_.Length, (Get-FileHash $_.FullName -Algorithm SHA256).Hash }
  } | Out-Null
}

function PyRun($label, $python, $kArgs) {
  Run ("python-" + $label) {
    $venv = Join-Path $Tools ("venvs\" + $label)
    if (Test-Path $venv) { Remove-Item -Recurse -Force $venv }
    & $Uv venv --python $python $venv
    $vpy = Join-Path $venv "Scripts\python.exe"
    $wheel = (Get-ChildItem (Join-Path $pyDir "dist\*.whl") | Select-Object -First 1).FullName
    & $Uv pip install --python $vpy $wheel "pytest>=8" "pytest-asyncio>=0.24"
    & $vpy -c "import sys, myvocal_stt_realtime as m; print(sys.version); print('installed from', m.__file__)"
    Push-Location $pyDir
    & $vpy -m pytest -q -p no:cacheprovider tests @kArgs
    Pop-Location
  } | Out-Null
}

# ------------------------------------------------------------------ clean installs (section 8.1)
function InstallChecks {
  # Java: isolated local repository + standalone consumer project (no SDK source classpath)
  foreach ($pair in @(@("jdk8", $Jdk8), @("jdk17", $Jdk17))) {
    $label = $pair[0]; $jdk = $pair[1]
    Run ("install-java-consumer-" + $label) {
      $env:JAVA_HOME = $jdk
      $repo = Join-Path $Tools ("m2-consumer-" + $label)
      if (Test-Path $repo) { Remove-Item -Recurse -Force $repo }
      $t = Join-Path $javaDir "target"
      & $Maven -B -q "-Dmaven.repo.local=$repo" install:install-file "-Dfile=$t\myvocal-stt-realtime-1.0.0.jar" "-DpomFile=$javaDir\pom.xml" "-Dsources=$t\myvocal-stt-realtime-1.0.0-sources.jar"
      $work = Join-Path $Tools ("java-consumer-" + $label)
      if (Test-Path $work) { Remove-Item -Recurse -Force $work }
      Copy-Item -Recurse (Join-Path $javaDir "examples") $work
      Push-Location $work
      & $Maven -B -q "-Dmaven.repo.local=$repo" compile dependency:build-classpath "-Dmdep.outputFile=cp.txt"
      $cp = (Get-Content cp.txt -Raw -Encoding UTF8).Trim() + ";target\classes"
      Pop-Location
      "classpath: " + (($cp -split ";") | ForEach-Object { Split-Path $_ -Leaf }) -join ", "
      $fx = Start-Fixture
      try {
        $env:MYVOCAL_ACCESS_KEY = $fx.Key; $env:MYVOCAL_API_BASE = $fx.Base
        $wav = Join-Path $Root "fixtures\audio\tone_pcm_s16le_16000.wav"
        $raw = Join-Path $Root "fixtures\audio\tone_pcm_s16le_16000.raw"
        Push-Location $work
        $ok = $true
        foreach ($c in @(@("FileStream", $wav, "status: COMPLETED"), @("ByteStream", $raw, "status: COMPLETED"), @("PauseResume", $wav, "GET: COMPLETED"), @("AbnormalFinish", $wav, "via REST: true"))) {
          $o = & "$jdk\bin\java.exe" -cp $cp ("example." + $c[0]) $c[1] 2>&1 | Out-String
          $o.Trim()
          if (-not $o.Contains($c[2])) { $ok = $false; "MISSING: " + $c[2] }
        }
        Pop-Location
      } finally { Stop-Fixture $fx; Remove-Item Env:MYVOCAL_ACCESS_KEY -ErrorAction SilentlyContinue }
      if ($ok) { $global:LASTEXITCODE = 0 } else { $global:LASTEXITCODE = 1 }
    } | Out-Null
  }

  # Go: consumer module with replace -> clean export of the committed candidate
  Run "install-go-consumer-1.25" {
    $env:GOROOT = $Go125; $env:GOTOOLCHAIN = "local"; $env:GOMODCACHE = Join-Path $Tools "gomodcache"
    $env:GOCACHE = Join-Path $Tools "gocache-consumer"; $env:GOPATH = Join-Path $Tools "gopath"; $env:CGO_ENABLED = "0"
    $cand = Join-Path $Tools "go-candidate"
    if (Test-Path $cand) { Remove-Item -Recurse -Force $cand }
    New-Item -ItemType Directory $cand | Out-Null
    $sha = (& git -C $Root rev-parse HEAD).Trim()
    "candidate commit: $sha"
    & git -C $Root archive --format=zip -o (Join-Path $cand "go.zip") HEAD go
    Expand-Archive (Join-Path $cand "go.zip") $cand
    $work = Join-Path $Tools "go-consumer"
    if (Test-Path $work) { Remove-Item -Recurse -Force $work }
    New-Item -ItemType Directory $work | Out-Null
    Copy-Item (Join-Path $Root "go\examples\file_stream\main.go") $work
    Push-Location $work
    & "$Go125\bin\go.exe" mod init example.com/consumer
    & "$Go125\bin\go.exe" mod edit "-replace=github.com/MyVocal-AI/myvocal-stt-realtime-sdks/go=$cand\go"
    & "$Go125\bin\go.exe" mod edit "-require=github.com/MyVocal-AI/myvocal-stt-realtime-sdks/go@v1.0.0"
    & "$Go125\bin\go.exe" mod tidy
    & "$Go125\bin\go.exe" build -o consumer.exe .
    Pop-Location
    $fx = Start-Fixture
    try {
      $env:MYVOCAL_ACCESS_KEY = $fx.Key; $env:MYVOCAL_API_BASE = $fx.Base
      $o = & (Join-Path $work "consumer.exe") (Join-Path $Root "fixtures\audio\tone_pcm_s16le_16000.wav") 2>&1 | Out-String
      $o.Trim()
      $global:LASTEXITCODE = if ($o.Contains("status: COMPLETED")) { 0 } else { 1 }
    } finally { Stop-Fixture $fx; Remove-Item Env:MYVOCAL_ACCESS_KEY -ErrorAction SilentlyContinue }
  } | Out-Null

  # Python: wheel in a clean venv, sync + async examples (no editable install)
  Run "install-python-wheel-examples-3.13" {
    $venv = Join-Path $Tools "venvs\wheel-only-313"
    if (Test-Path $venv) { Remove-Item -Recurse -Force $venv }
    & $Uv venv --python $Py313 $venv
    $vpy = Join-Path $venv "Scripts\python.exe"
    & $Uv pip install --python $vpy (Get-ChildItem (Join-Path $pyDir "dist\*.whl") | Select-Object -First 1).FullName
    & $Uv pip list --python $vpy
    $fx = Start-Fixture
    try {
      $env:MYVOCAL_ACCESS_KEY = $fx.Key; $env:MYVOCAL_API_BASE = $fx.Base
      $ok = $true
      $tmp = Join-Path $Tools "py-examples-run"; if (Test-Path $tmp) { Remove-Item -Recurse -Force $tmp }
      Copy-Item -Recurse (Join-Path $pyDir "examples") $tmp   # outside the source tree: only the wheel is importable
      foreach ($c in @(@("file_stream.py", "tone_pcm_s16le_16000.wav", "status: COMPLETED"), @("byte_stream_async.py", "tone_pcm_s16le_16000.raw", "status: COMPLETED"))) {
        $o = & $vpy (Join-Path $tmp $c[0]) (Join-Path $Root ("fixtures\audio\" + $c[1])) 2>&1 | Out-String
        $o.Trim()
        if (-not $o.Contains($c[2])) { $ok = $false; "MISSING: " + $c[2] }
      }
      $global:LASTEXITCODE = if ($ok) { 0 } else { 1 }
    } finally { Stop-Fixture $fx; Remove-Item Env:MYVOCAL_ACCESS_KEY -ErrorAction SilentlyContinue }
  } | Out-Null
}

function Start-Fixture {
  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName = $Py313
  $psi.Arguments = "`"" + (Join-Path $Root "tools\fixture\server.py") + "`""
  $psi.RedirectStandardOutput = $true; $psi.RedirectStandardError = $true; $psi.UseShellExecute = $false
  $p = [System.Diagnostics.Process]::Start($psi)
  $info = $p.StandardOutput.ReadLine() | ConvertFrom-Json
  $key = (Get-Content (Join-Path $Root "fixtures\scenarios.json") -Raw -Encoding UTF8 | ConvertFrom-Json).accessKey
  return @{ Proc = $p; Base = "http://127.0.0.1:" + $info.http; Key = $key }
}
function Stop-Fixture($fx) { try { $fx.Proc.Kill() } catch {} }

# ------------------------------------------------------------------ run
Log "=== matrix start (repo $Root, HEAD $((& git -C $Root rev-parse HEAD).Trim())) stage=$Stage"
$compatJava = @("--include-tag", "compat")
if ($Stage -in @("all", "java")) {
  JavaBuild
  JavaRun "jdk8-full" $Jdk8 @()
  JavaRun "jdk17-full" $Jdk17 @()
  JavaRun "jdk11-compat" $Jdk11 $compatJava
  JavaRun "jdk13-compat" $Jdk13 $compatJava
  JavaRun "jdk21-compat" $Jdk21 $compatJava
}
if ($Stage -in @("all", "go")) {
  GoRun "1.22-full" $Go122 @()
  GoRun "1.25-compat" $Go125 @("-run", "TestA01|TestA06ManualLifecycleWS|TestA07Rotation|TestA08AllEvents|TestA10AckLost")
}
if ($Stage -in @("all", "python")) {
  PyBuild
  PyRun "py310-full" $Py310 @()
  PyRun "py313-compat" $Py313 @("-k", "a01 or a06_manual_commit or a07_rotation or a08_all_public or a10_ack_lost")
}
if ($Stage -in @("all", "install")) { InstallChecks }
Log "=== matrix end"
