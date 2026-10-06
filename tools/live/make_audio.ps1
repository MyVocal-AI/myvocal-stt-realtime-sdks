<#
Builds the project-owned synthetic speech used by the live validation: Windows SAPI text-to-speech
(English, no customer audio, no real person's voice sample) -> 16-bit mono WAV -> FFmpeg into the
seven MyVocal input encodings (speech_<encoding>.raw) plus speech_pcm_s16le_16000.wav.

  powershell -ExecutionPolicy Bypass -File tools\live\make_audio.ps1 -Out <dir> [-Ffmpeg <path to ffmpeg.exe>]
#>
param([Parameter(Mandatory = $true)][string]$Out, [string]$Ffmpeg = "")
$ErrorActionPreference = "Stop"
if (-not $Ffmpeg) { $Ffmpeg = (Get-Command ffmpeg -ErrorAction SilentlyContinue).Source }
if (-not $Ffmpeg) { throw "ffmpeg not found: pass -Ffmpeg <path>" }
New-Item -ItemType Directory -Force $Out | Out-Null
Add-Type -AssemblyName System.Speech
$text = "MyVocal realtime speech to text check. The quick brown fox jumps over the lazy dog. " +
        "Alice met Bob in Paris on Monday at nine thirty. Please call five five five, one two three four. " +
        "This sentence is the end of the test recording."
$src = Join-Path $Out "speech_source.wav"
$synth = New-Object System.Speech.Synthesis.SpeechSynthesizer
$fmt = New-Object System.Speech.AudioFormat.SpeechAudioFormatInfo(16000, [System.Speech.AudioFormat.AudioBitsPerSample]::Sixteen, [System.Speech.AudioFormat.AudioChannel]::Mono)
$synth.SetOutputToWaveFile($src, $fmt)
$synth.Speak($text)
$synth.Dispose()
$encodings = @{ "pcm_s16le_8000" = @("s16le", 8000); "pcm_s16le_16000" = @("s16le", 16000); "pcm_s16le_22050" = @("s16le", 22050);
                "pcm_s16le_24000" = @("s16le", 24000); "pcm_s16le_44100" = @("s16le", 44100); "pcm_s16le_48000" = @("s16le", 48000);
                "mulaw_8000" = @("mulaw", 8000) }
foreach ($e in $encodings.Keys) {
  $f = $encodings[$e]
  & $Ffmpeg -y -loglevel error -i $src -ac 1 -ar $f[1] -f $f[0] (Join-Path $Out ("speech_" + $e + ".raw"))
}
& $Ffmpeg -y -loglevel error -i $src -ac 1 -ar 16000 -c:a pcm_s16le (Join-Path $Out "speech_pcm_s16le_16000.wav")
Get-ChildItem $Out | ForEach-Object { "{0} {1}" -f $_.Name, $_.Length }
