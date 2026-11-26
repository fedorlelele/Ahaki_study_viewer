$ErrorActionPreference = 'Stop'
[Console]::InputEncoding = [Text.UTF8Encoding]::new($false)
[Console]::OutputEncoding = [Text.UTF8Encoding]::new($false)
try {
    $request = [Console]::In.ReadToEnd() | ConvertFrom-Json
    Add-Type -AssemblyName System.Speech
    $speaker = New-Object System.Speech.Synthesis.SpeechSynthesizer
    try {
        $voices = @($speaker.GetInstalledVoices() | Where-Object Enabled | ForEach-Object {
            [PSCustomObject]@{ id = $_.VoiceInfo.Name; name = $_.VoiceInfo.Name; language = $_.VoiceInfo.Culture.Name }
        })
        if ($request.action -eq 'voices') {
            [Console]::WriteLine((ConvertTo-Json -InputObject $voices -Compress))
        } else {
            $name = $request.voice
            if (-not $name) { $name = ($voices | Where-Object language -Like 'ja*' | Select-Object -First 1).name }
            if ($name) { $speaker.SelectVoice($name) }
            $speaker.Rate = [int]$request.rate
            $speaker.SetOutputToDefaultAudioDevice()
            [Console]::WriteLine('READY')
            $speaker.Speak([string]$request.text)
        }
    } finally { $speaker.Dispose() }
} catch {
    [Console]::Error.WriteLine($_.Exception.Message)
    exit 1
}
