$ErrorActionPreference = 'Stop'

$wslDirectory = (& wsl.exe -e wslpath -a $PSScriptRoot).Trim()
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($wslDirectory)) {
    throw "Unable to resolve the Citus integration directory inside WSL."
}

& wsl.exe -e bash "$wslDirectory/verify.sh"
$verificationExitCode = $LASTEXITCODE
if ($verificationExitCode -ne 0) {
    exit $verificationExitCode
}
