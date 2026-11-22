# Opt-in Windows task for an already enrolled native collector.
[CmdletBinding()]
param(
    [string]$Python = (Get-Command python.exe -CommandType Application -ErrorAction Stop).Source,
    [string]$State = (Join-Path $env:LOCALAPPDATA 'csl\sync'),
    [string]$TaskName = 'CSL Session Collector',
    [switch]$AtStartup
)

$ErrorActionPreference = 'Stop'
$root = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..')).Path
$State = [IO.Path]::GetFullPath($State)
$Python = (Resolve-Path -LiteralPath $Python).Path
$identity = [Security.Principal.WindowsIdentity]::GetCurrent()

Push-Location $root
try {
    $status = & $Python -m csl_sync sync --home $State status | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0 -or -not $status.enabled -or -not $status.authentication.signed_in) {
        throw 'Sign in to this collector state before enabling the task.'
    }
} finally {
    Pop-Location
}

$action = New-ScheduledTaskAction -Execute $Python -WorkingDirectory $root `
    -Argument ('-m csl_sync sync --home "{0}" watch' -f $State)
if ($AtStartup) {
    # S4U runs under the account without storing its password. It has no Windows
    # network credentials. The collector uses its own HTTPS access token.
    $trigger = New-ScheduledTaskTrigger -AtStartup
    $principal = New-ScheduledTaskPrincipal -UserId $identity.User.Value -LogonType S4U -RunLevel Limited
} else {
    $trigger = New-ScheduledTaskTrigger -AtLogOn -User $identity.Name
    $principal = New-ScheduledTaskPrincipal -UserId $identity.User.Value -LogonType Interactive -RunLevel Limited
}
$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit ([TimeSpan]::Zero) -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
Register-ScheduledTask -TaskName $TaskName -Action $action -Principal $principal `
    -Trigger $trigger -Settings $settings -Description 'Publish opted-in native CSL sessions.' | Out-Null
