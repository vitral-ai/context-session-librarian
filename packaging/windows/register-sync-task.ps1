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
$State = (Resolve-Path -LiteralPath $State).Path
$Python = (Resolve-Path -LiteralPath $Python).Path
$identity = [Security.Principal.WindowsIdentity]::GetCurrent()

foreach ($name in @('CODEX_HOME', 'CLAUDE_CONFIG_DIR', 'CSL_ANTIGRAVITY_HOME')) {
    $active = [Environment]::GetEnvironmentVariable($name, 'Process')
    $scheduled = [Environment]::GetEnvironmentVariable($name, 'User')
    if (-not $scheduled) {
        $scheduled = [Environment]::GetEnvironmentVariable($name, 'Machine')
    }
    if ($active -cne $scheduled) {
        throw "$name differs from the user or machine environment the task will use. Persist the intended profile and register from a fresh PowerShell session."
    }
    if ($active -and $active -notmatch '^(?:[A-Za-z]:[\\/]|\\\\[^\\])') {
        throw "$name must be an absolute Windows path for the scheduled collector."
    }
}

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
