# Opt-in Windows task for an already enrolled native collector.
[CmdletBinding()]
param(
    [string]$Python = (Get-Command python.exe -CommandType Application -ErrorAction Stop).Source,
    [string]$State = (Join-Path $env:LOCALAPPDATA 'csl\sync'),
    [string]$TaskName = 'CSL Session Collector',
    [switch]$AtStartup,
    [switch]$Replace
)

$ErrorActionPreference = 'Stop'
$root = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..')).Path
$State = (Resolve-Path -LiteralPath $State).Path
$Python = (Resolve-Path -LiteralPath $Python).Path
$identity = [Security.Principal.WindowsIdentity]::GetCurrent()

$defaults = @{
    CODEX_HOME = '.codex'
    CLAUDE_CONFIG_DIR = '.claude'
    CSL_ANTIGRAVITY_HOME = '.gemini\antigravity-cli'
}
$profiles = @{}
foreach ($name in $defaults.Keys) {
    $location = [Environment]::GetEnvironmentVariable($name, 'Process')
    if (-not $location) {
        $location = Join-Path $env:USERPROFILE $defaults[$name]
    }
    $provider = $null
    $drive = $null
    $profiles[$name] = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath(
        $location, [ref]$provider, [ref]$drive)
    if ($provider.Name -ne 'FileSystem') {
        throw "$name must point to a filesystem path."
    }
}
$profileBytes = [Text.Encoding]::UTF8.GetBytes(($profiles | ConvertTo-Json -Compress))
$profileArgument = [Convert]::ToBase64String($profileBytes)

Push-Location $root
try {
    $status = & $Python -m csl_sync sync --home $State status --json | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0 -or -not $status.enabled -or -not $status.authentication.signed_in) {
        throw 'Sign in to this collector state before enabling the task.'
    }
} finally {
    Pop-Location
}

$launcher = Join-Path $PSScriptRoot 'run-sync-task.py'
$action = New-ScheduledTaskAction -Execute $Python -WorkingDirectory $root `
    -Argument ('"{0}" "{1}" {2}' -f $launcher, $State, $profileArgument)
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
    -Trigger $trigger -Settings $settings -Description 'Publish opted-in native CSL sessions.' `
    -Force:$Replace | Out-Null
