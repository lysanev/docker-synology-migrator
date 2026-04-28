param(
    [string[]]$SourceFiles,
    [string]$OutputDir,
    [string]$IconPath
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSCommandPath
$WorkspaceRoot = Split-Path -Parent $ProjectRoot

function Resolve-FirstExistingPath {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Candidates,
        [Parameter(Mandatory = $true)]
        [string]$Description
    )

    foreach ($candidate in $Candidates) {
        if (-not [string]::IsNullOrWhiteSpace($candidate) -and (Test-Path -LiteralPath $candidate)) {
            return $candidate
        }
    }

    throw "$Description not found. Checked: $($Candidates -join ', ')"
}

if (-not $SourceFiles -or $SourceFiles.Count -eq 0) {
    $SourceFiles = @(
        (Join-Path $ProjectRoot "DockerSynologyMigrator.cs"),
        (Join-Path $ProjectRoot "DockerSynologyMigrator.Gui.cs"),
        (Join-Path $ProjectRoot "DockerSynologyMigrator.VmGui.cs")
    )
}

if ([string]::IsNullOrWhiteSpace($OutputDir)) {
    $OutputDir = Join-Path $ProjectRoot "dist"
}

if ([string]::IsNullOrWhiteSpace($IconPath)) {
    $IconPath = Resolve-FirstExistingPath `
        -Description "Application icon" `
        -Candidates @(
            (Join-Path $ProjectRoot "assets\DockerSynologyMigrator.ico"),
            (Join-Path $WorkspaceRoot "assets\DockerSynologyMigrator.ico")
        )
}

$compiler = "C:\Windows\Microsoft.NET\Framework64\v4.0.30319\csc.exe"
$VendorRoot = Resolve-FirstExistingPath `
    -Description "Vendor dependency folder" `
    -Candidates @(
        (Join-Path $ProjectRoot "vendor"),
        (Join-Path $WorkspaceRoot "vendor"),
        "D:\codex\vendor"
    )

$sshNet = Join-Path $VendorRoot "sshnet_pkg\lib\net462\Renci.SshNet.dll"
$bouncyCastle = Join-Path $VendorRoot "deps\BouncyCastle.Cryptography.2.4.0\lib\net461\BouncyCastle.Cryptography.dll"
$asyncInterfaces = Join-Path $VendorRoot "deps\Microsoft.Bcl.AsyncInterfaces.1.0.0\lib\net461\Microsoft.Bcl.AsyncInterfaces.dll"
$asn1 = Join-Path $VendorRoot "deps\System.Formats.Asn1.8.0.1\lib\net462\System.Formats.Asn1.dll"
$tasksExtensions = Join-Path $VendorRoot "deps\System.Threading.Tasks.Extensions.4.5.2\lib\netstandard2.0\System.Threading.Tasks.Extensions.dll"
$buffers = Join-Path $VendorRoot "deps\System.Buffers.4.5.1\lib\net461\System.Buffers.dll"
$memory = Join-Path $VendorRoot "deps\System.Memory.4.5.5\lib\net461\System.Memory.dll"
$valueTuple = Join-Path $VendorRoot "deps\System.ValueTuple.4.5.0\lib\net461\System.ValueTuple.dll"
$unsafe = Join-Path $VendorRoot "deps\System.Runtime.CompilerServices.Unsafe.4.5.3\lib\net461\System.Runtime.CompilerServices.Unsafe.dll"
$vectors = Join-Path $VendorRoot "deps\System.Numerics.Vectors.4.5.0\lib\net46\System.Numerics.Vectors.dll"
$output = Join-Path $OutputDir "DockerSynologyMigrator.exe"

if (-not (Test-Path $compiler)) {
    throw "csc.exe not found: $compiler"
}

if (-not (Test-Path $sshNet)) {
    throw "SSH.NET DLL not found: $sshNet"
}

$dependencyDlls = @(
    $bouncyCastle,
    $asyncInterfaces,
    $asn1,
    $tasksExtensions,
    $buffers,
    $memory,
    $valueTuple,
    $unsafe,
    $vectors
)

foreach ($dll in $dependencyDlls) {
    if (-not (Test-Path $dll)) {
        throw "Dependency DLL not found: $dll"
    }
}

if (-not (Test-Path $IconPath)) {
    throw "Application icon not found: $IconPath"
}

New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null

& $compiler `
    /nologo `
    /target:winexe `
    /optimize+ `
    /platform:anycpu `
    /out:$output `
    /win32icon:$IconPath `
    /reference:System.Web.Extensions.dll `
    /reference:System.Drawing.dll `
    /reference:System.Windows.Forms.dll `
    /reference:$sshNet `
    /resource:$sshNet,DockerMigrator.Resources.Renci.SshNet.dll `
    /resource:$bouncyCastle,DockerMigrator.Resources.BouncyCastle.Cryptography.dll `
    /resource:$asyncInterfaces,DockerMigrator.Resources.Microsoft.Bcl.AsyncInterfaces.dll `
    /resource:$asn1,DockerMigrator.Resources.System.Formats.Asn1.dll `
    /resource:$tasksExtensions,DockerMigrator.Resources.System.Threading.Tasks.Extensions.dll `
    /resource:$buffers,DockerMigrator.Resources.System.Buffers.dll `
    /resource:$memory,DockerMigrator.Resources.System.Memory.dll `
    /resource:$valueTuple,DockerMigrator.Resources.System.ValueTuple.dll `
    /resource:$unsafe,DockerMigrator.Resources.System.Runtime.CompilerServices.Unsafe.dll `
    /resource:$vectors,DockerMigrator.Resources.System.Numerics.Vectors.dll `
    $SourceFiles

if ($LASTEXITCODE -ne 0) {
    throw "Compilation failed with exit code $LASTEXITCODE"
}

Write-Host "Built: $output"
