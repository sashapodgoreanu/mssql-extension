[CmdletBinding()]
param(
    [ValidateSet("Debug", "Release", "RelWithDebInfo")]
    [string]$Configuration = "Debug",

    [string]$Arch = "x64",

    [string]$VcpkgTriplet = "x64-windows-static",

    [int]$Jobs = 0,

    [switch]$Clean
)

$ErrorActionPreference = "Stop"

function Resolve-RequiredPath {
    param([string]$Path, [string]$Description)

    $resolved = Resolve-Path -LiteralPath $Path -ErrorAction SilentlyContinue
    if (-not $resolved) {
        throw "$Description not found: $Path"
    }
    return $resolved.Path
}

function Find-VcVarsAll {
    if ($env:VCVARSALL -and (Test-Path -LiteralPath $env:VCVARSALL)) {
        return (Resolve-Path -LiteralPath $env:VCVARSALL).Path
    }

    $vswhere = "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe"
    if (Test-Path -LiteralPath $vswhere) {
        $installPath = & $vswhere -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath
        if ($LASTEXITCODE -eq 0 -and $installPath) {
            $candidate = Join-Path $installPath "VC\Auxiliary\Build\vcvarsall.bat"
            if (Test-Path -LiteralPath $candidate) {
                return (Resolve-Path -LiteralPath $candidate).Path
            }
        }
    }

    $roots = @(
        "${env:ProgramFiles}\Microsoft Visual Studio",
        "${env:ProgramFiles(x86)}\Microsoft Visual Studio"
    ) | Where-Object { $_ -and (Test-Path -LiteralPath $_) }

    foreach ($root in $roots) {
        $candidate = Get-ChildItem -LiteralPath $root -Recurse -Filter vcvarsall.bat -ErrorAction SilentlyContinue |
            Where-Object { $_.FullName -like "*\VC\Auxiliary\Build\vcvarsall.bat" } |
            Sort-Object FullName -Descending |
            Select-Object -First 1
        if ($candidate) {
            return $candidate.FullName
        }
    }

    throw "vcvarsall.bat not found. Install Visual Studio Build Tools with MSVC x64 tools, or set VCVARSALL."
}

function Find-VsRoot {
    param([string]$VcVarsAll)

    $dir = Split-Path -Parent $VcVarsAll
    while ($dir) {
        if (Test-Path -LiteralPath (Join-Path $dir "Common7")) {
            return $dir
        }
        $parent = Split-Path -Parent $dir
        if ($parent -eq $dir) {
            break
        }
        $dir = $parent
    }
    return $null
}

function Find-Tool {
    param([string]$Name, [string[]]$Candidates)

    foreach ($candidate in $Candidates) {
        if ($candidate -and (Test-Path -LiteralPath $candidate)) {
            return (Resolve-Path -LiteralPath $candidate).Path
        }
    }

    $command = Get-Command $Name -ErrorAction SilentlyContinue
    if ($command) {
        return $command.Source
    }

    throw "$Name not found."
}

$repoRoot = Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")
$repoRoot = $repoRoot.Path
$duckdbDir = Resolve-RequiredPath (Join-Path $repoRoot "duckdb") "DuckDB submodule"
$extensionConfig = Resolve-RequiredPath (Join-Path $repoRoot "extension_config.cmake") "Extension config"
$vcpkgDir = Join-Path $repoRoot "vcpkg"
$vcpkgToolchain = Join-Path $vcpkgDir "scripts\buildsystems\vcpkg.cmake"
$vcpkgExe = Join-Path $vcpkgDir "vcpkg.exe"
$cmakeConfiguration = $Configuration
$buildName = $Configuration.ToLowerInvariant()
if ($Configuration -eq "Debug") {
    # Debug DuckDB builds on MSVC use the debug STL/runtime ABI. Downloaded
    # DuckDB extensions are built with the release ABI, so loading extensions
    # such as ducklake/postgres_scanner from a Debug binary can corrupt C++ API
    # objects and fail with errors like "Missing DB manager".
    #
    # RelWithDebInfo keeps release-compatible ABI while still producing PDBs.
    $cmakeConfiguration = "RelWithDebInfo"
    $buildName = "Debug"
}
elseif ($Configuration -eq "Debug") {
    Write-Warning "MSVC Debug builds are not ABI-compatible with downloaded DuckDB extensions such as ducklake. Use -Configuration Debug for debugger-friendly dynamic extension loading."
}

$buildDir = Join-Path $repoRoot ("build\" + $buildName)

if (-not (Test-Path -LiteralPath $vcpkgToolchain)) {
    if (Test-Path -LiteralPath $vcpkgDir) {
        throw "vcpkg directory exists but toolchain file is missing: $vcpkgToolchain"
    }
    git clone --branch 2025.12.12 https://github.com/microsoft/vcpkg.git $vcpkgDir
}

if (-not (Test-Path -LiteralPath $vcpkgExe)) {
    & (Join-Path $vcpkgDir "bootstrap-vcpkg.bat") -disableMetrics
    if ($LASTEXITCODE -ne 0) {
        throw "vcpkg bootstrap failed with exit code $LASTEXITCODE"
    }
}

$vcVarsAll = Find-VcVarsAll
$vsRoot = Find-VsRoot $vcVarsAll
$cmakeExe = Find-Tool "cmake.exe" @(
    $(if ($vsRoot) { Join-Path $vsRoot "Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin\cmake.exe" })
)
$ninjaExe = Find-Tool "ninja.exe" @(
    $(if ($vsRoot) { Join-Path $vsRoot "Common7\IDE\CommonExtensions\Microsoft\CMake\Ninja\ninja.exe" })
)

if ($Clean -and (Test-Path -LiteralPath $buildDir)) {
    $resolvedBuild = (Resolve-Path -LiteralPath $buildDir).Path
    if (-not $resolvedBuild.StartsWith($repoRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Refusing to remove build directory outside repo: $resolvedBuild"
    }
    Remove-Item -LiteralPath $resolvedBuild -Recurse -Force
}

New-Item -ItemType Directory -Force -Path (Join-Path $repoRoot "build") | Out-Null

$cmakeDir = Split-Path -Parent $cmakeExe
$ninjaDir = Split-Path -Parent $ninjaExe
$cmdPath = Join-Path $repoRoot "build\build-windows-$Configuration.cmd"
$buildParallelArg = ""
if ($Jobs -gt 0) {
    $buildParallelArg = " --parallel $Jobs"
}

$cmd = @"
@echo off
setlocal
call "$vcVarsAll" $Arch
if errorlevel 1 exit /b %errorlevel%
set "PATH=$cmakeDir;$ninjaDir;%PATH%"
"$cmakeExe" -G "Ninja" -S "$duckdbDir" -B "$buildDir" -DCMAKE_BUILD_TYPE=$cmakeConfiguration -DCMAKE_MAKE_PROGRAM="$ninjaExe" -DCMAKE_TOOLCHAIN_FILE="$vcpkgToolchain" -DVCPKG_TARGET_TRIPLET=$VcpkgTriplet -DVCPKG_HOST_TRIPLET=$VcpkgTriplet -DVCPKG_MANIFEST_DIR="$repoRoot" -DDUCKDB_EXTENSION_CONFIGS="$extensionConfig" -DEXTENSION_STATIC_BUILD=1 -DDISABLE_EXTENSION_LOAD=FALSE -DUNITTEST_ROOT_DIRECTORY="$repoRoot" -DBENCHMARK_ROOT_DIRECTORY="$repoRoot" -DENABLE_UNITTEST_CPP_TESTS=FALSE
if errorlevel 1 exit /b %errorlevel%
"$cmakeExe" --build "$buildDir" --config $cmakeConfiguration$buildParallelArg
exit /b %errorlevel%
"@

Set-Content -LiteralPath $cmdPath -Value $cmd -Encoding ASCII

Write-Host "MSVC environment: $vcVarsAll"
Write-Host "CMake: $cmakeExe"
Write-Host "Ninja: $ninjaExe"
Write-Host "vcpkg toolchain: $vcpkgToolchain"
Write-Host "Configuration: $Configuration (CMake: $cmakeConfiguration)"
if ($Jobs -gt 0) {
    Write-Host "Build parallelism: $Jobs"
}
Write-Host "Build directory: $buildDir"

& cmd.exe /d /s /c "`"$cmdPath`""
if ($LASTEXITCODE -ne 0) {
    throw "Windows $Configuration build failed with exit code $LASTEXITCODE"
}
