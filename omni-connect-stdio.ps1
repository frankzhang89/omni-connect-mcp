# JDK home directory (absolute path), or the name of a JDK home environment variable.
# Default: JAVA_HOME; if JAVA_HOME is empty/unset, use java.exe from PATH.
$jdkHome = 'JAVA_HOME'
$projectDir = $PSScriptRoot
# Configure the JAR here. Prefer a deployed sibling JAR, then the local build output.
$jarPath = Join-Path $projectDir 'omni-connect-mcp.jar'
if (-not (Test-Path -LiteralPath $jarPath -PathType Leaf)) {
    $jarPath = Join-Path $projectDir 'omni-connect-app/target/omni-connect-mcp.jar'
}
$jvmOptions = @('-Xms64m', '-Xmx512m')
$springOptions = @(
    ('--spring.config.location=file:' + (Join-Path $projectDir 'application.yml')),
    '--spring.ai.mcp.server.stdio=true'
    '--spring.ai.mcp.server.protocol=STREAMABLE'
    '--spring.main.web-application-type=none'
    '--spring.main.banner-mode=off'
    '--logging.config=classpath:logback-stdio.xml'
)

$ErrorActionPreference = 'Stop'
try {
    if ($jdkHome -match '^(?:[A-Za-z]:[\\/]|\\\\)') {
        $jdkDirectory = $jdkHome
    }
    elseif ($jdkHome -match '^[A-Za-z_][A-Za-z0-9_]*$') {
        $jdkDirectory = [Environment]::GetEnvironmentVariable($jdkHome)
        if ([string]::IsNullOrWhiteSpace($jdkDirectory) -and $jdkHome -ne 'JAVA_HOME') {
            throw "JDK environment variable is empty or unset: $jdkHome"
        }
    }
    else { throw 'jdkHome must be an absolute JDK home path or an environment variable name.' }

    if ([string]::IsNullOrWhiteSpace($jdkDirectory)) {
        $java = (Get-Command java.exe -CommandType Application -ErrorAction Stop | Select-Object -First 1).Source
    }
    else {
        $java = Join-Path $jdkDirectory 'bin/java.exe'
        if (-not (Test-Path -LiteralPath $java -PathType Leaf)) {
            throw "Java executable not found: $java"
        }
    }
    if (-not (Test-Path -LiteralPath $jarPath -PathType Leaf)) {
        throw "JAR not found: $jarPath; build the project or configure jarPath in this script."
    }

    # Quote Windows native arguments, including embedded quotes and trailing backslashes.
    $nativeArguments = @($jvmOptions) + @('-jar', $jarPath) + @($springOptions)
    $quotedArguments = foreach ($argument in $nativeArguments) {
        '"' + ([regex]::Replace([string]$argument, '(\\*)"', '$1$1\"') -replace '(\\+)$', '$1$1') + '"'
    }
    $startInfo = New-Object System.Diagnostics.ProcessStartInfo
    $startInfo.FileName = $java
    $startInfo.Arguments = $quotedArguments -join ' '
    $startInfo.WorkingDirectory = $projectDir
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    # Copy raw bytes so Windows PowerShell never decodes/re-encodes MCP messages.
    $startInfo.RedirectStandardInput = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $process = [System.Diagnostics.Process]::Start($startInfo)
    try {
        # Dedicated byte pumps flush each read instead of buffering until stdin EOF.
        Add-Type -TypeDefinition @"
using System;
using System.IO;
using System.Diagnostics;
using System.Threading.Tasks;
public static class OmniStdioPumps {
    private static void Copy(Stream source, Stream destination) {
        byte[] buffer = new byte[8192];
        int count;
        while ((count = source.Read(buffer, 0, buffer.Length)) > 0) {
            destination.Write(buffer, 0, count);
            destination.Flush();
        }
    }
    public static int Run(Process process) {
        Task.Run(() => {
            try { Copy(Console.OpenStandardInput(), process.StandardInput.BaseStream); }
            catch (IOException) { }
            finally { try { process.StandardInput.Close(); } catch (IOException) { } }
        });
        Task output = Task.Run(() => Copy(process.StandardOutput.BaseStream, Console.OpenStandardOutput()));
        Task errors = Task.Run(() => Copy(process.StandardError.BaseStream, Console.OpenStandardError()));
        process.WaitForExit();
        Task.WaitAll(output, errors);
        return process.ExitCode;
    }
}
"@
        $exitCode = [OmniStdioPumps]::Run($process)
    }
    finally {
        if (-not $process.HasExited) { $process.Kill() }
        $process.Dispose()
    }
    exit $exitCode
}
catch {
    [Console]::Error.WriteLine($_.Exception.Message)
    exit 1
}
