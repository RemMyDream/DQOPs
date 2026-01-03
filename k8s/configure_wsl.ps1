# Script to configure WSL 2 for Docker and Kubernetes (with cgroup v2 support)
$ErrorActionPreference = "Stop"
$ConfigPath = "$env:USERPROFILE\.wslconfig"
$TotalRam = (Get-CimInstance Win32_PhysicalMemory | Measure-Object -Property Capacity -Sum).Sum / 1GB

Write-Host "=== WSL 2 Configuration for Docker & Kubernetes ===" -ForegroundColor Cyan
Write-Host "Detected Total System RAM: $([math]::Round($TotalRam, 1)) GB" -ForegroundColor Gray

# Determine safe memory limit (target 8GB, but don't exceed 80% of system RAM)
$TargetMem = 8
if ($TotalRam -lt 10) {
    Write-Host "WARNING: You have less than 10GB of RAM. Setting memory to 4GB." -ForegroundColor Yellow
    $TargetMem = 4
}

# Configuration with cgroup v2 support for Kubernetes
$ConfigContent = @"
[wsl2]
# Memory allocation
memory=${TargetMem}GB
processors=4

# Enable cgroup v2 (required for newer Kubernetes versions)
kernelCommandLine = cgroup_no_v1=all systemd.unified_cgroup_hierarchy=1

# Memory management
autoMemoryReclaim=gradual
"@

if (Test-Path $ConfigPath) {
    Write-Host "`nA .wslconfig file already exists at: $ConfigPath" -ForegroundColor Yellow
    Write-Host "Current content:" -ForegroundColor Gray
    Write-Host "----------------------------------------" -ForegroundColor DarkGray
    Get-Content $ConfigPath
    Write-Host "----------------------------------------" -ForegroundColor DarkGray
    Write-Host "`nOverwriting with new settings..." -ForegroundColor Yellow
} else {
    Write-Host "`nCreating new .wslconfig file at: $ConfigPath" -ForegroundColor Green
}

$ConfigContent | Out-File -FilePath $ConfigPath -Encoding ASCII

Write-Host "`n=== Configuration Applied ===" -ForegroundColor Green
Write-Host "Memory: ${TargetMem}GB" -ForegroundColor White
Write-Host "CPUs: 4" -ForegroundColor White
Write-Host "Cgroup: v2 (enabled)" -ForegroundColor White
Write-Host "`n=== Next Steps ===" -ForegroundColor Cyan
Write-Host "1. Shut down WSL: " -NoNewline -ForegroundColor White
Write-Host "wsl --shutdown" -ForegroundColor Yellow
Write-Host "2. Restart Docker Desktop" -ForegroundColor White
Write-Host "3. Verify cgroup v2: " -NoNewline -ForegroundColor White
Write-Host "docker info | grep -i cgroup" -ForegroundColor Yellow
Write-Host "`nExpected output after restart:" -ForegroundColor Gray
Write-Host "  Cgroup Driver: systemd" -ForegroundColor DarkGray
Write-Host "  Cgroup Version: 2" -ForegroundColor DarkGray