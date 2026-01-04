# BDSP Pipeline Helm Deployment Script

param(
    [Parameter(Position=0)]
    [ValidateSet('deploy', 'install', 'uninstall', 'status', 'dry-run', 'values')]
    [string]$Command = 'deploy',
    
    [Parameter(ValueFromRemainingArguments=$true)]
    [string[]]$AdditionalArgs
)

$ErrorActionPreference = 'Stop'

$SCRIPT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path
$CHART_DIR = Join-Path $SCRIPT_DIR "bdsp-pipeline"
$RELEASE_NAME = "bdsp"
$NAMESPACE = "dqops"

# Function to print colored messages
function Print-Info {
    param([string]$Message)
    Write-Host "[INFO] $Message" -ForegroundColor Green
}

function Print-Warning {
    param([string]$Message)
    Write-Host "[WARNING] $Message" -ForegroundColor Yellow
}

function Print-Error {
    param([string]$Message)
    Write-Host "[ERROR] $Message" -ForegroundColor Red
}

# Check if helm is installed
if (-not (Get-Command helm -ErrorAction SilentlyContinue)) {
    Print-Error "Helm is not installed. Please install Helm first."
    Write-Host "Download from: https://github.com/helm/helm/releases"
    exit 1
}

# Check if kubectl is installed
if (-not (Get-Command kubectl -ErrorAction SilentlyContinue)) {
    Print-Error "kubectl is not installed. Please install kubectl first."
    exit 1
}

# Check Kubernetes connection
try {
    kubectl cluster-info 2>$null | Out-Null
} catch {
    Print-Error "Cannot connect to Kubernetes cluster. Please check your connection."
    exit 1
}

Print-Info "Starting BDSP Pipeline deployment..."

# Function to install/upgrade
function Invoke-Deploy {
    param([string[]]$Args)
    
    Print-Info "Validating Helm chart..."
    helm lint $CHART_DIR
    if ($LASTEXITCODE -ne 0) {
        Print-Error "Helm chart validation failed"
        exit 1
    }
    
    Print-Info "Deploying $RELEASE_NAME..."
    $helmArgs = @(
        'upgrade', '--install', $RELEASE_NAME, $CHART_DIR,
        '--create-namespace',
        '--wait',
        '--timeout', '10m'
    )
    if ($Args) {
        $helmArgs += $Args
    }
    
    & helm $helmArgs
    if ($LASTEXITCODE -ne 0) {
        Print-Error "Deployment failed"
        exit 1
    }
    
    Print-Info "Deployment completed successfully!"
    
    # Show release status
    helm status $RELEASE_NAME
    
    Print-Info "Getting pod status..."
    kubectl get pods -n $NAMESPACE
}

# Function to uninstall
function Invoke-Uninstall {
    Print-Warning "Uninstalling $RELEASE_NAME..."
    helm uninstall $RELEASE_NAME 2>$null
    
    Print-Warning "Deleting namespace $NAMESPACE..."
    kubectl delete namespace $NAMESPACE --ignore-not-found=true 2>$null
    
    Print-Info "Uninstallation completed!"
}

# Function to show status
function Show-Status {
    Print-Info "Helm release status:"
    helm status $RELEASE_NAME
    if ($LASTEXITCODE -ne 0) {
        Print-Error "Release not found"
    }
    
    Print-Info "Pods status:"
    kubectl get pods -n $NAMESPACE
    if ($LASTEXITCODE -ne 0) {
        Print-Error "Namespace not found"
    }
    
    Print-Info "Services:"
    kubectl get svc -n $NAMESPACE
    if ($LASTEXITCODE -ne 0) {
        Print-Error "Namespace not found"
    }
}

# Function to dry-run
function Invoke-DryRun {
    param([string[]]$Args)
    
    Print-Info "Running dry-run..."
    $helmArgs = @(
        'upgrade', '--install', $RELEASE_NAME, $CHART_DIR,
        '--dry-run',
        '--debug'
    )
    if ($Args) {
        $helmArgs += $Args
    }
    
    & helm $helmArgs
}

# Function to show values
function Show-Values {
    Print-Info "Current values:"
    helm get values $RELEASE_NAME
    if ($LASTEXITCODE -ne 0) {
        Print-Error "Release not found"
    }
}

# Main script execution
switch ($Command) {
    'deploy' {
        Invoke-Deploy -Args $AdditionalArgs
    }
    'install' {
        Invoke-Deploy -Args $AdditionalArgs
    }
    'uninstall' {
        Invoke-Uninstall
    }
    'status' {
        Show-Status
    }
    'dry-run' {
        Invoke-DryRun -Args $AdditionalArgs
    }
    'values' {
        Show-Values
    }
    default {
        Write-Host @"
Usage: .\helm-deploy.ps1 {deploy|install|uninstall|status|dry-run|values} [additional-helm-options]

Commands:
  deploy|install  - Install or upgrade the Helm chart
  uninstall       - Uninstall the Helm chart and delete namespace
  status          - Show deployment status
  dry-run         - Test the deployment without applying
  values          - Show current values

Examples:
  .\helm-deploy.ps1 deploy
  .\helm-deploy.ps1 deploy --set postgres.persistence.size=20Gi
  .\helm-deploy.ps1 deploy -f custom-values.yaml
  .\helm-deploy.ps1 dry-run
  .\helm-deploy.ps1 status
  .\helm-deploy.ps1 uninstall
"@
        exit 1
    }
}
