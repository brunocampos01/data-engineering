Param (
    [Parameter(Mandatory=$false)]
    [String] $AzureVMName = "PRODUCT_NAME-client_name-vm",

    [Parameter(Mandatory = $false)]
    [String] $NameUser = "application_user",

    [parameter(Mandatory=$false)]
    [string] $ResourceGroupName = "PRODUCT_NAME-client_name-rg",

    [parameter(Mandatory=$false)]
    [string] $ServiceTimeZone = "E. South America Standard Time",

    [parameter(Mandatory=$false)]
    [string] $AutomationAccountName = "PRODUCT_NAME-client_name-autoacc",

    [parameter(Mandatory=$false)]
    [string] $configStr = "
                            [
                                {
                                    WeekDays:[1,2,3,4,5]
                                    ,StartTime: ""05:55:00""
                                    ,StopTime: ""19:55:00""
                                }
                            ]
                          "
)

#******************************************************************************
# Runbook logs
#******************************************************************************
$ErrorActionPreference = "Stop"
filter timestamp {"[$(Get-Date -Format G)]: $_"}
Write-Output "Script started with UTC zone." | timestamp

Write-Output "Parameters: `
                          $AzureVMName `
                          $NameUser `
                          $ServiceTimeZone `
                          $ResourceGroupName `
                          $AutomationAccountName `
                          $configStr "
#*****************************************************************************
# Set Timestamp
#*****************************************************************************
# Get current date/time and convert time zone
Write-Output "Handling time zone" | timestamp
Write-Output "Script started with UTC zone." | timestamp
$stateConfig = $configStr | ConvertFrom-Json
$startTime = Get-Date

Write-Output "Azure Automation local time: $startTime." | timestamp
$toTimeZone = [System.TimeZoneInfo]::FindSystemTimeZoneById($ServiceTimeZone)
Write-Output "Time zone convert to: $toTimeZone." | timestamp
$newTime = [System.TimeZoneInfo]::ConvertTime($startTime, $toTimeZone)
Write-Output "Converted time: $newTime." | timestamp
$startTime = $newTime

# Get current day of week based on converted start time
$currentDayOfWeek = [Int]($startTime).DayOfWeek
Write-Output "Current day of week: $currentDayOfWeek." | timestamp

# Find a match in the config
$dayObjects = $stateConfig `
    | Where-Object {$_.WeekDays -contains $currentDayOfWeek} `
    | Select-Object Sku, `
    @{ Name = "StartTime"; `
    Expression = {
        [datetime]::ParseExact($_.StartTime,"HH:mm:ss", [System.Globalization.CultureInfo]::InvariantCulture)}}, `
    @{ Name = "StopTime";`
    Expression = {
        [datetime]::ParseExact($_.StopTime,"HH:mm:ss", [System.Globalization.CultureInfo]::InvariantCulture)}}

#******************************************************************************
# Logging in to Azure
#******************************************************************************
Write-Output "Logging in to Azure..." | timestamp

try {
    # Ensures you do not inherit an AzureRMContext in your runbook
    Disable-AzureRmContextAutosave –Scope Process

    $connection = Get-AutomationConnection -Name AzureRunAsConnection

    while(!($connectionResult) -And ($logonAttempt -le 10)) {
        $LogonAttempt++
        # Logging in to Azure...
        $connectionResult = Login-AzureRmAccount `
                                -ServicePrincipal `
                                -Tenant $connection.TenantID `
                                -ApplicationID $connection.ApplicationID `
                                -CertificateThumbprint $connection.CertificateThumbprint

        Start-Sleep -Seconds 5
    }

} catch {
    Write-Output "Logging FAILED !" | timestamp
    Write-Output "connection: $connection"
    Write-Output $_.Exception
    Write-Error -Message $_.Exception

    throw $_.Exception
}

#******************************************************************************
# Azure Virtual Machine
#******************************************************************************
try {
    # Get status
    $AzureStateVM = Get-AzureRmVM -ResourceGroupName $ResourceGroupName -Name $AzureVMName -Status
    $VMState = $AzureStateVM.Statuses.DisplayStatus
    Write-Output "Azure Virtual Machine STATUS: $VMState" | timestamp

    if($dayObjects -ne $null) {
        $matchingObject = $dayObjects | Where-Object {($startTime -ge $_.StartTime) -and ($startTime -lt $_.StopTime)} | Select-Object -First 1

        if($matchingObject -ne $null) {
            # if Paused, resume
            if($VMState -eq "VM deallocated") {
                Write-Output "Resuming vm..." | timestamp
                Get-AzureRmVM | ? {$_.Name -eq $AzureVMName} | Start-AzureRmVM
            }
        }else {

            if($VMState -eq "VM running"){
                Write-Output "Pausing vm..." | timestamp
                Get-AzureRmVM | ? {$_.Name -eq $AzureVMName} | Stop-AzureRmVM -Force
            }
        }
    }else {
        Write-Output "No object config for current day of week" | timestamp

        if($VMState -eq "VM running") {
            Write-Output "Pausing vm..." | timestamp
            Get-AzureRmVM | ? {$_.Name -eq $AzureVMName} | Stop-AzureRmVM -Force
        }
    }

} catch {
    Write-Output "FAILED !" | timestamp
    Write-Output "$($_.Exception.Message)"

    throw $_.Exception

} finally {
    Write-Output "--- Script finished ---" | timestamp
}
