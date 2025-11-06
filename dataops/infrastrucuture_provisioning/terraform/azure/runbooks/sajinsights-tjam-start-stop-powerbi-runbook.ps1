<#
.DESCRIPTION
    Resume/stop an Azure Power BI Embedded Capacity according to a schedule using Azure Automation.
.PARAMETER resourceGroupName
    Name of the resource group to which the capacity is assigned.
.PARAMETER azureRunAsConnectionName
    Azure Automation Run As account name. Needs to be able to access
    the $capacityName.
.PARAMETER serverName
    Azure Power BI Embedded Capacity name.
.EXAMPLE
        [string] $resourceGroupName = "PRODUCT_NAME-client_name-TF",
        [string] $serverName = "client_name"
#>
param(
    [parameter(Mandatory=$false)]
    [string] $resourceGroupName = "PRODUCT_NAME-client_name-TF",

    [parameter(Mandatory=$false)]
    [string] $serverName = "client_name",

    [parameter(Mandatory=$false)]
    [string] $azureProfilePath  = "",

    [parameter(Mandatory=$false)]
    [string] $azureRunAsConnectionName = "AzureRunAsConnection",

    [parameter(Mandatory=$false)]
    [string] $serviceTimeZone = "E. South America Standard Time",

    [parameter(Mandatory=$false)]
    [string] $configStr = "
                            [
                                {
                                    WeekDays:[1,2,3,4,5]
                                    ,StartTime: ""07:55:00""
                                    ,StopTime: ""19:55:00""
                                    ,Sku: ""A1""
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

Write-Output "Parameters: $resourceGroupName,`
                          $serverName,`
                          $configStr,`
                          $azureProfilePath,`
                          $azureRunAsConnectionName" | timestamp
# Verbose mode
$VerbosePreference = "Continue"
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
# Azure Power BI
#******************************************************************************
try {
    # Get status
    $pbiEmbCap = Get-AzureRmPowerBIEmbeddedCapacity -ResourceGroupName $resourceGroupName `
                                                    -Name $capacityName
    Write-Output "PBI found: $($pbiEmbCap.Name)" | timestamp
    Write-Output "Current pricing tier: $($pbiEmbCap.Sku)" | timestamp
    Write-Output "Current PBI STATUS: $($pbiEmbCap.State)" | timestamp

    # If not match any day then exit
    if($dayObjects -ne $null) {
        # Can't treat several objects for same time-frame, if there's more than one, pick first
        $matchingObject = $dayObjects | Where-Object { ($startTime -ge $_.StartTime) -and ($startTime -lt $_.StopTime) } | Select-Object -First 1

        if($matchingObject -ne $null) {
            # if Paused, resume
            if($pbiEmbCap.State -eq "Paused") {
                Write-Output "PBI was paused. Resuming..." | timestamp
                $pbiEmbCap = Resume-AzureRmPowerBIEmbeddedCapacity -Name $pbiEmbCap.Name `
                                                                   -ResourceGroupName $resourceGroupName
                Write-Output "PBI resumed." | timestamp
                $pbiEmbCap = Get-AzureRmPowerBIEmbeddedCapacity -ResourceGroupName $resourceGroupName `
                                                                -Name $capacityName
                Write-Output "Current PBI  state: $($pbiEmbCap.State)" | timestamp
            }

        }
        else {
            if($pbiEmbCap.State -eq "Succeeded"){
                Write-Output "PBI resumed. Pausing" | timestamp
                $pbiEmbCap | Suspend-AzureRmPowerBIEmbeddedCapacity  -Verbose
            }
        }
    }
    else {
        Write-Output "No object config for current day of week" | timestamp
        if($pbiEmbCap.State -eq "Succeeded") {
            Write-Output "PBI resumed. Pausing" | timestamp
            $pbiEmbCap | Suspend-AzureRmPowerBIEmbeddedCapacity  -Verbose
        }
    }
} catch {
    Write-Output "FAILED !" | timestamp
    Write-Output "$($_.Exception.Message)"

    throw $_.Exception

} finally {
    Write-Output "--- Script finished ---" | timestamp
}
