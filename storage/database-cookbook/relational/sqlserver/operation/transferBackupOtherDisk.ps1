# TODO: refactor hardcode

param(
    [parameter(Mandatory=$false)]
    [string] $DestinationPath = '<192.168.0.19>',

    [parameter(Mandatory=$false)]
    [string] $ServerIP = "<192.168.0.1>"
)

#******************************************************************************
# logs
#******************************************************************************
$ErrorActionPreference = "Stop"
filter timestamp {"[$(Get-Date -Format G)]: $_"}
Write-Output "Script started with UTC zone." | timestamp

Write-Output "PARAMETERS: `
                        DestinationPath: $DestinationPath `
                        ServerIP: $ServerIP " | timestamp

#******************************************************************************
# into Server
#******************************************************************************
$LocalIP = netsh interface ip show config | findstr $ServerIP
$LocalIP = $LocalIP -replace "[^0-9.]" , '' # only number and dot

if($ServerIP -eq $LocalIP) {
    Write-Output "Transferring backup to $DestinationPath" | timestamp
    # transfer sqlserver
    robocopy *.* 'C:\Program Files\Microsoft SQL Server\MSSQL15.MSSQLSERVER\MSSQL\Backup'  $DestinationPath /mir
    # transfer ssas
    robocopy *.* 'C:\Program Files\Microsoft SQL Server\MSAS15.MSSQLSERVER\OLAP\Backup'  $DestinationPath

    # delete files older than X days
    Get-ChildItem $DestinationPath `
        -Recurse `
         -File `
         | Where LastWriteTime -lt  (Get-Date).AddDays(-1)  `
         | Remove-Item -Force

} else {
    Write-Warning "Execution Process out Server. Please check ServerIP `
        LocalIP = $LocalIP `
        ServerIP = $ServerIP" | timestamp
        Start-Sleep -s 6000
}

Write-Output "Script finished" | timestamp
Start-Sleep -s 30
