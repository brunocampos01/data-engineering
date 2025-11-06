$TenantName = '<TenantName>'
$Site = '<Site>'
$SiteURL = "https://$TenantName.sharepoint.com/sites/$Site"
$ListName = "List Name"
$CSVPath = "C:\sharepoint_dump_list.csv"

$Login = "brunocampos01@gmail.com";
$Pwd = "<password>";
$Pwd = ConvertTo-SecureString $pwd -AsPlainText -Force;
#******************************************************************************
# Add-SharePointLibraries
#******************************************************************************
try {
    Write-Output "Add Libraries SharePoint"
    Add-Type -Path "C:\Program Files\Common Files\Microsoft Shared\Web Server Extensions\16\ISAPI\Microsoft.SharePoint.Client.dll"
    Add-Type -Path "C:\Program Files\Common Files\Microsoft Shared\Web Server Extensions\16\ISAPI\Microsoft.SharePoint.Client.Runtime.dll"
}
catch {
    Write-Error "Unable to load SharePoint Client Libraries"
}

#******************************************************************************
# Get credentials
#******************************************************************************
$Cred = Get-Credential
$Credentials = New-Object Microsoft.SharePoint.Client.SharePointOnlineCredentials($Cred.Username, $Cred.Password)

#******************************************************************************
# Connecting to sharepoint site
#******************************************************************************
try {
    Write-Output "Connecting to sharepoint site $SiteURL."
    # Setup the context
    $Ctx = New-Object Microsoft.SharePoint.Client.ClientContext($SiteURL)
    $Ctx.Credentials = $Credentials

    $List = $Ctx.web.Lists.GetByTitle($ListName)
    $Query = New-Object Microsoft.SharePoint.Client.CamlQuery
    $ListItems = $List.GetItems($Query)
    $FieldColl = $List.Fields
    $Ctx.Load($ListItems)
    $Ctx.Load($FieldColl)
    $Ctx.ExecuteQuery()
    $ListItemCollection = @()

    Foreach ($Item in $ListItems) {
        $ExportItem = New-Object PSObject
        Foreach ($Field in $FieldColl) {
            if ($NULL -ne $Item[$Field.InternalName]) {
                #Expand the value of Person or Lookup fields
                $FieldType = $Item[$Field.InternalName].GetType().name

                if (($FieldType -eq "FieldLookupValue") -or ($FieldType -eq "FieldUserValue")) {
                    $FieldValue = $Item[$Field.InternalName].LookupValue
                }
                else {
                    $FieldValue = $Item[$Field.InternalName]
                    #Write-Output $Field.InternalName # columns name
                }
            }
            $ExportItem | Add-Member -MemberType NoteProperty -name $Field.InternalName -value $FieldValue
        }
        $ListItemCollection += $ExportItem
    }
} catch {
    Write-Error "Error in connecting to sharepoint site $SiteURL"
}

$ListItemCollection | Export-CSV $CSVPath -Encoding "UTF8"
Write-host "List data Exported to CSV file successfully!"
