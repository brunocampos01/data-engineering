# Create a resource group if it doesn’t exist
resource "azurerm_resource_group" "rg" {
    name                        = "${var.product_client_name}-rg"
    location                    = var.location
    tags                        = var.tags
}

resource "azurerm_automation_account" "automation_account" {
    name                        = "${var.product_client_name}-autoacc"
    location                    = var.location
    resource_group_name         = azurerm_resource_group.rg.name
    sku_name                    = "Basic"
    tags                        = var.tags
}

resource "azurerm_automation_credential" "automation_credential" {
    name                        = "application_user"
    resource_group_name         = azurerm_resource_group.rg.name
    account_name                = azurerm_automation_account.automation_account.name
    username                    = var.application_user_login
    password                    = var.application_user_password
}

#******************************************************************************
# Modules powershell
#******************************************************************************
resource "azurerm_automation_module" "azurerm_profile" {
    name                        = "AzureRM.Profile"
    resource_group_name         = azurerm_resource_group.rg.name
    automation_account_name     = azurerm_automation_account.automation_account.name
    module_link {
        uri                     = "https://www.powershellgallery.com/api/v2/package/AzureRM.profile/5.8.3"
    }
}

resource "azurerm_automation_module" "azurerm_powerbi" {
    name                        = "AzureRM.PowerBIEmbedded"
    resource_group_name         = azurerm_resource_group.rg.name
    automation_account_name     = azurerm_automation_account.automation_account.name
    module_link {
        uri                     = "https://www.powershellgallery.com/api/v2/package/AzureRM.PowerBIEmbedded/4.1.10"
    }
    depends_on                  = [azurerm_automation_module.azurerm_profile]
}

resource "azurerm_automation_module" "azurerm_sql" {
    name                        = "AzureRM.Sql"
    resource_group_name         = azurerm_resource_group.rg.name
    automation_account_name     = azurerm_automation_account.automation_account.name
    module_link {
        uri                     = "https://www.powershellgallery.com/api/v2/package/AzureRM.Sql/4.12.1"
    }
    depends_on                  = [azurerm_automation_module.azurerm_powerbi]
}
#******************************************************************************
# Runbooks
#******************************************************************************
data "local_file" "start_stop_powerbi_runbook_file" {
    filename                    = "${path.module}/runbooks/${var.product_client_name}-start-stop-powerbi-runbook.ps1"
    # Module workaround for create depends_on
    depends_on                  = [azurerm_automation_account.automation_account]
}

resource "azurerm_automation_runbook" "start_stop_powerbi_runbook" {
    name                        = "${var.product_client_name}-start-stop-powerbi-runbook"
    location                    = var.location
    resource_group_name         = azurerm_resource_group.rg.name
    account_name                = azurerm_automation_account.automation_account.name
    log_verbose                 = "true"
    log_progress                = "true"
    description                 = "Start and stop powerBI Embedded"
    runbook_type                = "PowerShell"
    tags                        = var.tags
    publish_content_link {
        uri                     = "https://raw.githubusercontent.com/Azure/azure-quickstart-templates/c4935ffb69246a6058eb24f54640f53f69d3ac9f/101-automation-runbook-getvms/Runbooks/Get-AzureVMTutorial.ps1"
    }
    content                     = data.local_file.start_stop_powerbi_runbook_file.content
}


data "local_file" "start_stop_vm_runbook_file" {
    filename                    = "${path.module}/runbooks/${var.product_client_name}-start-stop-vm-runbook.ps1"
    # Module workaround for create depends_on
    depends_on                  = [azurerm_automation_account.automation_account]
}

resource "azurerm_automation_runbook" "start_stop_vm_runbook" {
    name                        = "${var.product_client_name}-start-stop-vm-runbook"
    location                    = var.location
    resource_group_name         = azurerm_resource_group.rg.name
    account_name                = azurerm_automation_account.automation_account.name
    log_verbose                 = "true"
    log_progress                = "true"
    description                 = "Stat and stop Vvrtual machine"
    runbook_type                = "PowerShell"
    tags                        = var.tags
    publish_content_link {
        uri                     = "https://raw.githubusercontent.com/Azure/azure-quickstart-templates/c4935ffb69246a6058eb24f54640f53f69d3ac9f/101-automation-runbook-getvms/Runbooks/Get-AzureVMTutorial.ps1"
    }
    content                     = data.local_file.start_stop_vm_runbook_file.content
}

data "local_file" "update_modules_powershell_certificate_file" {
    filename                    = "${path.module}/runbooks/${var.product_client_name}-update-modules-runbook.ps1"
    # Module workaround for create depends_on
    depends_on                  = [azurerm_automation_account.automation_account]
}

resource "azurerm_automation_runbook" "update_modules_powershell_runbook" {
    name                        = "${var.product_client_name}-update-modules-runbook"
    location                    = var.location
    resource_group_name         = azurerm_resource_group.rg.name
    account_name                = azurerm_automation_account.automation_account.name
    log_verbose                 = "true"
    log_progress                = "true"
    description                 = "Update modules powershell"
    runbook_type                = "PowerShell"
    tags                        = var.tags
    publish_content_link {
        uri                     = "https://raw.githubusercontent.com/Azure/azure-quickstart-templates/c4935ffb69246a6058eb24f54640f53f69d3ac9f/101-automation-runbook-getvms/Runbooks/Get-AzureVMTutorial.ps1"
    }
    content                     = data.local_file.update_modules_powershell_certificate_file.content
}
#******************************************************************************
# Schedules
#******************************************************************************
resource "azurerm_automation_schedule" "update_modules_schedule" {
    name                        = "update-modules-schedule"
    resource_group_name         = azurerm_resource_group.rg.name
    automation_account_name     = azurerm_automation_account.automation_account.name
    description                 = "Update modules of powershell"
    frequency                   = "OneTime"
    timezone                    = "E. South America Standard Time"
    start_time                  = timeadd(timestamp(), "30m")
}

resource "azurerm_automation_schedule" "start_powerbi_schedule" {
    name                        = "start-powerbi-schedule"
    resource_group_name         = azurerm_resource_group.rg.name
    automation_account_name     = azurerm_automation_account.automation_account.name
    description                 = "Start PowerBI embedded"
    frequency                   = "OneTime"
    timezone                    = "E. South America Standard Time"
    start_time                  = timeadd(timestamp(), "55m")
}

resource "azurerm_automation_schedule" "stop_powerbi_schedule" {
    name                        = "stop-powerbi-schedule"
    resource_group_name         = azurerm_resource_group.rg.name
    automation_account_name     = azurerm_automation_account.automation_account.name
    description                 = "Stop PowerBI embedded"
    frequency                   = "Week"
    week_days                   = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
    timezone                    = "E. South America Standard Time"
    start_time                  = timeadd(timestamp(), "65m")
}

resource "azurerm_automation_schedule" "start_vm_schedule" {
    name                        = "start-vm-schedule"
    resource_group_name         = azurerm_resource_group.rg.name
    automation_account_name     = azurerm_automation_account.automation_account.name
    description                 = "Start virtual machine"
    frequency                   = "Week"
    week_days                   = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
    timezone                    = "E. South America Standard Time"
    start_time                  = timeadd(timestamp(), "70m")
}

resource "azurerm_automation_schedule" "stop_vm_schedule" {
    name                        = "stop-vm-schedule"
    resource_group_name         = azurerm_resource_group.rg.name
    automation_account_name     = azurerm_automation_account.automation_account.name
    description                 = "Stop virtual machine"
    frequency                   = "Week"
    week_days                   = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
    timezone                    = "E. South America Standard Time"
    start_time                  = timeadd(timestamp(), "75m")
}
#******************************************************************************
# Runbooks + Schedules
#******************************************************************************
resource "azurerm_automation_job_schedule" "update_modules_job_schedule" {
    resource_group_name         = azurerm_resource_group.rg.name
    automation_account_name     = azurerm_automation_account.automation_account.name
    runbook_name                = azurerm_automation_runbook.update_modules_powershell_runbook.name
    schedule_name               = azurerm_automation_schedule.update_modules_schedule.name
}

resource "azurerm_automation_job_schedule" "start_powerbi_job_schedule" {
    resource_group_name         = azurerm_resource_group.rg.name
    automation_account_name     = azurerm_automation_account.automation_account.name
    runbook_name                = azurerm_automation_runbook.start_stop_powerbi_runbook.name
    schedule_name               = azurerm_automation_schedule.start_powerbi_schedule.name
}

resource "azurerm_automation_job_schedule" "stop_powerbi_job_schedule" {
    resource_group_name         = azurerm_resource_group.rg.name
    automation_account_name     = azurerm_automation_account.automation_account.name
    runbook_name                = azurerm_automation_runbook.start_stop_powerbi_runbook.name
    schedule_name               = azurerm_automation_schedule.stop_powerbi_schedule.name
}

resource "azurerm_automation_job_schedule" "start_vm_job_schedule" {
    resource_group_name         = azurerm_resource_group.rg.name
    automation_account_name     = azurerm_automation_account.automation_account.name
    runbook_name                = azurerm_automation_runbook.start_stop_vm_runbook.name
    schedule_name               = azurerm_automation_schedule.start_vm_schedule.name
}

resource "azurerm_automation_job_schedule" "stop_vm_job_schedule" {
    resource_group_name         = azurerm_resource_group.rg.name
    automation_account_name     = azurerm_automation_account.automation_account.name
    runbook_name                = azurerm_automation_runbook.start_stop_vm_runbook.name
    schedule_name               = azurerm_automation_schedule.stop_vm_schedule.name
}
