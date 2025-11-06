# Create a resource group if it doesn’t exist
resource "azurerm_resource_group" "rg" {
    name                        = "${var.product_client_name}-rg"
    location                    = var.location
    tags                        = var.tags
}

resource "azurerm_sql_server" "PRODUCT_NAME" {
    name                        = "${var.product_client_name}-sqlserver"
    resource_group_name         = azurerm_resource_group.rg.name
    location                    = var.location
    version                     = "12.0"
    administrator_login         = var.administrator_login
    administrator_login_password= var.administrator_password
    tags                        = var.tags
}

resource "azurerm_sql_database" "PRODUCT_NAME" {
    name                        = "${var.product_client_name}-db"
    resource_group_name         = azurerm_resource_group.rg.name
    location                    = var.location
    server_name                 = azurerm_sql_server.PRODUCT_NAME.name
    edition                     = "Basic"
    tags                        = var.tags
}
