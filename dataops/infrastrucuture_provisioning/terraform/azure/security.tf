# Create a resource group if it doesn’t exist
resource "azurerm_resource_group" "rg" {
    name                        = "${var.product_client_name}-rg"
    location                    = var.location
    tags                        = var.tags
}

resource "azurerm_sql_firewall_rule" "company_1" {
    name                        = "${var.product_client_name}-sql-firew-rule-1"
    resource_group_name         = azurerm_resource_group.rg.name
    server_name                 = azurerm_sql_server.PRODUCT_NAME.name
    start_ip_address            = "155.55.55.55"
    end_ip_address              = "255.255.255.255"
}

resource "azurerm_sql_firewall_rule" "company_2" {
    name                          = "${var.product_client_name}-sql-firew-rule-2"
    resource_group_name           = azurerm_resource_group.rg.name
    server_name                   = azurerm_sql_server.PRODUCT_NAME.name
    start_ip_address              = "101.101.101.101"
    end_ip_address                = "255.255.255.255"
}

resource "azurerm_sql_firewall_rule" "company_3" {
    name                        = "${var.product_client_name}-sql-firew-rule-3"
    resource_group_name         = azurerm_resource_group.rg.name
    server_name                 = azurerm_sql_server.PRODUCT_NAME.name
    start_ip_address            = "0.0.0.0"
    end_ip_address              = "255.255.255.255"
}

resource "azurerm_sql_firewall_rule" "acess_container" {
    name                        = "${var.product_client_name}-sql-firew-rule-4"
    resource_group_name         = azurerm_resource_group.rg.name
    server_name                 = azurerm_sql_server.PRODUCT_NAME.name
    start_ip_address            = azurerm_container_group.PRODUCT_NAME.ip_address
    end_ip_address              = azurerm_container_group.PRODUCT_NAME.ip_address
}
