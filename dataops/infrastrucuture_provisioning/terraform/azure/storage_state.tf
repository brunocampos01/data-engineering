# Create a resource group if it doesn’t exist
resource "azurerm_resource_group" "rg" {
  name                          = "${var.product_client_name}-rg"
  location                      = var.location
  tags                          = var.tags
}

terraform {
  backend "azurerm" {
    resource_group_name         = "StorageAccount-ResourceGroup"
    storage_account_name        = "abcd1234"
    container_name              = "tfstate"
    key                         = "prod.terraform.tfstate"
  }
}
