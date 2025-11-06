resource "azurerm_container_group" "PRODUCT_NAME" {
    name                        = "${var.product_client_name}-cont-group"
    location                    = var.location
    resource_group_name         =  azurerm_resource_group.rg.name
    ip_address_type             = "public"
    dns_name_label              = var.product_client_name
    os_type                     = "Linux"

    image_registry_credential {
        username                = "xpto"
        password                = "dockerdockerdockerdockerdocker"
        server                  = "docker.company.com"
    }

    container {
        name                    = "backend"
        image                   = "docker-endpoint"
        cpu                     = "0.5"
        memory                  = "0.5"
        ports {
            port                = 80
            protocol            = "TCP"
        }
        environment_variables   = {
            APP_NAME            = "brunocampos01"
            API_BASE_URL        = "http://${var.product_client_name}.${azurerm_resource_group}.azurecontainer.io:8081"
        }
    }

    container {
        name                    = "api"
        image                   = "docker-endpoint"
        cpu                     = "0.5"
        memory                  = "0.5"
        ports {
            port                = 8081
            protocol            = "TCP"
        }
        environment_variables   = {
            APP_NAME            = "brunocampos01"
            API_BASE_URL        = "http://${var.product_client_name}.${azurerm_resource_group}.azurecontainer.io:8081"
        }
    }

    tags                        = var.tags
}
