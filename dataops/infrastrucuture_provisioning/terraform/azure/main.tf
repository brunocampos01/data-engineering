module "PRODUCT_NAME-CLIENT_NAME" {
    source                      = "../../terraform-modules/PRODUCT_NAME"
    administrator_password      = "123456"
    custom_url                  = "CLIENT_NAME.PRODUCT_NAME.com"
    application_user_login      = "gatewaypbi@PRODUCT_NAME.com.br"
    application_user_password   = "saj@insights#123!"
    client_name                 = "CLIENT_NAME"
    product_client_name         = "PRODUCT_NAME-CLIENT_NAME"

    tags = {
        PRODUCT_NAME            = "CLIENT_NAME"
    }
}

output "fqdn-CLIENT_NAME" {
    value = module.PRODUCT_NAME-CLIENT_NAME.fqdn
}
