variable "location" {
    default = "Central US"
}

variable "administrator_login" {
    default = "brunocampos01"
}

variable "administrator_password" {}

variable "tags" {
    type = "map"
    default = {}
}

variable "custom_url" {
    default = ""
}

variable "client_name" {
    type = string
}

variable "product_client_name" {
    type = string
}

variable "application_user_login" {
    type = string
    description = "automation account"
}

variable "application_user_password" {
    type = string
    description = "automation account"
}
