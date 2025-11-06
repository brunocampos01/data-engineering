# Terraform 

## Samples
### Azure
- [Case 1](https://github.com/brunocampos01/business-intelligence-at-azure)
  - Azure Analysis Services
  - Azure Automation Account
  - Azure Storage Account
  - Azure Log Analytics
  - Runbooks Scripts Generator

- [Case 2](azure/)
  - Azure Automation Account
  - Container
  - Security
  - DataBase

### AWS

 
---

## Install
Check which latest version in https://www.terraform.io/downloads.html
```
wget https://releases.hashicorp.com/terraform/0.12.18/terraform_0.12.18_linux_amd64.zip
unzip terraform_0.12.18_linux_amd64.zip
sudo mv terraform /usr/local/bin/

terraform --version 
```

## Start service
O comando terraform init inicializa o diretório.
```
terraform init
```

### Validate
```
terraform plan --out plan.tf
```

### Running Script
Open directory with contains terraform file `<script>.tf`

```
terraform apply plan.out
``` 

## Structure Files
```
├── layers
│   ├── company-dev
│   │   ├── application
│   │   │   ├── main.tf
│   │   │   ├── output.tf
│   │   │   └── variables.tf
│   │   ├── global
│   │   │   ├── main.tf
│   │   │   ├── output.tf
│   │   │   └── variables.tf
```

## Variables

Using Input Variable Values
Within the module that declared a variable, its value can be accessed from within expressions as var.<NAME>, where <NAME> matches the label given in the declaration block:
```
resource "aws_instance" "example" {
istance_type = "t2.micro"
ami           = var.image_id
}
```
## Storage State
https://docs.microsoft.com/pt-br/azure/terraform/terraform-backend
    
    
# Usage Azure-CLI
```
resource "null_resource" "azure-cli" {
  
  provisioner "local-exec" {
    # Call Azure CLI Script here
    command = "ssl-script.sh"

    # We are going to pass in terraform derived values to the script
    environment {
      webappname = "${azurerm_app_service.demo.name}"
      resourceGroup = ${azurerm_resource_group.demo.name}
    }
  }

  depends_on = ["azurerm_app_service_custom_hostname_binding.demo"]
}
```
