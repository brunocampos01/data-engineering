variable "client_name" {
    type = string
}

variable "rds_username" {
  default = "brunocampos01"
}

variable "ec2_elastic_type" {
    default = "t3.xlarge"
}

variable "rds_password" {
  default = "123456"
}

variable "key_name" {}

variable "vpc_id" {}

variable "private_subnets" {}

variable "public_subnets" {}

