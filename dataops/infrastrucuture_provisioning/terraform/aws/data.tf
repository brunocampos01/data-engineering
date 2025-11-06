data "aws_ami" "ubuntu" {
  most_recent                   = true

  filter {
    name                        = "virtualization-type"
    values                      = ["hvm"]
  }

  filter {
    name                        = "name"
    values                      = ["ubuntu/images/hvm-ssd/ubuntu-bionic-18.04-amd64-server-*"]
  }

  owners                        = ["666666666666666"]
}
