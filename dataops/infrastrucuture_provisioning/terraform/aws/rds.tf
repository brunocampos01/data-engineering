resource "aws_db_instance" "product_name" {
  allocated_storage             = 5
  storage_type                  = "gp2"
  engine                        = "postgres"
  instance_class                = "db.t2.micro"
  name                          = "brunocampos01"
  username                      = var.rds_username
  password                      = var.rds_password
  db_subnet_group_name          = aws_db_subnet_group.client_name
  vpc_security_group_ids        = [aws_security_group.postgresql.id]
  identifier                    = "product_name-client_name"
  tags = {
    Name                        = "product_name-client_name"
  }
}

resource "aws_db_subnet_group" "saj" {
  name                          = "product_name-client_name"
  subnet_ids                    = var.private_subnets

}

