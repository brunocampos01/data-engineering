resource "aws_s3_bucket" "dados" {
  bucket                        = "${lower(var.client_name)}-bucket"
}
