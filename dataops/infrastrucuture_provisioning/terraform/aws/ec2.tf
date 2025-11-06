resource "aws_instance" "elastic-ec2" {
    instance_type               = var.ec2_elastic_type
    vpc_security_group_ids      = [aws_security_group.elastic.id]
    ami                         = data.aws_ami.ubuntu.id
    key_name                    = var.key_name
    subnet_id                   = element(var.private_subnets, 0)
    associate_public_ip_address = false

    root_block_device {
        volume_size             = "100"
    }
}
