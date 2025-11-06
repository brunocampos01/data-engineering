resource "aws_security_group" "elastic" {
    name                        = "${var.client_name}-sec-group"
    vpc_id                      = var.vpc_id

    ingress {
        from_port               = 443
        to_port                 = 443
        protocol                = "tcp"
        cidr_blocks             = ["0.0.0.0/0"]
    }

    ingress {
        from_port               = 80
        to_port                 = 80
        protocol                = "tcp"
        cidr_blocks             = ["0.0.0.0/0"]
    }

    ingress {
        from_port               = 22
        to_port                 = 22
        protocol                = "tcp"
        cidr_blocks             = ["0.0.0.0/0"]
    }
    ingress {
        from_port               = 9200
        to_port                 = 9200
        protocol                = "tcp"
        cidr_blocks             = ["0.0.0.0/0"]
    }

    ingress {
        from_port               = 0
        to_port                 = 0
        protocol                = "-1"
        cidr_blocks             = ["10.10.10.0/22"]
    }

    egress {
        from_port               = 0
        to_port                 = 0
        protocol                = "-1"
        cidr_blocks             = ["0.0.0.0/0"]
    }
}

resource "aws_security_group" "postgresql" {
    vpc_id                      = var.vpc_id

    ingress {
        from_port               = 5432
        to_port                 = 5432
        protocol                = "TCP"
        cidr_blocks =           ["0.0.0.0/0"]
    }
}
