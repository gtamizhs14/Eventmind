variable "aws_region" {
  default = "us-east-1"
}

variable "project_name" {
  default = "eventmind"
}

variable "environment" {
  default = "prod"
}

variable "eks_cluster_version" {
  description = "Kubernetes version for EKS"
  default     = "1.29"
}

variable "node_instance_type" {
  default = "t3.medium"
}

variable "node_desired_count" {
  default = 2
}

variable "node_min_count" {
  default = 1
}

variable "node_max_count" {
  default = 5
}
