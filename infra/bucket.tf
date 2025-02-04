# resource "aws_s3_bucket" "spark_job_bucket" {
#   bucket = "spark-job-bucket-zeineb"
#
#   tags = {
#     Name = "spark-job-bucket"
#   }
#
#   lifecycle {
#     prevent_destroy = true
#     ignore_changes  = all
#   }
# }
