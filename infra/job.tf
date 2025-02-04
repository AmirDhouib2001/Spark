resource "aws_glue_job" "spark_job" {
  name     = "spark-job-zeineb"
  role_arn = aws_iam_role.glue.arn

  command {
    script_location = "s3://spark-job-bucket-zeineb/spark_app.py"
  }

  glue_version = "3.0"
  number_of_workers = 2
  worker_type = "Standard"

  default_arguments = {
    "--additional-python-modules" = "s3://spark-job-bucket-zeineb/packages/spark_handson-0.1.0-py3-none-any.whl"
    "--job-language" = "python"
  }
}
