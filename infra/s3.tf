resource "aws_s3_bucket" "hot_reload" {
  count         = data.aws_caller_identity.this.id == "000000000000" ? 1 : 0
  bucket        = "hot-reload"
  force_destroy = true

  lifecycle {
    prevent_destroy = false
  }

  tags = local.app_tags
}

resource "aws_s3_bucket" "this" {
  bucket        = format("%s-website-%s", var.aws_project, local.app_id)
  force_destroy = true

  lifecycle {
    prevent_destroy = false
  }

  tags = local.app_tags
}

resource "aws_s3_bucket_ownership_controls" "this" {
  bucket = aws_s3_bucket.this.id

  rule {
    object_ownership = "BucketOwnerPreferred"
  }
}

resource "aws_s3_bucket_acl" "this" {
  bucket = aws_s3_bucket.this.id
  acl    = "private"

  depends_on = [aws_s3_bucket_ownership_controls.this]
}

resource "aws_s3_bucket_website_configuration" "this" {
  bucket = aws_s3_bucket.this.id

  index_document {
    suffix = "index.html"
  }

  error_document {
    key = "index.html"
  }
}

# ---- ACME talent data pipeline (data/talent-etl): source and data lake buckets, AWS only ----
# Names follow the participant pattern (<project>-<app_id>-...-<app_id>) used by the workshop IAM policy.
resource "aws_s3_bucket" "talent_source" {
  count         = data.aws_caller_identity.this.id != "000000000000" ? 1 : 0
  bucket        = local.talent_bucket_names.source
  force_destroy = true
  tags          = local.app_tags
}

resource "aws_s3_bucket" "talent_lake" {
  count         = data.aws_caller_identity.this.id != "000000000000" ? 1 : 0
  bucket        = local.talent_bucket_names.lake
  force_destroy = true
  tags          = local.app_tags
}

# The provided sample data, which the job loads into Aurora and the source bucket if they are empty.
resource "aws_s3_object" "talent_sample" {
  count  = data.aws_caller_identity.this.id != "000000000000" ? 1 : 0
  bucket = one(aws_s3_bucket.talent_source.*.id)
  key    = "seed/data-engineering-sample-performance-scenario.zip"
  source = abspath("${path.module}/../sampleData/data-engineering-sample-performance-scenario.zip")
  etag   = filemd5("${path.module}/../sampleData/data-engineering-sample-performance-scenario.zip")
  tags   = local.app_tags
}
