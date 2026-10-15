resource "aws_eks_cluster" "this" {
  count    = data.aws_caller_identity.this.id != "000000000000" && var.aws_eks_enabled ? 1 : 0
  name     = format("%s-%s", var.aws_project, local.app_id)
  version  = "1.36"
  role_arn = local.eks_role_arn

  access_config {
    authentication_mode                         = "API_AND_CONFIG_MAP"
    bootstrap_cluster_creator_admin_permissions = true
  }

  vpc_config {
    endpoint_public_access  = true
    endpoint_private_access = true
    subnet_ids              = local.public_subnet_ids
    security_group_ids      = data.aws_security_groups.this.ids
  }

  tags = local.app_tags
}

resource "aws_eks_node_group" "this" {
  count           = data.aws_caller_identity.this.id != "000000000000" && var.aws_eks_enabled ? 1 : 0
  cluster_name    = one(aws_eks_cluster.this.*.name)
  node_group_name = format("%s-%s", var.aws_project, local.app_id)
  node_role_arn   = local.eks_role_arn
  subnet_ids      = local.public_subnet_ids
  capacity_type   = var.aws_eks_type
  instance_types  = ["t3.medium", "t3a.medium", "t2.medium"]
  disk_size       = 50

  scaling_config {
    desired_size = 1
    max_size     = 1
    min_size     = 1
  }

  update_config {
    max_unavailable = 1
  }

  tags = local.app_tags
}

# The participant's directory account (u<app_id>) is allowed by name as well as by group: the
# directory has no group CN=<app_id>, so the group check alone rejects every login. Logging in
# still requires the account's directory password.
locals {
  jupyterhub_allowed_user = format("u%s", local.app_id)

  # Notebook servers read the talent data lake (read-only) through EKS Pod Identity, and get the
  # talent insights notebook and its code at every start (see null_resource.jupyter_talent_files).
  jupyter_service_account = "jupyter-user"
  jupyter_values = {
    singleuser = {
      serviceAccountName = local.jupyter_service_account
      extraEnv = {
        IS_LOCAL           = "false"
        AWS_REGION         = data.aws_region.this.region
        AWS_DEFAULT_REGION = data.aws_region.this.region
        LAKE_BUCKET        = local.talent_bucket_names.lake
        SOURCE_BUCKET      = local.talent_bucket_names.source
        # The notebook reads only S3; the pipeline's settings require these in cloud mode, but no
        # database credentials are given to notebook servers.
        POSTGRES_HOST = "not-used-by-notebooks"
        POSTGRES_NAME = "not-used-by-notebooks"
        POSTGRES_USER = "not-used-by-notebooks"
        POSTGRES_PASS = "not-used-by-notebooks"
      }
      storage = {
        # Replaces the list in helm/config.yaml, so the existing scratch volume is repeated here.
        extraVolumes = [
          { name = "scratch-volume", emptyDir = {} },
          { name = "talent-insights", configMap = { name = "talent-insights" } },
        ]
        extraVolumeMounts = [
          { name = "scratch-volume", mountPath = "/var/run/jupyter" },
          { name = "talent-insights", mountPath = "/opt/talent-insights", readOnly = true },
        ]
      }
      lifecycleHooks = { postStart = { exec = { command = ["sh", "/opt/talent-insights/setup.sh"] } } }
    }
  }
}

resource "null_resource" "helm_chart" {
  count = data.aws_caller_identity.this.id != "000000000000" && var.aws_eks_enabled ? 1 : 0

  triggers = {
    cluster_id   = one(aws_eks_node_group.this.*.id)
    source_hash  = local.helm_hash
    aws_ds_ip    = var.aws_ds_ip
    allowed_user = local.jupyterhub_allowed_user
    notebooks    = sha256(jsonencode(local.jupyter_values))
  }

  provisioner "local-exec" {
    environment = {
      JUPYTER_VALUES = jsonencode(local.jupyter_values)
    }
    command = <<-EOT
      WORK=$(mktemp -d) && trap 'rm -rf "$WORK"' EXIT && printf '%s' "$JUPYTER_VALUES" > "$WORK/notebooks.json" && \
      if [ -z "${var.aws_ds_ip}" ] || [ "${var.aws_ds_ip}" = "null" ]; then echo "ERROR: aws_ds_ip is not set"; exit 1; fi && \
      aws eks update-kubeconfig --region ${data.aws_region.this.region} --name ${one(aws_eks_cluster.this.*.name)} || true && \
      helm repo add jupyterhub https://hub.jupyter.org/helm-chart/ && helm repo update && \
      helm upgrade --cleanup-on-fail \
        --install jupyterhub jupyterhub/jupyterhub \
        --set proxy.secretToken="${sha256(random_pet.this.id)}" \
        --set hub.config.LDAPAuthenticator.server_address="${var.aws_ds_ip}" \
        --set-string "hub.config.LDAPAuthenticator.allowed_groups[0]=CN=${local.app_id}\,OU=Users\,OU=CORP\,DC=corp\,DC=codingworkshop\,DC=net" \
        --set-string "hub.config.LDAPAuthenticator.allowed_users[0]=${local.jupyterhub_allowed_user}" \
        --namespace default --values ./helm/config.yaml --values "$WORK/notebooks.json"
    EOT
  }

  # Notebook servers use this service account and mount these files.
  depends_on = [null_resource.jupyter_talent_files, aws_eks_pod_identity_association.jupyter_reader]
}

resource "null_resource" "helm_python_job" {
  for_each = data.aws_caller_identity.this.id != "000000000000" && var.aws_eks_enabled ? local.data_names_python : {}

  triggers = {
    cluster_id  = one(aws_eks_node_group.this.*.id)
    source_hash = timestamp()
  }

  provisioner "local-exec" {
    # JOB_VALUES carries the job's environment (including the database password) as a Helm
    # values file, so secrets never appear on a command line; the chart stores them in a Secret.
    environment = {
      JOB_VALUES = jsonencode({ pythonJob = { env = local.data_job_env, serviceAccount = local.talent_etl_service_account } })
    }
    command = <<-EOT
      set -e
      WORK=$(mktemp -d)
      trap 'rm -rf "$WORK"' EXIT
      printf '%s' "$JOB_VALUES" > "$WORK/values.json"
      # Zip every top-level Python package of the job (folders with __init__.py, except tests).
      python3 - "${each.value.path}" "$WORK/package.b64" <<'PY'
      import base64, io, os, sys, zipfile
      root, out = sys.argv[1], sys.argv[2]
      buffer = io.BytesIO()
      with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
          for package in sorted(os.listdir(root)):
              if package == "tests" or not os.path.isfile(os.path.join(root, package, "__init__.py")):
                  continue
              for folder, dirs, files in os.walk(os.path.join(root, package)):
                  dirs[:] = sorted(d for d in dirs if d != "__pycache__")
                  for name in sorted(f for f in files if f.endswith(".py")):
                      path = os.path.join(folder, name)
                      archive.write(path, os.path.relpath(path, root))
      open(out, "w").write(base64.b64encode(buffer.getvalue()).decode())
      PY
      helm upgrade --cleanup-on-fail \
        --install ${replace(each.key, "_", "-")}-python-job ./helm/ \
        --set pythonJob.enabled=true \
        --set pythonJob.name=${replace(each.key, "_", "-")} \
        --set-file pythonJob.scriptContent=${each.value.path}/${each.value.file} \
        --set-file pythonJob.requirementsContent=${each.value.path}/${each.value.reqs} \
        --set-file pythonJob.packageContent="$WORK/package.b64" \
        --values "$WORK/values.json" \
        --namespace default
    EOT
  }

  # The pod must start after the agent and association exist, or it gets no AWS credentials.
  depends_on = [
    null_resource.helm_chart,
    aws_eks_addon.pod_identity_agent,
    aws_iam_role_policy.talent_etl_s3,
    aws_eks_pod_identity_association.talent_etl,
  ]
}

# EKS Pod Identity: the talent-etl job gets its own IAM role, scoped to the two talent buckets.
# Pods cannot use the node role (the nodes allow one IMDSv2 hop), and the node role is far broader
# than the job needs. The agent add-on and the association must exist before the job's pod starts.
locals {
  talent_etl_enabled         = data.aws_caller_identity.this.id != "000000000000" && var.aws_eks_enabled
  talent_etl_service_account = "talent-etl"
}

resource "aws_eks_addon" "pod_identity_agent" {
  count        = local.talent_etl_enabled ? 1 : 0
  cluster_name = one(aws_eks_cluster.this.*.name)
  addon_name   = "eks-pod-identity-agent"
  tags         = local.app_tags

  depends_on = [aws_eks_node_group.this]
}

resource "aws_iam_role" "talent_etl" {
  count = local.talent_etl_enabled ? 1 : 0
  name  = format("%s-talent-etl-%s", var.aws_project, local.app_id)
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "pods.eks.amazonaws.com" }
        Action    = ["sts:AssumeRole", "sts:TagSession"]
      },
    ]
  })
  tags = local.app_tags
}

resource "aws_iam_role_policy" "talent_etl_s3" {
  count = local.talent_etl_enabled ? 1 : 0
  name  = "talent-etl-s3"
  role  = one(aws_iam_role.talent_etl.*.name)
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListTalentBuckets"
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [one(aws_s3_bucket.talent_source.*.arn), one(aws_s3_bucket.talent_lake.*.arn)]
      },
      {
        Sid      = "ReadWriteTalentObjects"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = ["${one(aws_s3_bucket.talent_source.*.arn)}/*", "${one(aws_s3_bucket.talent_lake.*.arn)}/*"]
      },
    ]
  })
}

resource "aws_eks_pod_identity_association" "talent_etl" {
  count           = local.talent_etl_enabled ? 1 : 0
  cluster_name    = one(aws_eks_cluster.this.*.name)
  namespace       = "default"
  service_account = local.talent_etl_service_account
  role_arn        = one(aws_iam_role.talent_etl.*.arn)
  tags            = local.app_tags
}

# JupyterHub notebook servers: read-only access to the talent data lake (nothing else).
resource "aws_iam_role" "jupyter_reader" {
  count = local.talent_etl_enabled ? 1 : 0
  name  = format("%s-jupyter-reader-%s", var.aws_project, local.app_id)
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "pods.eks.amazonaws.com" }
        Action    = ["sts:AssumeRole", "sts:TagSession"]
      },
    ]
  })
  tags = local.app_tags
}

resource "aws_iam_role_policy" "jupyter_reader_lake" {
  count = local.talent_etl_enabled ? 1 : 0
  name  = "talent-lake-read"
  role  = one(aws_iam_role.jupyter_reader.*.name)
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListLake"
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [one(aws_s3_bucket.talent_lake.*.arn)]
      },
      {
        Sid      = "ReadLake"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = ["${one(aws_s3_bucket.talent_lake.*.arn)}/*"]
      },
    ]
  })
}

resource "aws_eks_pod_identity_association" "jupyter_reader" {
  count           = local.talent_etl_enabled ? 1 : 0
  cluster_name    = one(aws_eks_cluster.this.*.name)
  namespace       = "default"
  service_account = local.jupyter_service_account
  role_arn        = one(aws_iam_role.jupyter_reader.*.arn)
  tags            = local.app_tags

  depends_on = [aws_eks_addon.pod_identity_agent, aws_iam_role_policy.jupyter_reader_lake]
}

# The notebook servers' service account, and the ConfigMap with the talent insights notebook,
# the talent_etl package, its requirements and the start-up script (built from data/talent-etl).
resource "null_resource" "jupyter_talent_files" {
  count = local.talent_etl_enabled ? 1 : 0

  triggers = {
    cluster_id = one(aws_eks_node_group.this.*.id)
    content = sha256(join("", concat(
      [for f in sort(fileset("${path.module}/../data/talent-etl", "talent_etl/**/*.py")) : filesha256("${path.module}/../data/talent-etl/${f}")],
      [filesha256("${path.module}/../data/talent-etl/notebooks/talent_insights.ipynb"),
        filesha256("${path.module}/../data/talent-etl/notebooks/explore_lake.ipynb"),
        filesha256("${path.module}/../data/talent-etl/requirements.txt"),
      filesha256("${path.module}/scripts/talent_notebook_files.py")],
    )))
  }

  provisioner "local-exec" {
    command = <<-EOT
      set -e
      WORK=$(mktemp -d)
      trap 'rm -rf "$WORK"' EXIT
      aws eks update-kubeconfig --region ${data.aws_region.this.region} --name ${one(aws_eks_cluster.this.*.name)} >/dev/null
      python3 ./scripts/talent_notebook_files.py ../data/talent-etl "$WORK"
      kubectl create serviceaccount ${local.jupyter_service_account} --namespace default --dry-run=client -o yaml | kubectl apply -f -
      kubectl create configmap talent-insights --namespace default --from-file="$WORK" --dry-run=client -o yaml | kubectl apply -f -
    EOT
  }

  depends_on = [aws_eks_node_group.this]
}
