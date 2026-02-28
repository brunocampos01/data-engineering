"""
Databricks remote connection without Personal Access Tokens (PAT).

When tokens are disabled (Settings → Developer → Access tokens),
use one of the strategies below.

Auth method summary
-------------------
1. OAuth M2M  — Databricks service principal + client secret. Best for pipelines / CI.
2. OAuth U2M  — Browser-based login. Best for local / interactive development.
3. AWS IAM    — Reuses AWS credentials (env vars, ~/.aws/credentials, or instance profile).
4. Config file — `databricks configure --host <host>` stores credentials in
                 ~/.databrickscfg and is picked up automatically.
"""

import os

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient


# ------------------------------------------------------------------
# 1. OAuth Machine-to-Machine (M2M) — Databricks Service Principal
#
#    Required env vars:
#      DATABRICKS_HOST          — https://<workspace-id>.cloud.databricks.com
#      DATABRICKS_CLIENT_ID     — Service principal client ID
#      DATABRICKS_CLIENT_SECRET — Service principal client secret
#
#    Setup:
#      1. In the Databricks account console, create a service principal
#      2. Assign it to the workspace with the required entitlements
#      3. Generate a client secret for it (OAuth M2M credentials)
# ------------------------------------------------------------------
def connect_oauth_m2m() -> DatabricksSession:
    w = WorkspaceClient(
        host=os.getenv('DATABRICKS_HOST'),
        client_id=os.getenv('DATABRICKS_CLIENT_ID'),
        client_secret=os.getenv('DATABRICKS_CLIENT_SECRET'),
    )
    return DatabricksSession.builder.sdkConfig(w.config).getOrCreate()


# ------------------------------------------------------------------
# 2. OAuth User-to-Machine (U2M) — Interactive browser login
#
#    Required env vars:
#      DATABRICKS_HOST  — https://<workspace-id>.cloud.databricks.com
#
#    Opens a browser window on first run; subsequent runs reuse the
#    cached token (~/.databricks/token-cache.json).
#
#    Or run once via CLI:
#      databricks auth login --host https://<workspace-id>.cloud.databricks.com
# ------------------------------------------------------------------
def connect_oauth_u2m() -> DatabricksSession:
    w = WorkspaceClient(
        host=os.getenv('DATABRICKS_HOST'),
        auth_type='databricks-oauth',
    )
    return DatabricksSession.builder.sdkConfig(w.config).getOrCreate()


# ------------------------------------------------------------------
# 3. AWS IAM — reuses existing AWS credentials
#
#    No extra Databricks-specific credentials needed. The SDK resolves
#    credentials in this order:
#      a) Environment variables  — AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY
#                                   (+ AWS_SESSION_TOKEN for assumed roles)
#      b) Shared credentials file — ~/.aws/credentials  (aws configure)
#      c) Instance / task profile — EC2 instance profile or ECS task role
#
#    Required env vars:
#      DATABRICKS_HOST  — https://<workspace-id>.cloud.databricks.com
# ------------------------------------------------------------------
def connect_aws_iam() -> DatabricksSession:
    w = WorkspaceClient(
        host=os.getenv('DATABRICKS_HOST'),
        auth_type='aws-iam',
    )
    return DatabricksSession.builder.sdkConfig(w.config).getOrCreate()


# ------------------------------------------------------------------
# 4. Config file (~/.databrickscfg) — zero env vars needed at runtime
#
#    One-time setup:
#      databricks configure --host https://<workspace-id>.cloud.databricks.com
#    Then choose the auth type prompted (OAuth, AWS IAM, etc.)
#
#    To use a non-default profile:
#      DatabricksSession.builder.profile('my-profile').getOrCreate()
# ------------------------------------------------------------------
def connect_config_file(profile: str = 'DEFAULT') -> DatabricksSession:
    return DatabricksSession.builder.profile(profile).getOrCreate()


# ------------------------------------------------------------------
# Example — choose the method that matches your setup
# ------------------------------------------------------------------
if __name__ == '__main__':
    # Pick one:
    spark = connect_oauth_m2m()      # service principal (CI/CD)
    # spark = connect_oauth_u2m()    # browser login (interactive)
    # spark = connect_aws_iam()      # reuse AWS credentials
    # spark = connect_config_file()  # ~/.databrickscfg

    df = spark.read.table('dev_silver.bloomberg.ld_currency')
    df.show(5)
