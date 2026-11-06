# Polaris deployment lab

Deployed Apache Polaris 1.8.0 and PostgreSQL using Helm.

## Validation
- Helm release: polaris, status deployed, revision 1.
- Polaris and PostgreSQL pods: Running and Ready.
- Bootstrap job: Completed.
- Polaris readiness endpoint: UP.
- Credentials for dev and analyst saved outside the Git repository.

## Configuration
- Namespace and catalog: user-l-deduit-ece.
- Warehouse: s3://user-l-deduit-ece/warehouse/
- dev: data_engineer role with catalog_contributor access.
- analyst: data_analyst role with catalog_reader access.
- S3 credentials stored in a Kubernetes Secret.
- Root uses the demonstration credential provided in the lab.

Polaris is kept running for the next lab.
