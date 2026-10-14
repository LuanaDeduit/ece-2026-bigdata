# Lab 2 — Object storage with S3

## 1. Why use a Secret instead of a ConfigMap?
A Secret is designed for sensitive information such as credentials.
A ConfigMap stores non-sensitive configuration.
Secrets still require appropriate access controls; base64 encoding is not encryption.

## 2. What happens if the Job runs again tomorrow?
The temporary credentials may have expired, so authentication can fail.
In production, the Job should obtain short-lived credentials through
a workload identity or an IAM role, rather than reuse a static Secret.

## 3. How can this Job become a daily ingestion?
Use a Kubernetes CronJob with the schedule "0 0 * * *".
It runs daily at midnight in its configured time zone.
Each run needs valid credentials and up-to-date source data.

## Result
The Kubernetes Job uploaded users.csv and orders.csv to bronze/.
A CPU limit of 500m was added to satisfy the Onyxia quota.
Temporary Kubernetes resources were deleted after completion.
The datasets remain in S3 for the next labs.
