# Configuring PostgreSQL Connection for Airflow in AWS MWAA using AWS Secrets Manager

Configuring a PostgreSQL connection for Apache Airflow in AWS MWAA using AWS Secrets Manager involves several key steps. This approach enhances security by centralizing credential management.

Here's a detailed guide:

---

## 1. Permissions for MWAA to Access Secrets Manager

Your MWAA environment's execution role needs permissions to read secrets from AWS Secrets Manager.

### Attach a policy to your MWAA execution role:

* Go to the MWAA console, select your environment, and note down the "Execution role ARN" from the "Permissions" pane.
* Navigate to IAM, find the role by its ARN, and attach a policy that grants `secretsmanager:GetSecretValue` permission. A common managed policy is `SecretsManagerReadWrite`, but for production, you should create a custom policy that only allows access to the specific secrets your MWAA environment needs.

### Example IAM Policy (least privilege):

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "secretsmanager:GetSecretValue",
            "Resource": "arn:aws:secretsmanager:<REGION>:<ACCOUNT_ID>:secret:airflow/connections/<YOUR_POSTGRES_CONN_ID>-*"
        }
    ]
}
```

---

## 2. Configure MWAA to Use Secrets Manager as a Backend

You need to tell Airflow within your MWAA environment to look for connections in Secrets Manager.

### Via MWAA Console:

1.  Open the Amazon MWAA console and select your environment.
2.  Choose **Edit**.
3.  Go to the **Airflow configuration options** section and select **Add custom configuration**.
4.  Add the following key-value pairs:
    * **Key:** `secrets.backend`
        **Value:** `airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend`
    * **Key:** `secrets.backend_kwargs`
        **Value:** `{"connections_prefix": "airflow/connections", "variables_prefix": "airflow/variables"}`

This configuration tells Airflow to look for connection strings at paths like `airflow/connections/*` and variables at `airflow/variables/*` in Secrets Manager. You can customize these prefixes. For improved cost efficiency with Secrets Manager API calls, you can also specify `connections_lookup_pattern` and `variables_lookup_pattern` using a regular expression. For example:

`{"connections_prefix": "airflow/connections", "connections_lookup_pattern": "^your_postgres_conn_id", "variables_prefix": "airflow/variables"}`

---
## 3. Store PostgreSQL Connection in AWS Secrets Manager

You'll store the PostgreSQL connection details as a secret in AWS Secrets Manager. Airflow can consume this in two formats: a Connection URI or a JSON object. The JSON format is generally more readable and flexible.

### Recommended: Storing as a JSON Object

1.  **Go to AWS Secrets Manager console:**
    * Choose **Store a new secret**.
    * For **Secret type**, choose **Other type of secret**.
    * In the **Key/value pairs** section, define your PostgreSQL connection details. Use standard Airflow connection field names or allowed aliases.

2.  **Here's an example for PostgreSQL:**

    ```json
    {
        "conn_type": "postgresql",
        "host": "<YOUR_POSTGRES_HOST>",
        "login": "<YOUR_POSTGRES_USERNAME>",
        "password": "<YOUR_POSTGRES_PASSWORD>",
        "port": 5432,
        "schema": "<YOUR_POSTGRES_DATABASE_NAME>",
        "extra": "{\"sslmode\": \"require\"}"  // Optional: Add extra parameters as a JSON string
    }
    ```
    * `conn_type`: `postgresql`
    * `host`: The endpoint of your PostgreSQL database (e.g., from RDS).
    * `login`: Your PostgreSQL username.
    * `password`: Your PostgreSQL password.
    * `port`: PostgreSQL's default port is `5432`.
    * `schema`: The specific database name.
    * `extra`: A JSON string for any additional connection parameters, like SSL settings. For example, `{"sslmode": "require"}` or `{"sslmode": "prefer", "keepalives_idle": "300"}`.

3.  **Configure Secret Name:**
    * For **Secret name**, use the format `airflow/connections/<YOUR_AIRFLOW_CONN_ID>`. The `connections_prefix` you configured in MWAA (e.g., `airflow/connections`) combined with your desired Airflow `conn_id` (e.g., `my_postgres_db`) will form the full secret name: `airflow/connections/my_postgres_db`.
    * Add a description for easy identification.
    * Proceed to store the secret.

---