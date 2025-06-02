# airflow-etl
ETL with airflow (MWAA), Amazon S3 and Amazon RDS 



## Configuring an Apache Airflow connection using a AWS Secrets Manager secret [Documentation](https://docs.aws.amazon.com/mwaa/latest/userguide/connections-secrets-manager.html#connections-sm-policy)

### Add below key values in airflow mwaa configuration 
```
secrets.backend: airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend

secrets.backend_kwargs: {"connections_prefix" : "airflow/connections", "variables_prefix" : "airflow/variables"}

```

### Create secrets in secrets_manager 
```
Secret name: airflow/connections/postgres_default
```
```
value: 
{
    "conn_type": "postgresql",
    "host": "xxx.rds.amazonaws.com",
    "login": "<your login>",
    "password": "<your password>",
    "port": 5432,
    "schema": "<database name>"
}
```

```
Secret name: airflow/connections/aws_default
```
```
Value:
{
    "conn_type": "aws",
    "login": "<Access key>",
    "password": "<secret key>",
    "extra": "{\"region_name\": \"region\"}"
}
```

