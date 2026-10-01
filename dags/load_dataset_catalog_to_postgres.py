from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import yaml

with DAG(
    dag_id="load_dataset_catalog_to_postgres",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    def read_csv(**context):
        df = pd.read_csv("/opt/airflow/dags/dataset_catalog.csv")
        df.columns = df.columns.str.replace(" ", "_").str.lower()
        context['ti'].xcom_push(key="raw_data", value=df.to_json())

    def validate_schema(**context):
        json_data = context["ti"].xcom_pull(key="raw_data", task_ids="read_csv_task")
        df = pd.read_json(json_data)

        with open("/opt/airflow/dags/schema.yaml") as f:
            schema = yaml.safe_load(f)

        for col in schema["columns"]:
            if col["name"] not in df.columns:
                raise ValueError(f"Missing column: {col['name']}")
            if not col["nullable"] and df[col["name"]].isnull().any():
                raise ValueError(f"Null found in required column: {col['name']}")

        context["ti"].xcom_push(key="validated_data", value=df.to_json())

    def transform(**context):
        json_data = context["ti"].xcom_pull(key="validated_data", task_ids="validate_schema_task")
        df = pd.read_json(json_data)

        # 1. Drop fully-empty rows (only dataset_code present, rest all null)
        subset_cols = [c for c in df.columns if c != "dataset_code"]
        df = df.dropna(subset=subset_cols, how="all")

        # 2. Remove duplicate links (keep first occurrence)
        df = df.drop_duplicates(subset="link", keep="first")

        # 3. Standardize inconsistent status values
        status_map = {"Done": "Completed", "done": "Completed", "pending": "Pending"}
        df["analysis_status"] = df["analysis_status"].replace(status_map)
        df["analysis_status2"] = df["analysis_status2"].replace(status_map)

        context["ti"].xcom_push(key="clean_data", value=df.to_json())

    def load_to_postgres(**context):
        json_data = context["ti"].xcom_pull(key="clean_data", task_ids="transform_task")
        df = pd.read_json(json_data)

        hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = hook.get_conn()
        cursor = conn.cursor()

        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO dataset_catalog (
                    dataset_code, dataset_name, link, description, comments,
                    author, is_conversation_like, ai_generated, comments_on_relevance,
                    analysis_author, analysis_author2, analysis_status, analysis_status2
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (dataset_code) DO UPDATE SET
                    dataset_name = EXCLUDED.dataset_name,
                    link = EXCLUDED.link,
                    description = EXCLUDED.description,
                    comments = EXCLUDED.comments,
                    author = EXCLUDED.author,
                    is_conversation_like = EXCLUDED.is_conversation_like,
                    ai_generated = EXCLUDED.ai_generated,
                    comments_on_relevance = EXCLUDED.comments_on_relevance,
                    analysis_author = EXCLUDED.analysis_author,
                    analysis_author2 = EXCLUDED.analysis_author2,
                    analysis_status = EXCLUDED.analysis_status,
                    analysis_status2 = EXCLUDED.analysis_status2;
            """, (
                row["dataset_code"], row["dataset_name"], row["link"], row["description"], row["comments"],
                row["author"], row["is_conversation_like"], row["ai_generated"], row["comments_on_relevance"],
                row["analysis_author"], row["analysis_author2"], row["analysis_status"], row["analysis_status2"]
            ))

        conn.commit()
        cursor.close()
        conn.close()

    read_csv_task = PythonOperator(
        task_id="read_csv_task",
        python_callable=read_csv,
    )

    validate_schema_task = PythonOperator(
        task_id="validate_schema_task",
        python_callable=validate_schema,
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform,
    )

    load_to_postgres_task = PythonOperator(
        task_id="load_to_postgres_task",
        python_callable=load_to_postgres,
    )

    read_csv_task >> validate_schema_task >> transform_task >> load_to_postgres_task