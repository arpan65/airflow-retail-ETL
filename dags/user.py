from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import random
import pandas as pd
import os

@dag
def user_processing():
    
    # Step 1: Create the table
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS users (
            id INT PRIMARY KEY,
            firstname VARCHAR(255),
            lastname VARCHAR(255),
            email VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    # Step 2: Extract a user
    @task
    def extract_user():
        return {
            "id": random.randint(1, 1000000),
            "firstname": "Arpan",
            "lastname": "Das",
            "email": "arpan.das@gmail.com",
            "created_at": "2025-07-21 10:00:00"
        }

    # Step 3: Process the user
    @task
    def process_user(user):
        user["firstname"] += " processed"
        return user

    # Step 4: Store the user using SQLExecuteQueryOperator
    def store_user_sql(user):
        return f"""
        INSERT INTO users (id, firstname, lastname, email, created_at)
        VALUES ({user["id"]}, '{user["firstname"]}', '{user["lastname"]}', '{user["email"]}', '{user["created_at"]}')
        """

    def extract_user_sql():
        return """
        SELECT * FROM users
        """

    # Step 5: Define the task using .partial and .expand if needed
    @task
    def store_user(user):
        return store_user_sql(user)

    # Now build the flow
    user = extract_user()
    processed_user = process_user(user)
    final_sql = store_user(processed_user)

    insert_user = SQLExecuteQueryOperator(
        task_id="insert_user",
        conn_id="postgres",
        sql=final_sql,
    )
    select_user = SQLExecuteQueryOperator(
        task_id="select_user",
        conn_id="postgres",
        sql=extract_user_sql(),
    )

    create_table >> user >> processed_user >> final_sql >> insert_user >> select_user

# Register the DAG
user_processing_dag = user_processing()
