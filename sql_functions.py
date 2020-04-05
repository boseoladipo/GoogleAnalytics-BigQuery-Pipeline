from sqlalchemy import text

def push_data_to_sql(df, db_conn, TABLE_NAME, SCHEMA_NAME, insert_method):
        """
            push cars data to mysql
        """
        df.to_sql(TABLE_NAME, db_conn, schema=SCHEMA_NAME, if_exists='append', index=False, method=insert_method
        )
        print(f"{len(df)} Records succesfully added to {TABLE_NAME}.")

def delete_from_sql(db_conn, TABLE_NAME, query):
        """
            delete rows from sql table
        """
        sql = text(query)
        db_conn.execute(sql.execution_options(autocommit=True))

        print(f"Records succesfully deleted from {TABLE_NAME}.")