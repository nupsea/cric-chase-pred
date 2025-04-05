from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def main():
    # 1) Create a streaming Table Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(
        "file:///Users/sethurama/DEV/LM/cric-chase-pred/jars/flink-connector-jdbc-3.2.0-1.19.jar",
        "file:///Users/sethurama/DEV/LM/cric-chase-pred/jars/postgresql-42.2.18.jar",
    )
    # env_settings = EnvironmentSettings.in_streaming_mode()
    # t_env = TableEnvironment.create(env_settings)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # 2) Create a mock source using 'datagen', rename 'year' -> 'year_col'
    t_env.execute_sql("""
        CREATE TABLE source_books (
            id INT,
            title STRING,
            authors STRING,
            year_col INT
        ) WITH (
            'connector' = 'datagen',
            'rows-per-second' = '5',
            'fields.id.min' = '100',
            'fields.id.max' = '999',
            'fields.title.length' = '30',
            'fields.authors.length' = '20',
            'fields.year_col.min' = '2000',
            'fields.year_col.max' = '2023'
        )
    """)

    # 3) Create the sink table, also rename 'year' -> 'year_col'
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS sink_books (
            id INT,
            title STRING,
            authors STRING,
            year_col INT
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://localhost:5432/flinkdb',
            'table-name' = 'sink_books',
            'driver' = 'org.postgresql.Driver',
            'username' = 'flink',
            'password' = 'flink'
        )
    """)

    # 4) Insert statement
    table_result = t_env.execute_sql("""
        INSERT INTO sink_books
        SELECT id, title, authors, year_col
        FROM source_books
    """)

    # 5) Execute streaming job
    table_result.wait()


if __name__ == "__main__":
    main()
