from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.jdbc import (
    JdbcExecutionOptions,
    JdbcConnectionOptions,
    JdbcSink,
)

env = StreamExecutionEnvironment.get_execution_environment()
env.add_jars(
    "file:///Users/sethurama/DEV/LM/cric-chase-pred/jars/flink-connector-jdbc-3.2.0-1.19.jar",
    "file:///Users/sethurama/DEV/LM/cric-chase-pred/jars/flink-connector-jdbc-3.2.0-1.19.jar",
)
type_info = Types.ROW([Types.INT(), Types.STRING(), Types.STRING(), Types.INT()])
env.from_collection(
    [
        (
            101,
            "Stream Processing with Apache Flink",
            "Fabian Hueske, Vasiliki Kalavri",
            2019,
        ),
        (102, "Streaming Systems", "Tyler Akidau, Slava Chernyak, Reuven Lax", 2018),
        (103, "Designing Data-Intensive Applications", "Martin Kleppmann", 2017),
        (
            104,
            "Kafka: The Definitive Guide",
            "Gwen Shapira, Neha Narkhede, Todd Palino",
            2017,
        ),
    ],
    type_info=type_info,
).add_sink(
    JdbcSink.sink(
        "insert into books (id, title, authors, year) values (?, ?, ?, ?)",
        type_info,
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .with_url("jdbc:postgresql://dbhost:5432/flinkdb")
        .with_driver_name("org.postgresql.Driver")
        .with_user_name("flink")
        .with_password("flink")
        .build(),
        JdbcExecutionOptions.builder()
        .with_batch_interval_ms(1000)
        .with_batch_size(200)
        .with_max_retries(5)
        .build(),
    )
)

env.execute()
