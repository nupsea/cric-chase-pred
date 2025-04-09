from pyflink.common import Types, Time, Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema


from pyflink.datastream.state import MapStateDescriptor
from pyflink.datastream.window import TumblingProcessingTimeWindows

from src.process.event_transformer import ComputeChaseState
from src.process.first_innings import ComputeFirstInnings
from src.schema.delivery_event import parse_event, DeliveryEvent


def main():
    config = Configuration()
    config.set_string("execution.checkpointing.interval", "60000")
    config.set_string(
        "state.checkpoints.dir",
        "file:///Users/sethurama/DEV/LM/cric-chase-pred/checkpoints/c3",
    )
    config.set_string("parallelism.default", "2")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(
        "file:///Users/sethurama/DEV/LM/cric-chase-pred/jars/flink-sql-connector-kafka-3.2.0-1.19.jar",
        "file:///Users/sethurama/DEV/LM/cric-chase-pred/jars/flink-sql-connector-kafka-3.2.0-1.19.jar",
    )
    env.enable_checkpointing(60000)  # enable checkpointing every 60 seconds
    env.configure(config)

    kafka_consumer = FlinkKafkaConsumer(
        topics="t20-deliveries",
        deserialization_schema=SimpleStringSchema(),
        properties={
            "bootstrap.servers": "localhost:9094",
            "group.id": "t20-deliveries-consumer",
            "security.protocol": "plaintext",
            "client.id": "my-python-client",
            "auto.offset.reset": "latest",
        },
    )

    stream = env.add_source(kafka_consumer)

    parsed_stream = stream.map(lambda raw: parse_event(raw, DeliveryEvent)).filter(
        lambda x: x is not None
    )
    # parsed_stream.print("parsed_stream")

    first_innings_stream = parsed_stream.filter(lambda x: x["inning"] == 1)
    # first_innings_stream.print("first_innings_stream")

    second_innings_stream = parsed_stream.filter(lambda x: x["inning"] == 2)
    # second_innings_stream.print("second_innings_stream")

    processed_first_innings_stream = (
        first_innings_stream.key_by(lambda x: x["match_id"])
        # .window(TumblingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(0)))
        .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
        .process(
            ComputeFirstInnings(),
            output_type=Types.TUPLE([Types.STRING(), Types.INT()]),
        )
    )
    processed_first_innings_stream.print("processed_first_innings_stream")

    target_state_desc = MapStateDescriptor(
        "first_innings_target", Types.STRING(), Types.INT()
    )
    broadcast_first_innings = processed_first_innings_stream.broadcast(
        target_state_desc
    )

    enriched_stream = (
        second_innings_stream.key_by(lambda x: x["match_id"])
        .connect(broadcast_first_innings)
        .process(
            ComputeChaseState(),
            output_type=Types.STRING(),
        )
    )

    # settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    # t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    #
    # sink = TableSink(t_env)
    # sink.write_to_sink(enriched_stream)

    # Write to redis
    # enriched_stream.add_sink(RedisSink())

    enriched_stream.print("enriched_stream")

    kafka_producer = FlinkKafkaProducer(
        topic="t20-model-input",
        serialization_schema=SimpleStringSchema(),
        producer_config={
            "bootstrap.servers": "localhost:9094",
        },
    )

    enriched_stream.add_sink(kafka_producer)

    env.execute("T20 Match Chase")


if __name__ == "__main__":
    main()
