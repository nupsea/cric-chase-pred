from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer

from src.process.first_innings import ComputeFirstInnings
from src.schema.delivery_event import parse_event, DeliveryEvent
import io
import csv
import json

from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.common import Types, Time, Configuration


def delivery_source(env, batch=False):
    # Set up Kafka consumer for first innings events.
    # Ensure your Kafka broker and topic are correct.
    if batch:
        csv_file_path = (
            "/Users/sethurama/DEV/LM/cric-chase-pred/data/Every_ball_data.csv"
        )
        with open(csv_file_path, "r") as f:
            header_line = f.readline()
            print(f"Header: {header_line}")
        columns = header_line.strip().split(",")
        columns[-1] = "match_id"
        print(f"Columns: {columns}")

        # Read the CSV file as a text stream
        csv_stream = env.read_text_file(csv_file_path)

        data_stream = csv_stream.filter(lambda line: line != header_line)
        # data_stream.print("data_stream")

        def to_json(line: str) -> str:
            # Use csv.reader to correctly parse the CSV line
            reader = csv.reader(io.StringIO(line))
            values = next(reader)
            return json.dumps(dict(zip(columns, values)))

        json_stream = data_stream.map(to_json, output_type=Types.STRING())
        # json_stream.print("json_stream")
        stream = json_stream.filter(
            lambda x: json.loads(x)["match_id"].strip().isdigit()
            and int(json.loads(x)["match_id"].strip()) == 31231
        )

    else:
        kafka_props = {
            "bootstrap.servers": "localhost:9094",
            "group.id": "first-innings-group",
        }
        kafka_consumer = FlinkKafkaConsumer(
            topics="t20-deliveries",
            deserialization_schema=SimpleStringSchema(),
            properties=kafka_props,
        )
        # Add Kafka as a source
        stream = env.add_source(kafka_consumer)

    return stream


def main():
    """
    This function demonstrates how to use Flink's DataStream API to process a stream of cricket delivery events
    in a batch mode. The function reads a CSV file containing cricket delivery events and processes the first innings
    :return:
    """
    config = Configuration()
    config.set_string("execution.checkpointing.interval", "60000")
    config.set_string(
        "state.checkpoints.dir",
        "file:///Users/sethurama/DEV/LM/cric-chase-pred/checkpoints/b1",
    )
    config.set_string("parallelism.default", "2")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(
        "file:///Users/sethurama/DEV/LM/cric-chase-pred/jars/flink-sql-connector-kafka-3.2.0-1.29.jar",
        "file:///Users/sethurama/DEV/LM/cric-chase-pred/jars/flink-sql-connector-kafka-3.2.0-1.19.jar",
    )
    # Set the runtime mode to BATCH for this example.
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)

    env.set_parallelism(1)

    stream = delivery_source(env, batch=True)

    # Parse incoming JSON messages
    parsed_stream = stream.map(lambda raw: parse_event(raw, DeliveryEvent)).filter(
        lambda x: x is not None
    )

    first_innings_stream = parsed_stream.filter(lambda x: x["inning"] == 1)
    first_innings_stream.print("first_innings_stream")
    # Key the stream by match_id so that state is maintained per match.
    keyed_stream = first_innings_stream.key_by(lambda event: event["match_id"])

    # Apply a tumbling processing time window of 5 minutes.
    # This aggregates the events received in each 5-minute period.
    aggregated_stream = keyed_stream.window(
        TumblingProcessingTimeWindows.of(Time.seconds(60))
    ).process(
        ComputeFirstInnings(), output_type=Types.TUPLE([Types.STRING(), Types.INT()])
    )

    # For demonstration purposes, print the results.
    aggregated_stream.print("agg_stream")
    env.execute("First Innings Window Aggregation")


if __name__ == "__main__":
    main()
