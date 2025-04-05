import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import SinkFunction
import redis
import json


class RedisSink(SinkFunction):
    def __init__(self, redis_host="localhost", redis_port=6379):
        # gateway = get_gateway()
        # dummy_java_sink = gateway.jvm.org.apache.flink.streaming.api.functions.sink.DiscardingSink()
        # super().__init__(dummy_java_sink)
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.client = None
        logging.basicConfig(level=logging.DEBUG)
        self.logger = logging.getLogger("RedisSink")

    def open(self, runtime_context):
        self.logger.debug("Opening Redis connection...")
        print("Opening Redis connection...")
        self.client = redis.Redis(host=self.redis_host, port=self.redis_port, db=0)
        self.logger.debug(f"Connected to Redis at {self.redis_host}:{self.redis_port}")
        print(f"Connected to Redis at {self.redis_host}:{self.redis_port}")

    def invoke(self, value, context):
        self.logger.debug(f"Debug to RedisSink: {value}")
        print(f"Debug to RedisSink: {value}")
        try:
            data = json.loads(value)
            match_id = data["match_id"]
            self.logger.debug(f"Writing to Redis: {match_id}")
            print(f"Writing to Redis: {match_id}")
            self.client.set(match_id, value)
            self.logger.debug(f"Successfully wrote to Redis: {match_id}")
            print(f"Successfully wrote to Redis: {match_id}")
        except Exception as e:
            self.logger.error(f"Error writing to Redis: {e}")
            print(f"Error writing to Redis: {e}")


# Set up the Flink environment
env = StreamExecutionEnvironment.get_execution_environment()

# Example data stream
# Convert dictionaries to JSON strings
json_stream = env.from_collection(
    [
        json.dumps({"match_id": "1", "total_run": 100, "is_wicket": 0}),
        json.dumps({"match_id": "2", "total_run": 150, "is_wicket": 1}),
    ]
)
json_stream.add_sink(RedisSink())
json_stream.print("Json Stream")


# Execute the Flink job
env.execute("Flink Redis Sink Job")
