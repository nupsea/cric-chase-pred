import logging
from pyflink.java_gateway import get_gateway
from pyflink.datastream.functions import SinkFunction
import redis
import json


class RedisSink(SinkFunction):
    def __init__(self, redis_host="localhost", redis_port=6379):
        # Get the Java gateway.
        gateway = get_gateway()
        # Create a dummy Java sink function using DiscardingSink.
        dummy_java_sink = (
            gateway.jvm.org.apache.flink.streaming.api.functions.sink.DiscardingSink()
        )
        # Pass the dummy sink to the parent constructor.
        super().__init__(dummy_java_sink)
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.client = None
        # Set up logging
        logging.basicConfig(level=logging.DEBUG)
        self.logger = logging.getLogger("RedisSink")

    def open(self, runtime_context):
        self.logger.debug("Opening Redis connection...")
        self.client = redis.Redis(host=self.redis_host, port=self.redis_port, db=0)
        self.logger.debug(f"Connected to Redis at {self.redis_host}:{self.redis_port}")

    def invoke(self, value, context):
        self.logger.debug(f"Debug to RedisSink: {value}")
        try:
            data = json.loads(value)
            match_id = data["match_id"]
            self.logger.debug(f"Writing to Redis: {match_id}")
            self.client.set(match_id, value)
            self.logger.debug(f"Successfully wrote to Redis: {match_id}")
        except Exception as e:
            self.logger.error(f"Error writing to Redis: {e}")
