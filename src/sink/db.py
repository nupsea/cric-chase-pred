from typing import Optional

from pydantic import ValidationError
from pyflink.common import Row, Types
from pyflink.datastream import DataStream
from pyflink.table import StreamTableEnvironment

from src.schema.delivery_event import ModelInput, parse_event


class TableSink:
    def __init__(
        self, t_env: StreamTableEnvironment, url=None, table_name=None, driver=None
    ):
        self.t_env = t_env
        self.url = url or "jdbc:postgresql://localhost:5432/flinkdb"
        self.table_name = table_name or "match_state"
        self.driver = driver or "org.postgresql.Driver"

    def parse_event_with_model(self, raw_event: str) -> Optional[Row]:
        try:
            # Parse the raw JSON string into a ModelInput instance.
            event = parse_event(raw_event, ModelInput)
            # Create a Row from the instance. The order must match your desired table schema.
            return Row(
                event["match_id"],
                event["inning"],
                event["season"],
                event["match_type"],
                event["batting_team"],
                event["toss_winner"],
                event["city"],
                event["target_runs"],
                event["current_score"],
                event["wickets_down"],
                event["balls_remaining"],
                event["runs_last_12_balls"],
                event["wickets_last_12_balls"],
            )
        except ValidationError as e:
            print("Validation error:", e)
            return None

    def write_to_sink(self, stream: DataStream):
        """
        Write to the sink
        :return:
        """
        self.t_env.execute_sql(f"""
            CREATE TABLE IF NOT EXISTS match_state (
                match_id STRING,
                inning INT,
                season STRING,
                match_type STRING,
                batting_team STRING,
                toss_winner STRING,
                city STRING,
                target_runs INT,
                current_score INT,
                wickets_down INT,
                balls_remaining INT,
                runs_last_12_balls INT,
                wickets_last_12_balls INT
            ) WITH (
                'connector' = 'jdbc',
                'url' = '{self.url}',
                'table-name' = '{self.table_name}',
                'driver' = '{self.driver}',
                'username' = 'flink',
                'password' = 'flink'
            )

        """)

        parsed_stream = stream.map(
            self.parse_event_with_model,
            output_type=Types.ROW(
                [
                    Types.STRING(),  # match_id
                    Types.INT(),  # inning
                    Types.STRING(),  # season
                    Types.STRING(),  # match_type
                    Types.STRING(),  # batting_team
                    Types.STRING(),  # toss_winner
                    Types.STRING(),  # city
                    Types.INT(),  # target_runs
                    Types.INT(),  # current_score
                    Types.INT(),  # wickets_down
                    Types.INT(),  # balls_remaining
                    Types.INT(),  # runs_last_12_balls
                    Types.INT(),  # wickets_last_12_balls
                ]
            ),
        )

        parsed_stream = parsed_stream.filter(lambda x: x is not None)

        # Convert the DataStream into a Table with an explicit schema.
        table = self.t_env.from_data_stream(parsed_stream)

        table_result = self.t_env.execute_sql(f"""
            INSERT INTO match_state
            SELECT match_id, inning, season, match_type, batting_team, toss_winner, city, target_runs, current_score, wickets_down, balls_remaining, runs_last_12_balls, wickets_last_12_balls
            FROM {table}
        """)

        table_result.wait()
