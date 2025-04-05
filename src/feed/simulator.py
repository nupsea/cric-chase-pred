import random
import time
import json

from confluent_kafka import Producer

from src.util.file_util import get_records_for_match

KAFKA_TOPIC = "t20-deliveries"
KAFKA_BROKER = "localhost:9092"


def delivery_report(err, msg):
    """Delivery callback for produced messages."""
    if err is not None:
        print(f"Delivery failed for record {msg.value()}: {err}")
    else:
        print(
            f"Record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
        )


def produce(match_id, innings, min_delay=1, max_delay=3):
    conf = {"bootstrap.servers": KAFKA_BROKER}
    producer = Producer(**conf)
    data_file_path = f"/Users/sethurama/DEV/LM/cric-chase-pred/data/match_{match_id}/{innings}_innings.csv"

    # start of match clear state
    event_value = {
        "inning": 0,
        "team_name": "",
        "over_num": 0,
        "batsman_name": "",
        "bowler": "",
        "non_striker": "",
        "extra_type": "match_start",
        "extra_run": 0,
        "total_run": 0,
        "batsman_run": 0,
        "dismissal_kind": "",
        "dismissed_player": "",
        "fielders_name": "",
        "match_id": match_id,
    }
    producer.produce(
        KAFKA_TOPIC,
        key=str(event_value["match_id"]),
        value=json.dumps(event_value),
        callback=delivery_report,
    )
    producer.flush(0)
    time.sleep(3)

    match_rows = get_records_for_match(data_file_path, match_id)
    for row in match_rows:
        # Convert to JSON
        # CSV schema:
        # inning,team_name,over_num,batsman_name,bowler,non_striker,extra_type,extra_run,
        # total_run,batsman_run,dismissal_kind,dismissed_player,fielders_name,mergeid

        event_value = {
            "inning": row["inning"],
            "team_name": row["team_name"],
            "over_num": row["over_num"],
            "batsman_name": row["batsman_name"],
            "bowler": row["bowler"],
            "non_striker": row["non_striker"],
            "extra_type": row["extra_type"],
            "extra_run": row["extra_run"],
            "total_run": row["total_run"],
            "batsman_run": row["batsman_run"],
            "dismissal_kind": row["dismissal_kind"],
            "dismissed_player": row["dismissed_player"],
            "fielders_name": row["fielders_name"],
            "match_id": row["mergeid"],
        }

        producer.produce(
            KAFKA_TOPIC,
            key=str(event_value["match_id"]),
            value=json.dumps(event_value),
            callback=delivery_report,
        )
        producer.flush(0)

        if row["inning"] == "2":
            time.sleep(random.randint(min_delay, max_delay))

    # Wait for any outstanding messages to be delivered and delivery report
    producer.flush()

    print(" ** All match events delivered!")
