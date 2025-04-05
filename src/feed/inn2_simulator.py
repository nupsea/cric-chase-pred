from src.feed.simulator import produce

MATCH_ID = "38238"

# Delay between deliveries
MIN_DELAY = 1
MAX_DELAY = 3

if __name__ == "__main__":
    produce(
        match_id=MATCH_ID, innings="second", min_delay=MIN_DELAY, max_delay=MAX_DELAY
    )
