from src.feed import MATCH_ID
from src.feed.simulator import produce

# Delay between deliveries
MIN_DELAY = 1
MAX_DELAY = 3

if __name__ == "__main__":
    produce(
        match_id=MATCH_ID, innings="second", min_delay=MIN_DELAY, max_delay=MAX_DELAY
    )
