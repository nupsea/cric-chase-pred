from typing import Any, Type, Optional

from pydantic import BaseModel, ValidationError
import json


class DeliveryEvent(BaseModel):
    # Check the schema of the DeliveryEvent class in the schema/delivery_event.py file.
    inning: int
    team_name: str
    over_num: int
    batsman_name: str
    bowler: str
    non_striker: str
    extra_type: str
    extra_run: int
    total_run: int
    batsman_run: int
    dismissal_kind: str
    dismissed_player: str
    fielders_name: str
    match_id: str


class ModelInput(BaseModel):
    # Check the schema of the ChaseState class in the schema/delivery_event.py file.
    match_id: str
    inning: int
    season: str
    match_type: str
    batting_team: str
    toss_winner: str
    city: str
    target_runs: int
    current_score: int
    wickets_down: int
    balls_remaining: int
    runs_last_12_balls: int
    wickets_last_12_balls: int


def parse_event(raw_event: str, schema: Type[BaseModel]) -> Optional[dict[str, Any]]:
    """
    Parse the raw JSON event string into a validated DeliveryEvent.
    Returns a dictionary with the correctly typed fields if successful,
    otherwise returns None.
    """
    try:
        data = json.loads(raw_event)
        # Validate and convert the data into a DeliveryEvent instance.
        event = schema.model_validate(data)
        # Return the model data as a dict.
        res = event.model_dump()
        return res
    except ValidationError as e:
        print(f"Validation error for event: {raw_event}\n{e}")
        return None
    except Exception as e:
        print(e)
        return None
