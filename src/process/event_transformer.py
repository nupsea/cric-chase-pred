import json
from abc import ABC

from pyflink.common import Types
from pyflink.datastream import KeyedBroadcastProcessFunction

from pyflink.datastream.functions import RuntimeContext, IN2
from pyflink.datastream.state import (
    ValueStateDescriptor,
    ListStateDescriptor,
    MapStateDescriptor,
)
from pyflink.fn_execution.datastream.window.window_operator import Context

from src.schema.delivery_event import ModelInput, parse_event

target_state_desc = MapStateDescriptor(
    "first_innings_target", Types.STRING(), Types.INT()
)


class ComputeChaseState(KeyedBroadcastProcessFunction, ABC):
    def __init__(self, total_balls=120):
        self.last_12_state = None
        self.ball_count_state = None
        self.wickets_state = None
        self.score_state = None
        self.total_balls = total_balls

    def open(self, runtime_context: RuntimeContext):
        self.score_state = runtime_context.get_state(
            ValueStateDescriptor("score", Types.INT())
        )
        self.wickets_state = runtime_context.get_state(
            ValueStateDescriptor("wickets", Types.INT())
        )
        self.ball_count_state = runtime_context.get_state(
            ValueStateDescriptor("ball_count", Types.INT())
        )

        self.last_12_state = runtime_context.get_list_state(
            ListStateDescriptor("last_12", Types.PICKLED_BYTE_ARRAY())
        )

        # For testing purposes, clear the state.
        self.clear_state()

    def process_element(self, value, ctx):
        match_id = str(value["match_id"])
        inning = value["inning"]
        broadcast_state = ctx.get_broadcast_state(target_state_desc)
        target_score = broadcast_state.get(match_id)

        if target_score is None:
            target_score = 200  # default target score

        current_score = self.score_state.value() or 0
        wickets_down = self.wickets_state.value() or 0
        ball_count = self.ball_count_state.value() or 0

        run_this_ball = int(value.get("total_run", 0))
        is_wicket = 1 if value.get("dismissal_kind", None) else 0

        # Update cumulative stats
        current_score += run_this_ball
        wickets_down += is_wicket
        if (value.get("extra_type", None) == "noballs") or (
            value.get("extra_type", None) == "wides"
        ):
            # No ball or wide does not count as a ball
            pass
        else:
            ball_count += 1

        self.score_state.update(current_score)
        self.wickets_state.update(wickets_down)
        self.ball_count_state.update(ball_count)

        # Update last 12 balls
        deliveries = list(self.last_12_state.get())
        deliveries.append({"run_this_ball": run_this_ball, "is_wicket": is_wicket})
        if len(deliveries) > 12:
            deliveries.pop(0)

        self.last_12_state.update(deliveries)

        # Compute the chase state
        runs_last_12_balls = sum([d["run_this_ball"] for d in deliveries])
        wickets_last_12_balls = sum([d["is_wicket"] for d in deliveries])
        balls_remaining = self.total_balls - ball_count

        chase_state = {
            "match_id": match_id,
            "inning": inning,
            "season": "",
            "match_type": "",
            "batting_team": "",
            "toss_winner": "",
            "city": "",
            "target_runs": int(target_score),
            "current_score": int(current_score),
            "wickets_down": int(wickets_down),
            "balls_remaining": int(balls_remaining),
            "runs_last_12_balls": int(runs_last_12_balls),
            "wickets_last_12_balls": int(wickets_last_12_balls),
        }

        # Convert chase_state to JSON string
        chase_state_json = json.dumps(chase_state)

        # Parse and validate the chase_state JSON string
        parsed_chase_state = parse_event(chase_state_json, ModelInput)

        if parsed_chase_state is not None:
            yield json.dumps(parsed_chase_state)
        else:
            print("Validation error for chase_state")

    def process_broadcast_element(self, value: IN2, ctx: Context):
        match_id = value[0]  # match_id
        target_score = value[1]  # target_score
        ctx.get_broadcast_state(target_state_desc).put(match_id, target_score)

    def clear_state(self):
        self.score_state.clear()
        self.wickets_state.clear()
        self.ball_count_state.clear()
        self.last_12_state.clear()
