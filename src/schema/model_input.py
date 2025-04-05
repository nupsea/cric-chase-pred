from pyflink.common import Types


model_input_schema = Types.ROW_NAMED(
    [
        "season",
        "match_type",
        "batting_team",
        "toss_winner",
        "city",
        "target_runs",
        "current_score",
        "wickets_down",
        "balls_remaining",
        "runs_last_12_balls",
        "wickets_last_12_balls",
    ],
    [
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
        Types.INT(),
        Types.INT(),
        Types.INT(),
        Types.INT(),
        Types.INT(),
        Types.INT(),
    ],
)
