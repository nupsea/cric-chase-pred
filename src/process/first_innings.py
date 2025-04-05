import json

from pyflink.datastream.functions import MapFunction, ProcessWindowFunction


class ParseJson(MapFunction):
    def map(self, value):
        return json.loads(value)


class ComputeFirstInnings(ProcessWindowFunction):
    def process(self, key, context, elements):
        total_runs = 0
        total_wickets = 0
        ball_count = 0

        for event in elements:
            # Convert to int as needed
            total_runs += int(event.get("total_run", 0) or 0)
            total_wickets += int(event.get("is_wicket", 0) or 0)
            ball_count += 1

        target_score = total_runs + 1
        result = (key, target_score)

        yield result
