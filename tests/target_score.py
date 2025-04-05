from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common import Types


class InningTransitionProcess(KeyedProcessFunction):
    """
    This process function maintains state per match.
    It accumulates first innings runs until it detects a transition to inning "2".
    At that moment, it emits the aggregated target score (total_runs + 1) and then
    processes the second innings events normally.
    """

    def open(self, runtime_context: RuntimeContext):
        # State to keep track of the current inning for the match
        self.current_inning_state = runtime_context.get_state(
            ValueStateDescriptor("current_inning", Types.STRING())
        )
        # State to accumulate total runs for first innings
        self.total_runs_state = runtime_context.get_state(
            ValueStateDescriptor("total_runs", Types.INT())
        )

    def process_element(self, value, ctx):
        # 'value' is a dict that contains at least "inning", "total_run", "match_id"
        match_id = value["match_id"]
        event_inning = str(value["inning"]).strip()  # Ensure it's a string

        current_inning = self.current_inning_state.value()
        if current_inning is None:
            # Initialize state with the inning of the first event
            current_inning = event_inning
            self.current_inning_state.update(current_inning)
            self.total_runs_state.update(0)

        if current_inning == "1":
            # Still in first innings, accumulate runs
            total_runs = self.total_runs_state.value() or 0
            total_runs += int(value.get("total_run", 0))
            self.total_runs_state.update(total_runs)

            # Check if the current event is a second innings event.
            if event_inning == "2":
                # Transition detected: first innings is over.
                # Compute target score: first innings total + 1
                target_score = total_runs + 1
                # Emit the aggregated result for the first innings as a special event.
                yield {
                    "match_id": match_id,
                    "target_score": target_score,
                    "first_innings_done": True,
                }
                # Update the current inning state to "2"
                self.current_inning_state.update("2")
                # Also emit the current event (the first event of inning 2)
                yield value
            else:
                # In first innings and no transition yet.
                # Optionally, you can choose not to emit every intermediate event.
                # For instance, only emit at the end of a window, or simply drop.
                pass
        else:
            # Already in second innings, pass the event along.
            yield value


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Create a source from Kafka (or any source)
    # For this example, assume parsed_stream is obtained from Kafka and parsed as JSON.
    # Example:
    # kafka_source = env.add_source(your_kafka_consumer)
    # parsed_stream = kafka_source.map(lambda x: json.loads(x))
    #
    # Here we simulate with a collection for illustration:
    sample_events = [
        # First innings events (inning "1")
        {"inning": "1", "total_run": "10", "match_id": "31231"},
        {"inning": "1", "total_run": "8", "match_id": "31231"},
        {"inning": "1", "total_run": "12", "match_id": "31231"},
        # Transition: first event of second innings (inning "2")
        {"inning": "2", "total_run": "0", "match_id": "31231"},
        {"inning": "2", "total_run": "4", "match_id": "31231"},
    ]
    parsed_stream = env.from_collection(
        sample_events, type_info=Types.MAP(Types.STRING(), Types.STRING())
    )

    # Key the stream by match_id
    keyed_stream = parsed_stream.key_by(lambda x: x["match_id"])

    # Process the stream with our InningTransitionProcess
    enriched_stream = keyed_stream.process(
        InningTransitionProcess(), output_type=Types.MAP(Types.STRING(), Types.STRING())
    )

    enriched_stream.print()

    env.execute("First to Second Innings Transition")


if __name__ == "__main__":
    main()
