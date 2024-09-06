import logging
import os
from datetime import datetime, timedelta, timezone

from attrs import define, field, validators

from action_observer.common.events.events_publisher import EventsPublisher
from action_observer.common.events.status import Status

logger = logging.getLogger(__name__)
LOGLEVEL = os.environ.get("LOGGING_MODE", "INFO").upper()
logger.setLevel(LOGLEVEL)

component_tool = "dbt"

time_offsets_relative_to_end = {
    "run_metrics": -2,  # publish run metrics this many milliseconds after the run ends
    "outcomes_metrics": -4,  # publish the outcomes metrics this many milliseconds before the run ends
    "outcomes_end_time": -6,
}

time_offsets_relative_to_start = {
    "null_run_result_start_time": 1,
    "null_run_result_end_time": 2,
    "run_end_time": 10,  # when there is no start/end this ensures that the event happens after the run starts
}


@define(kw_only=True, slots=False)
class DBTResultsPublisher:
    manifest: dict = field(validator=validators.instance_of(dict))
    run_results: dict = field(validator=validators.instance_of(dict))
    events_publisher: EventsPublisher = field(validator=validators.instance_of(EventsPublisher))

    pipeline_key: str = field(validator=validators.instance_of(str))
    pipeline_name: str = field(validator=validators.instance_of(str))

    @staticmethod
    def dbt_status_to_datakitchen_run_status(status: str, resource_type: str) -> Status:
        """
        Convert dbt status to datakitchen status
        :param status: dbt status
        :return: datakitchen status
        @param status:
        @param resource_type:
        """
        # RUNNING = "RUNNING"
        # COMPLETED = "COMPLETED"
        # COMPLETED_WITH_WARNINGS = "COMPLETED_WITH_WARNINGS"
        # FAILED = "FAILED"
        # UNKNOWN = "UNKNOWN"

        if resource_type == "test":
            if status == "pass":
                task_status = Status.COMPLETED
            elif status == "warn":
                task_status = Status.COMPLETED_WITH_WARNINGS
            elif status == "fail":
                task_status = Status.FAILED
            else:
                # The unknown will crash the events API, so send along a warning
                task_status = Status.COMPLETED_WITH_WARNINGS
        else:
            if status == "success":
                task_status = Status.COMPLETED
            elif status == "warn":
                task_status = Status.COMPLETED_WITH_WARNINGS
            elif status == "error":
                task_status = Status.FAILED
            else:
                # The unknown will crash the events API, so send along a warning
                task_status = Status.COMPLETED_WITH_WARNINGS
        return task_status

    @staticmethod
    def dbt_status_to_datakitchen_outcome_status(status: str) -> str:
        if status == "pass":
            return "PASSED"
        elif status == "fail":
            return "FAILED"
        elif status == "warn":
            return "WARNING"
        else:
            # The unknown will crash the events API, so send along a warning
            return "WARNING"

    def find_run_start_and_end(self) -> tuple[datetime, datetime]:
        max_time = datetime.max.replace(tzinfo=timezone.utc)
        run_start_time = max_time

        # Find the earliest start time.
        # There is an elapsed key in the results we will use to get the end time.
        for result in self.run_results["results"]:
            run_result_start_time, run_result_end_time = self.find_run_result_timings(result)
            if run_result_start_time is None or run_result_end_time is None:
                continue
            if run_result_start_time < run_start_time:
                run_start_time = run_result_start_time

        if run_start_time == max_time:
            run_start_time = datetime.strptime(self.run_results["metadata"]["generated_at"], "%Y-%m-%dT%H:%M:%S.%f%z")

        run_end_time = run_start_time + timedelta(seconds=self.run_results["elapsed_time"]) + timedelta(milliseconds=1)
        run_start_time = run_start_time - timedelta(milliseconds=1)
        return run_start_time, run_end_time

    @staticmethod
    def find_run_result_timings(run_result: dict) -> tuple[datetime, datetime] | tuple[None, None]:
        if len(run_result["timing"]) != 0:
            run_result_start_time = datetime.max.replace(tzinfo=timezone.utc)
            run_result_end_time = datetime.min.replace(tzinfo=timezone.utc)
            for timing in run_result["timing"]:
                this_start_time = datetime.strptime(timing["started_at"], "%Y-%m-%dT%H:%M:%S.%f%z")
                if this_start_time < run_result_start_time:
                    run_result_start_time = this_start_time

                this_end_time = datetime.strptime(timing["completed_at"], "%Y-%m-%dT%H:%M:%S.%f%z")
                if this_end_time > run_result_end_time:
                    run_result_end_time = this_end_time
            return run_result_start_time, run_result_end_time
        else:
            return None, None

    def parse(self) -> None:
        run_key = self.run_results["metadata"]["invocation_id"]

        # infer the run time from all of the tasks
        run_start_time, run_end_time = self.find_run_start_and_end()

        self.events_publisher.publish_run_status_event(
            event_timestamp=run_start_time,
            task_key=None,
            run_key=run_key,
            status=Status.RUNNING,
            pipeline_name=self.pipeline_name,
            pipeline_key=self.pipeline_key,
            task_name=None,
            component_tool=component_tool,
        )

        outcomes = []
        outcomes_end_time = run_end_time + timedelta(milliseconds=time_offsets_relative_to_end["outcomes_end_time"])
        outcomes_metrics_end_time = run_end_time + timedelta(
            milliseconds=time_offsets_relative_to_end["outcomes_metrics"]
        )
        run_metrics_time = run_end_time + timedelta(milliseconds=time_offsets_relative_to_end["run_metrics"])
        test_count = 0
        test_failed_count = 0
        test_passed_count = 0
        test_warned_count = 0
        task_count = 0
        task_error_count = 0
        task_warning_count = 0
        task_skipped_count = 0

        for result in self.run_results["results"]:
            # We will use this node later.
            node = self.manifest["nodes"][result["unique_id"]]

            # Keep counters so we can do a log at the end.
            task_count += 1
            if result["status"] == "error":
                task_error_count += 1
            elif result["status"] == "warn":
                task_warning_count += 1
            elif result["status"] == "skipped":
                task_skipped_count += 1
                continue

            # Some resources are tests, so track them separately
            if node["resource_type"] == "test":
                test_count += 1
                if result["status"] == "pass":
                    test_passed_count += 1
                if result["status"] == "fail":
                    test_failed_count += 1
                if result["status"] == "warn":
                    test_warned_count += 1

            metadata = {
                "unique_id": result["unique_id"],
                "resource_type": node["resource_type"],
                "node": node,
                "run_result": result,
            }

            run_result_start_time, run_result_end_time = self.find_run_result_timings(result)
            if run_result_start_time is None or run_result_end_time is None:
                run_result_start_time = run_start_time + timedelta(
                    milliseconds=time_offsets_relative_to_start["null_run_result_start_time"]
                )
                run_result_end_time = run_start_time + timedelta(
                    milliseconds=time_offsets_relative_to_start["null_run_result_end_time"]
                )

            # Regardless of whether this is a model or a test, we want to publish the event so we know when it started
            self.events_publisher.publish_run_status_event(
                event_timestamp=run_result_start_time,
                task_key=result["unique_id"],
                run_key=run_key,
                status=Status.RUNNING,
                pipeline_name=self.pipeline_name,
                pipeline_key=self.pipeline_key,
                task_name=result["unique_id"],
                metadata=metadata,
                component_tool=component_tool,
            )

            # Regardless of whether this is a model or a test, we want to publish the event so we know when it ended
            self.events_publisher.publish_run_status_event(
                event_timestamp=run_result_end_time,
                task_key=result["unique_id"],
                run_key=run_key,
                status=self.dbt_status_to_datakitchen_run_status(result["status"], node["resource_type"]),
                pipeline_name=self.pipeline_name,
                pipeline_key=self.pipeline_key,
                task_name=result["unique_id"],
                metadata=metadata,
                component_tool=component_tool,
            )

            # If we are handling a test, keep track of certain metrics and outcomes
            if node["resource_type"] == "test":
                outcome = {
                    "name": result["unique_id"],
                    "status": self.dbt_status_to_datakitchen_outcome_status(result["status"]),
                    "metadata": result,
                    "start_time": run_result_start_time,
                    "description": node["description"],
                    "end_time": run_result_end_time,
                }

                outcomes.append(outcome)

        if len(outcomes) != 0:
            self.events_publisher.publish_test_outcomes_event(
                event_timestamp=outcomes_end_time,
                pipeline_key=self.pipeline_key,
                run_key=run_key,
                test_outcomes=outcomes,
                task_key=None,
                component_tool=component_tool,
            )

            self.events_publisher.publish_metric_log_event(
                event_timestamp=outcomes_metrics_end_time,
                pipeline_key=self.pipeline_key,
                run_key=run_key,
                task_key=None,
                metric_key="total_tests",
                metric_value=test_count,
                component_tool=component_tool,
            )

            self.events_publisher.publish_metric_log_event(
                event_timestamp=outcomes_metrics_end_time,
                pipeline_key=self.pipeline_key,
                run_key=run_key,
                task_key=None,
                metric_key="tests_passed",
                metric_value=test_passed_count,
                component_tool=component_tool,
            )

            self.events_publisher.publish_metric_log_event(
                event_timestamp=outcomes_metrics_end_time,
                pipeline_key=self.pipeline_key,
                run_key=run_key,
                task_key=None,
                metric_key="tests_failed",
                metric_value=test_failed_count,
                component_tool=component_tool,
            )

            self.events_publisher.publish_metric_log_event(
                event_timestamp=outcomes_metrics_end_time,
                pipeline_key=self.pipeline_key,
                run_key=run_key,
                task_key=None,
                metric_key="tests_warned",
                metric_value=test_warned_count,
                component_tool=component_tool,
            )

        if task_error_count != 0 or test_failed_count != 0:
            run_status = Status.COMPLETED_WITH_WARNINGS
        elif task_warning_count != 0 or test_warned_count != 0:
            run_status = Status.COMPLETED_WITH_WARNINGS
        else:
            run_status = Status.COMPLETED

        # Send over the stats on the dbt job tasks
        # f" Passed: {}, Errored: {}, Warned: {}, Skipped: {}"
        self.events_publisher.publish_metric_log_event(
            event_timestamp=run_metrics_time,
            pipeline_key=self.pipeline_key,
            pipeline_name=self.pipeline_name,
            run_key=run_key,
            task_key=None,
            metric_key="tasks_total",
            metric_value=task_count,
            component_tool=component_tool,
        )

        self.events_publisher.publish_metric_log_event(
            event_timestamp=run_metrics_time,
            pipeline_key=self.pipeline_key,
            pipeline_name=self.pipeline_name,
            run_key=run_key,
            task_key=None,
            metric_key="tasks_successful",
            metric_value=(task_count - task_error_count - task_warning_count - task_skipped_count),
            component_tool=component_tool,
        )

        self.events_publisher.publish_metric_log_event(
            event_timestamp=run_metrics_time,
            pipeline_key=self.pipeline_key,
            pipeline_name=self.pipeline_name,
            run_key=run_key,
            task_key=None,
            metric_key="task_warnings",
            metric_value=task_warning_count,
            component_tool=component_tool,
        )

        self.events_publisher.publish_metric_log_event(
            event_timestamp=run_metrics_time,
            pipeline_key=self.pipeline_key,
            pipeline_name=self.pipeline_name,
            run_key=run_key,
            task_key=None,
            metric_key="task_errors",
            metric_value=task_error_count,
            component_tool=component_tool,
        )

        self.events_publisher.publish_metric_log_event(
            event_timestamp=run_metrics_time,
            pipeline_key=self.pipeline_key,
            pipeline_name=self.pipeline_name,
            run_key=run_key,
            task_key=None,
            metric_key="tasks_skipped",
            metric_value=task_skipped_count,
            component_tool=component_tool,
        )

        # We are done parsing the run results. Close this out.
        self.events_publisher.publish_run_status_event(
            event_timestamp=run_end_time,
            task_key=None,
            run_key=run_key,
            status=run_status,
            pipeline_name=self.pipeline_name,
            pipeline_key=self.pipeline_key,
            task_name=None,
            component_tool=component_tool,
        )
        logger.info(
            f"Tests: {test_count}, Passed: {test_passed_count}, Failed: {test_failed_count}, Warned: {test_warned_count}"
        )
        logger.info(
            f"Tasks: {task_count}, Successful: {task_count-task_error_count-task_warning_count-task_skipped_count}, Errored: {task_error_count}, Warned: {task_warning_count}, Skipped: {task_skipped_count}"
        )
