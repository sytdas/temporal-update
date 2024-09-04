import asyncio
from dataclasses import dataclass
from datetime import timedelta
from enum import IntEnum
from typing import Any

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.common import RetryPolicy
from temporalio.worker import Worker

from utils import get_temporal_connection


class Language(IntEnum):
    Chinese = 1
    English = 2
    French = 3


@dataclass
class ComposeGreetingInput:
    greeting: str
    name: str


@dataclass
class UpdateWorkflowInput:
    workflow_id: str
    data: Any


@activity.defn
async def compose_greeting(input: ComposeGreetingInput) -> str:
    return f"{input.greeting}, {input.name}!"


@activity.defn
async def get_language():
    t = activity.datetime.now().time()
    return t.second % 3


@workflow.defn
class GreetingWorkflow:
    def __init__(self) -> None:
        self.greetings = {
            Language.Chinese: "你好，世界",
            Language.English: "Hello, world",
        }
        self.language = None
        self.count = 0  # some state we want to keep track
        self.lock = asyncio.Lock()

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: self.language is not None)
        result = await workflow.execute_activity(
            compose_greeting,
            ComposeGreetingInput(self.greetings[self.language], str(self.count)),
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(maximum_attempts=1)
        )
        self.count += 1
        # clear language and wait for next schedule
        self.language = None
        workflow.logger.info("Result: %s %d", (result, self.count))

    @workflow.update(unfinished_policy=workflow.HandlerUnfinishedPolicy.ABANDON)
    def set_language(self, language: Language):
        # 👉 An Update handler can mutate the Workflow state and return a value.
        workflow.logger.info("Updating Language")
        self.language = language
        return language

    @set_language.validator
    def validate_language(self, language: Language):
        if self.language:
            raise ValueError("current language is set")

    @workflow.query
    def get_count(self):
        return self.count


class TemporalActivity:
    def __init__(self, client: Client) -> None:
        self.client = client

    @activity.defn
    async def update_workflow(self, update_input: UpdateWorkflowInput):
        handler = self.client.get_workflow_handle_for(
            GreetingWorkflow.run,
            update_input.workflow_id
        )
        activity.logger.warn(f"Handler id {handler.id}")
        try:
            return await handler.execute_update(
                update=GreetingWorkflow.set_language,
                arg=update_input.data,
            )
        except Exception as e:
            activity.logger.exception("Error:", exc_info=e)


@workflow.defn
class GreetingCronWorkflow:
    @workflow.run
    async def run(self, workflow_id: str):
        lang = await workflow.execute_activity(get_language, start_to_close_timeout=timedelta(seconds=5))
        result = await workflow.execute_activity_method(
            TemporalActivity.update_workflow,
            UpdateWorkflowInput(workflow_id=workflow_id, data=lang),
            start_to_close_timeout=timedelta(seconds=50),
            retry_policy=RetryPolicy(maximum_attempts=1)
        )
        return result


async def main():
    # Start client

    address, tls, namespace = get_temporal_connection("default")
    client = await Client.connect(
        address,
        namespace=namespace,
        tls=tls,
    )
    temporal_activity = TemporalActivity(client)

    try:
        await client.start_workflow(
            GreetingWorkflow.run,
            id="hello-workflow-id",
            task_queue="hello-cron-task-queue",
        )
        print("Running workflow once a minute on ")
        await client.start_workflow(
            GreetingCronWorkflow.run,
            "hello-workflow-id",
            id="hello-cron-workflow-id-scheduler",
            task_queue="hello-cron-task-queue-schedule",
            cron_schedule="*/2 * * * *",
            request_eager_start=True,
        )

    except:
        pass
    worker1 = Worker(
        client,
        task_queue="hello-cron-task-queue",
        workflows=[GreetingWorkflow],
        activities=[compose_greeting, ],
    )
    worker2 = Worker(
        client,
        task_queue="hello-cron-task-queue-schedule",
        workflows=[GreetingCronWorkflow],
        activities=[temporal_activity.update_workflow, get_language, ],
    )
    await asyncio.gather(worker1.run(), worker2.run())


if __name__ == "__main__":
    asyncio.run(main())
