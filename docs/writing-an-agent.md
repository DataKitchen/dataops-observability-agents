# Tutorial: Writing an Agent

## Purpose

This document is a tutorial that will guide you through making an agent that functions against the Agent-target service:
A mock app created for this tutorial.
See source code: [here](<URL>)

## Process

There are 5 steps you will take when writing an agent.

1. Create a module for your agent
2. Define a Configuration validation class
3. Write Tasks and Handles
4. Write or use a loop for your agent.
5. Write and add a sub-main to the agent entry-point
6. Write tests.

We'll walk through each of these steps one-by-one. We will start with an Event driven agent. For a period driven agent
go ahead and look at the databricks agent.

## agent-target Agent

### 1. Creating a module

#### Code files

Your agent-specific code should be in its own module. In general, when you are trying to decide where code should go,
you should follow these general guidelines:

* Code which is useful to all _agents_ should be placed under `framework`.
* Code which is useful for all _Observability integrations_ (scripts, agents, etc) should go under `toolkit`.
* Code which is useful for only a specific agent should go under its own module.

So, for our agent if we have the following modules:

```sh
framework/
testlib/
toolkit/
databricks/
```

we will create a new folder for target_agent. We prefer snake_case, so we'll call it `target_example`. Run the following to
create that module.

```sh
mkdir -p target_example && touch target_example/__init__.py
```

Which gets us this:

```sh
framework/
testlib/
toolkit/
databricks/
+target_example/
```

We're going to have a specific Configuration, A `Task`, a `Loop`, and an agent `main()` function, so we'll also create files for each of those.
Files are cheap - it's fine to have many small ones.

```sh
touch target_example/configuration.py
touch target_example/handles.py
touch target_example/loop.py
touch target_example/task.py
touch target_example/agent.py
```


#### Test files

We require that agents are tested. Those files will live under `tests/<type>/<agent-name>/`

We have two types of tests right now:

* unit - single functions. These tests are expected to mock their dependencies and achieve high code coverage.
* functional - These tests are expected to hit real services setup for the purpose of testing.

so for our agent, run the following:

```sh
mkdir -p tests/{unit,functional}/target_example && touch tests/{unit,functional}/target_example/__init__.py
```

### 2. Define a Configuration validation class.

#### Background

Agents need to define, parse, and validate their configurations. This is so
they [Fail Fast](https://www.martinfowler.com/ieeeSoftware/failFast.pdf) when they've been configured improperly, and so
our users get feedback immediately. This also prevents us from needing to sprinkle our codebase with snippets of logic
coercing values and edge cases.

The framework relies on the `pydantic` library to define configurations. `Pydantic` is used to define an object with
members that have assigned [types](https://docs.pydantic.dev/latest/concepts/types/) that are validated when the object
is constructed. This is very much like a dataclass or a `marshmallow` model, if you are familiar with that.

Configurations are namespaced. Your agent will typically care about two models:


* `CoreConfiguration`:  defines configurations shared by all agents.
* A model with your specific settings.

#### Example

For the AgentTarget configuration, we want the user to be able to configure the following things.

| Configuration Name | Description                                                                             |
|--------------------|-----------------------------------------------------------------------------------------|
| target_url         | The target URL where the service is located. It needs to accept ws and wss (websockets) |
| component_type     | the resulting component type. Case insensitive.                                         |
| timeout            | A timeout on web requests                                                               |

What we get is the following:

```python
# constr -> "Constrained String"
from pydantic import NonNegativeFloat, StringConstraints
from typing import Annotated
from toolkit.configuration.setting_types import WebSocketUrl
from pydantic_settings import BaseSettings

# NOTE: (?i) turns on case-insensitive regex. Pydantic will apply the regex before to_upper.
# NOTE: You have to use StringConstraints instead of constr to satisfy mypy.
COMPONENT_TYPE = Annotated[str, StringConstraints(to_upper=True, pattern=r"(?i)BATCH_PIPELINE|DATASET")]


class ExampleConfiguration(BaseSettings):
   # Tip: pydantic also has a pre-made HttpUrl
   target_url: WebSocketUrl
   component_type: COMPONENT_TYPE
   timeout: NonNegativeFloat = 120.0
   model_config = SettingsConfigDict(env_prefix="DK_EXAMPLE_")

```

This configuration will ensure:

* target_url is a valid Websocket URL
* timeout is non-negative
* component_type is case-insensitive, and can only be "dataset" and "batch_pipeline"
* timeout is defaulted to 120.0 seconds.

NOTE: We may want to do something more nuanced with a field before we evaluate it. We can use  [field validator](https://docs.pydantic.dev/latest/concepts/validators/#field-validators). Say we were not using `constr`, we could ensure to_upper like so:

```python
from pydantic import field_validator
...
    @field_validator("component_type", mode="before")
    @classmethod
    def component_type_to_upper(cls, component_type: str) -> str:
        if component_type:
            return component_type.upper()
        return component_type
```

NOTE: You might notice `WebSocketUrl`. That's one of our own types. Pydantic has a lot of powerful building blocks for defining new types using `typing.Annotated`.


#### agent_type

To add a new agent, you will need to add a new value to `CoreConfiguration.agent_type`. This is how the framework will
identify your agent when you launch the container.

We'll add "example_agent" as the string. This is how we will identify AgentTarget agents.

```python
# in framework/configuration/common.py
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Literal


class CoreConfiguration(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DK_")
    """Ensures all environment variables are read from DK_<key-name>. Environment variables are case insensitive."""


-   agent_type: Literal["databricks"]
+   agent_type: Literal["databricks", "target_example"]
```

#### File configuration

Configurations can be defined in an `agent.toml`, an `/etc/observability/agent.toml`. When you use this method with the container, you'll need to mount the file into the container with a `-v`.

```sh
touch agent-setting-dir/agent.toml
docker run observability-agent:... -v /agent-setting-dir:/etc/observability
```

#### Environment variables and namespaces

Our configuration is namespaced - meaning settings are sorted into particular places both within the environment, and within the configuration file. The file namespace is going to match the `agent_type`, like so (in the .toml file)

```toml
[your_agent]
setting = "foo"
```

meanwhile, your environment variable namespace will be defined by setting a `model_config` in your settings class.

Example:

```shell
from pydantic_settings import BaseSettings, SettingsConfigDict
class NewBaseConfiguration(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DK_YOUR_AGENT_")
    foo: str
```

By doing this, the settings class will automatically read the environment if no other data is provided. It will look for a variable named `{env_prefix}{name}`. In the above example, that means the class will source data for `foo` from the environment variable `DK_YOUR_AGENT_FOO` Note: The variable look up is case-insensitive.

These are equivalent:

```shell
export DK_EXAMPLE_COMPONENT_TYPE=BATCH_PIPELINE
```

and

```toml
# in agent.toml
[example]
component_type = "BATCH_PIPELINE"
```

Or in `docker`:

```sh
docker run ... -e DK_COMPONENT_TYPE=BATCH_PIPELINE
```

You can mix-and-match these styles of configuration, so long as the combination of file, environment, and defaults provide a complete configuration.

NOTE: `agent.toml` settings take precedence over environment variables.

#### Registry

The settings registry defined in `registry/configuration_registry.py` allows us to lookup a configuration object without passing the object everywhere. Once you register your new configuration, it can be looked up anywhere. You will need to do the following to make it work:

1. Add your types to `registry/configuration_registry.py`
2. call `register` to alert the framework.

##### Add the types to the registry

Within configuration_registry.py, you'll need to add your configuration's type, and its ID to the following.

* `ConfigurationDict`
* `CONFIGURATION_ID`
* `CONFIGURATION_TYPES`
* `CONF_T`

Example:

Our configuration's type is the `ExampleConfiguration` we wrote above. Its ID is `example`.

The Example configuration was added like this:

```py
class ConfigurationDict(TypedDict, total=False):
    core: CoreConfiguration
    http: HTTPClientConfig
+   example: ExampleConfiguration
    databricks: DatabricksConfiguration


- CONFIGURATION_ID = Literal["core", "http", "databricks"]
+ CONFIGURATION_ID = Literal["core", "http", "databricks", "example"]

"""
The section 'name' of your configuration. This is used to look up your particular configuration.
"""
- CONFIGURATION_TYPES = CoreConfiguration | HTTPClientConfig | DatabricksConfiguration
+ CONFIGURATION_TYPES = CoreConfiguration | HTTPClientConfig | DatabricksConfiguration | ExampleConfiguration

- CONF_T = TypeVar("CONF_T", CoreConfiguration, HTTPClientConfig, DatabricksConfiguration)
+ CONF_T = TypeVar("CONF_T", CoreConfiguration, HTTPClientConfig, DatabricksConfiguration, ExampleConfiguration)
```

##### 2. Register the type

Within your main, just call the following to register the type.

```python
from registry import ConfigurationRegistry
from target_example.configuration import ExampleConfiguration
...
registry = ConfigurationRegistry()
registry.register("example", ExampleConfiguration)

# To retrieve it later
my_config = registry.lookup("example", ExampleConfiguration)
```

NOTE: You do not need to pass around `registry`. Once you register a configuration once, all newly created `ConfigurationRegistry`s will know about it.
Just create a fresh ConfigurationRegistry whenever you need to look up a registry.

### 3. Write Tasks and Handles

#### Handle: Background

A `Handle` is just the singular unit of work within your agent. This separation exists to make testing and reuse easier.

Handles are expected to have three methods:

* pre_hook: For any task that needs to happen after initialization (think of it as an post_init)
* handle: This is where the main body of work is expected to happen. Do requests, lookups, etc. here.
* post_hook: Put any post-processing, conversion, or cleanup here.

These methods are separated since you might have a time-out around any of these methods.


#### Example: Handle

We'll create our Handle by inheriting from the `framework/handles/handle.py:Handle` class. Here's how we break down the methods:

* **pre_hook**: We have no need for `pre_hook`, so we will just define an empty one to satisfy the interface.
* **handle**: `Agent-target` continuously sends us messages. Since that is the main I/O action, we put that in `handle()`.
* **post_hook**: The websocket message is read as a JSON-string, but we want to pass it on as a JSON. We'll also need to have a routing key for `EventSenderTask`. The deserializing and post-processing will be defined in `post_hook`.

```python
# target_example/handles.py
import json
from trio_websocket import WebSocketConnection
from framework.core.handles import Handle
from toolkit.observability import EVENT_TYPE_KEY, EventType
from toolkit.more_typing import JSON_DICT


class WebsocketHandle(Handle[str, JSON_DICT]):
    def __init__(self, connection: WebSocketConnection):
        self.connection = connection

    async def pre_hook(self) -> None:
        pass

    async def handle(self) -> str:
        message: str = await self.connection.get_message()
        return message

    async def post_hook(self, value: str) -> JSON_DICT:
        payload: JSON_DICT = json.loads(value)
        payload[EVENT_TYPE_KEY] = EventType.RUN_STATUS.value
        return payload
```

#### Tasks: Background

Tasks exist to orchestrate Handles and manage the lifetime of any resource that lives longer than an individual handle.

As mentioned, a task exists to orchestrate the handle(s) in your task. The pattern is pretty consistent:

* Call your pre_hook
* Call your handle, gather result
* Pass your result to post_hook
* call send() with post_hook payload

##### Timeouts

In trio, timeouts are handled by the caller, not the called. Thus, when you have a section you want to wrap in timeout, you have several methods.
`trio.fail_after`, `trio.move_on_after` and others.

* `trio.fail_after`: When the timeout is reached, a trio.TooSlowError is raised
* `trio.move_on_after`: When a timeout is reached, the context is exited and the function moves on.

See: [Trio Documentation](https://trio.readthedocs.io/en/stable/reference-core.html#cancellation-and-timeouts)

we use framework/timing/timeouts.py:timeout_scope_log, which demonstrates what a fail_after looks like.

```python
# framework/timing/timeouts.py
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from trio import CancelScope, TooSlowError, fail_after
LOGGER = logging.getLogger(__name__)

@asynccontextmanager
async def timeout_scope_log(timeout: float, name: str) -> AsyncGenerator[CancelScope, None]:
    try:
        c: CancelScope
        with fail_after(timeout) as c:
            yield c
    except TooSlowError:
        logging.exception("Could not complete '%s'. Failed after %f seconds.", name, timeout)
        raise
```

#### Task: Example

We want a timeout on all of our hooks, so we wrap them in `timeout_scope_log`.

```python
# target_example/task.py
import logging
from typing import final
from trio import MemorySendChannel, TooSlowError
from trio_websocket import WebSocketConnection
from framework.core.handles import Handle
from framework.tasks import Task
from framework.timing import timeout_scope_log
from toolkit.more_typing import JSON_DICT
from .handles import WebsocketHandle
from .configuration import ExampleConfiguration
from registry import ConfigurationRegistry

LOGGER = logging.getLogger(__name__)


@final
class WebsocketTask(Task):
    def __init__(self, outbound_channel: MemorySendChannel):
        super().__init__(outbound_channel=outbound_channel)
        self.configuration = ConfigurationRegistry().lookup("example", ExampleConfiguration)
        self.timeout = self.configuration.timeout

    async def execute(self, client: WebSocketConnection) -> None:
        try:
            handle: Handle = WebsocketHandle(client)
            async with timeout_scope_log(self.timeout, f"{WebsocketTask.__name__}.pre_hook"):
                await handle.pre_hook()
            async with timeout_scope_log(self.timeout, f"{WebsocketTask.__name__}.handle"):
                value = await handle.handle()
            async with timeout_scope_log(self.timeout, f"{WebsocketTask.__name__}.post_hook"):
                payload: JSON_DICT = await handle.post_hook(value)
            await self.send(payload)
        except TooSlowError:
            return
```

#### Task: Resource management

Tasks are responsible for managing the lifetime of certain resources, namely channels. You will notice we pass a channel here (more on that in Loops).

If your task has resource, you should create a `__aenter__ / __aexit__` pair to make the task a [context-manager](https://docs.python.org/3/library/stdtypes.html#typecontextmanager).

That would look like this,

```python
 async def __aenter__(self) -> Self:
        """
        See: https://docs.python.org/3/reference/datamodel.html#object.__aenter__
        """
        # call any resources aenter, or otherwise open the resource.
        await self.resource.__aenter__()
        return await super().__aenter__()


    async def __aexit__(self, exc_type: type[BaseException], exc_val: BaseException, exc_tb: TracebackType) -> None:
        """
        See: https://docs.python.org/3/reference/datamodel.html#object.__aexit__.
        """
        # free resource
        await self.resource.__aexit__()
        await super().__aexit__()
```


**WARNING**: *Remember to call the parent's method with `super()`!*
**NOTE**: *The `Task` base class's `__aenter__` / `__aexit__` handles `outbound_channel` for you.*

#### Task/Handle FAQ

##### Task/handle Size?

A common question is 'how big should a handle/task be?'

keep tasks as logically small and singular of purpose as possible. This has the following benefits:

1. Small, broken up tasks will allow the asynchronous framework to do more background processing while a task/handle is
   blocked.
2. It will allow us to write more composable, reusable units/
3. Smaller units will be easier to test and debug.
4. Smaller units are more likely to follow
   the [separation of concerns](https://learn.microsoft.com/en-us/dotnet/architecture/modern-web-apps-azure/architectural-principles#separation-of-concerns)
   principle.

### 4. Write and customize a loop for your agent

#### Background: Loop

Your agent will be made of recurring operations that we'll call _loops_. Your agent main may have multiple loops. A `Loop`'s job is to orchestrate the lifetime of `Tasks`.
If you follow the `framework/loops/loop.py:Loop` base class, you can write your own loop, but in most cases you will use one of the two base loops:

* `framework/loops/periodic_loop.py:PeriodicLoop`. This run is for periodic operations, and passes the current and previous times. It can be set to run every x seconds.
* `framework/loops/channel_receive_loop.py:ChannelReceiveLoop`: This loop infinitely reads from a receivable channel, and is useful for tasks which receive messages from other tasks.

the `example_agent` is event-driven, and needs its loop run.

##### Example: Loop

Here's how we break it down:

* Our loop has one task: WebsocketTask. It has a resource-lifetime (outbound_channel).
* We want to our loop to attempt reconnection.
* Our task needs a connection to the websocket that lives longer than the task
* Our task will retry from the beginning if it was too slow.

That looks like this:

```python
# target_example/loop.py
import logging

from trio_websocket import ConnectionClosed, WebSocketConnection, open_websocket_url
from trio import TooSlowError
from framework.loops import Loop
from toolkit.configuration.setting_types import WebSocketUrl
from .task import WebsocketTask
LOGGER = logging.getLogger(__name__)
class WebsocketLoop(Loop):
    def __init__(self, task: WebsocketTask, target_url: WebSocketUrl, connect_timeout: float):
        super().__init__(task)
        self.target_url: str = str(target_url)
        self.connect_timeout: float = connect_timeout

    async def run(self) -> None:
        with self.task:
            while True:
                try:
                    ws: WebSocketConnection
                    async with open_websocket_url(self.target_url, connect_timeout=self.connect_timeout) as ws:
                        while True:
                            try:
                                await self.task.execute_task(ws)
                            except TooSlowError:
                                continue
                except ConnectionClosed:
                    LOGGER.warning("Connection Closed! attempting to reconnect...")

```


### 5. Writing and adding a main to the agent entry-point

The agent has a single master entrypoint. It is located in `framework/__main__.py` You will need to write your own sub-main, and
then hook it there.

#### Adding your agent to `framework.__main__`

This is the entry point for _every_ agent. This main will use the `agent_type` configuration to launch into you agent's main function. If you recall, we called our agent `example_agent`. So, we would hook in our agent by making the following change

```python
from target_example import main as example_main
...
try:
    match core_config.agent_type:
        case "databricks"
            await databricks_agent())
+        case "example_agent":
+            await example_main())
        case _:
            raise NotImplementedError(f"{agent_type.agent_type}: unknown agent type.")
```

**Author's Note: Why not autodiscovery?**: 'Magical' features tend to be opaque and brittle.

#### Writing your main

Your main serves to do two things:
* Invoke configuration validation
* add loops to Trio
* Link tasks together with `channels`.

#### Add tasks to Trio

We add tasks to Trio using a `Nursery`. A [nursery](https://trio.readthedocs.io/en/stable/reference-core.html#trio.Nursery) is simply a context-manager you attach async functions. It ensures your tasks always get cleaned up. You can have  multiple of these nested even within your loops, handles, and tasks, but the agent main is the **top** level context.

#### Glue Tasks together: Channels

Next; We need to tell our tasks how to communicate with each other. We do that using [Channels](https://trio.readthedocs.io/en/stable/reference-core.html#using-channels-to-pass-values-between-tasks). Channels have two pieces: A class that you use to send data into the channel, and a class to take data out of a channel. We give one end to each task so that they can communicate.

*Note: 'Channel' is just Trio's word for 'queue'. Data goes in one end, and comes out the other. It's very easy to accidentally overthink this concept and the author is equally frustrated with the language of 'channels' and 'nurseries'*

#### Putting it together: The main

Our main will have two tasks: The task we wrote, and another framework task: `EventSenderTask`. EventSenderTask is useful for sending generic events to Observability. Since these tasks need to communicate, we need a channel-pair. Your main will look nearly identical to this one.

```python
# target_example/agent.py
import logging
from trio import open_memory_channel, open_nursery
from framework.core.loops import ChannelReceiveLoop
from framework.observability import EventSenderTask, create_heartbeat_loop
from framework.configuration import CoreConfiguration
from toolkit.more_typing import JSON_DICT
from .configuration import ExampleConfiguration
from registry.configuration_registry import ConfigurationRegistry
from .loop import WebsocketLoop
from .task import WebsocketTask

LOGGER = logging.getLogger(__name__)


async def main() -> None:
    registry = ConfigurationRegistry()
    registry.register("example", ExampleConfiguration)
    agent_config = registry.lookup("example", ExampleConfiguration)
    core_config = registry.lookup("core", CoreConfiguration)

    max_channel_capacity = int(core_config.max_channel_capacity)
    event_queue_send, event_queue_receive = open_memory_channel[JSON_DICT](max_channel_capacity)
    async with open_nursery() as n:
        n.start_soon(
            WebsocketLoop(
                connect_timeout=agent_config.timeout,
                target_url=agent_config.target_url,
                task=WebsocketTask(outbound_channel=event_queue_send),
            ).run
        )
        n.start_soon(
            ChannelReceiveLoop(
                inbound_channel=event_queue_receive, task=EventSenderTask()
            ).run
        )
        n.start_soon(create_heartbeat_loop(tool="example tool", configuration=agent_config))
```

The loops will begin executing as soon as you hit the end of the context. The agent-main exists solely to tie agent
tasks together, so there is not much more beyond this in any main. See `databricks/agent.py` for another example.

Use the pre-existing `create_heartbeat_loop` to start sending heartbeats to Observability.

**Note:** If you need the loops to do some kind of asynchronous action before the wholesale operation begins, consider using `start` instead of `start_soon.`

## 5. Writing tests for your tasks and agent

Tests for the agent framework are written
using [pytest](https://docs.pytest.org/en/7.4.x/), [pytest-trio](https://pytest-trio.readthedocs.io/),
and [pytest-httpx](https://pypi.org/project/pytest-httpx/)

### Define your tests with `async def test_*`

If you're testing `my_func`, each test definition should be defined like this

```python
import pytest

# or pytest.mark.function()
pytest.mark.unit()
async def test_my_func():
    assert True
```

### use the `autojump_clock` fixture

Asynchronous programs tend to sleep or block more than synchronous programs. Waiting for this would make the tests
extremely slow. Luckily, pytest-trio defines the `autojump_clock` fixture.

This fixture virtualizes trio's clock and makes it deterministic. If your function is sleeping for 5 seconds, it will
simply jump the clock by 5 seconds rather than waiting. This fixture is global, so all you need to do is pass it into
your tests.

```python
from trio import sleep

# ends immediately because of autojump_clock
async def test_my_func(autojump_clock):
    await sleep(5)
```

See: [Homepage](https://pytest-trio.readthedocs.io/en/stable/)

### testlib fixtures are plugins

Sharing raw data between multiple test suites is a chore, and it makes test-refactors painful when we change or move
test data. This project solves that by treating those fixtures as plugins to pytest.

Like `auto_jumpclock` and `httpx_mock`, the fixtures defined in `testlib` are loaded as pytest plugins. This way, they
can be referenced globally like `autojump_clock` without being imported.

You can see how this is done in `conftest.py`. The consequence of this is that the root of the test-execution must be
the root of the repository. A `.run` has been included for PyCharm in this repo, so it should just 'work'. If not, see
README.md for details on what needs to be set and how.


### Further Reading:

* This example is held under the target_example module
* The databricks agent under `databricks` is another example.
* [Pytest-httpx](https://colin-b.github.io/pytest_httpx/): Useful for mocking API calls.
