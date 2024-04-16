from scripts.invocations.dev import *

"""
This is the top-level config for the `make`-like tool `invoke`.

The tasks are found in scripts/invocations where also any new task should be
implemented. When suitable, create a new module for the tasks.

`invoke` has the concept of task collections. At this point all task are added
to the main collection for convenience. Any subcollections are prependended to
the final task name thus causing extra typing for no reason (`invoke local` vs
`invoke deploy.local`). Other collections can be added when needed.
"""
