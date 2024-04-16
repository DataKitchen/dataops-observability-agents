import logging
import os
import pkgutil
import sys
import traceback
from importlib import util
from typing import List, Type, Union

from listener_agents.abstract_event_handler import AbstractEventHandler
from poller_agents.abstract_runs_fetcher import AbstractRunsFetcher

logger = logging.getLogger(__name__)


def fetch_plugins(
    abstract_plugin_class: Union[Type[AbstractEventHandler], Type[AbstractRunsFetcher]],
    plugins_paths: List[str],
) -> None:
    """
    Fetch plugins from the provided list of plugin paths. This method does not return the discovered
    plugins. Instead, each plugin must inherit from a base class (e.g.
    :obj:`AbstractEventHandler <agents.listeners.abstract_event_handler.AbstractEventHandler>` and
    :obj:`AbstractRunsFetcher <agents.pollers.abstract_runs_fetcher.AbstractRunsFetcher>`) that
    leverages `subclass registration <https://peps.python.org/pep-0487/#subclass-registration>`_ to
    automatically register the plugin.

    Parameters
    ----------
    abstract_plugin_class: class
        Abstract base class that contains a list of plugins
    plugins_paths: list
        List of paths containing plugins
    """
    # This function was loosely derived from this implementation:
    # https://gist.github.com/dorneanu/cce1cd6711969d581873a88e0257e312
    cur_plugins: List[str] = [p.__module__ for p in abstract_plugin_class.plugins]
    for m in pkgutil.iter_modules(plugins_paths):
        if m.name in cur_plugins or m.name not in os.getenv("ENABLED_PLUGINS", "").split(","):
            # TODO: This doesn't handle if an existing plugin has been modified
            continue

        try:
            logger.info(f"Loading new plugin {m.name}...")
            file_path = os.path.join(m.module_finder.path, m.name + ".py")  # type: ignore
            spec = util.spec_from_file_location(m.name, file_path)
            if spec is None or spec.loader is None:
                # TODO: Add an ignore list so as not to report this issue on every poll interval.
                logger.error(f"Failed to load spec for new plugin {m.name}. Ignoring.")
                continue
            module = util.module_from_spec(spec)
            sys.modules[m.name] = module
            spec.loader.exec_module(module)
            logger.info(f"Finished loading new plugin {m.name}")
        except Exception:
            logger.error(f"Failed to load new plugin {m.name}: {traceback.format_exc()}")
