Welcome to Observability Legacy Agent Framework documentation!
=================================================

This repository contains agents for receiving, filtering, parsing, and consequently forwarding events to DataKitchen's
`Events Ingestion API <https://api.docs.datakitchen.io/production/events.html>`_. These agents are extensible via a
plugin architecture to facilitate the integration of new tools for observation. The agents are currently divided into
two flavors: listeners and pollers. Listener agents subscribe to event streams, whereas polling agents actively
interrogate third party tools for events.

An example listener agent (event_hubs_agent.py) is provided that leverages `Azure Event Hubs
<https://azure.microsoft.com/en-us/products/event-hubs/>`_. Most native Azure
applications contain configurable Diagnostic Settings. These diagnostics may be forwarded to an Event Hub and
consequently received, processed, and forwarded to DataKitchen's
`Events Ingestion API <https://api.docs.datakitchen.io/production/events.html>`_ via this agent. For example, Azure
Data Factory has two diagnostic settings (i.e. Pipeline runs log and Pipeline activity runs log) that broadcast when
a pipeline and each of its activities starts/stops. To observe these events, simply configure the diagnostic settings to
forward these events to an Event Hub and run the agent with the configuration settings of that Event Hub. The agent in
conjunction with the ADF Event Handler plugin will receive and forward the applicable events to DataKitchen's
Observability platform.

An example poller agent (poller.py) is also provided. This agent polls third party tools to fetch pipeline runs and
interrogate each run on a regular polling interval to send events of interest. For instance, `Airflow
<https://airflow.apache.org/>`_ exposes an API that can be leveraged to fetch DAG Runs and state of each run. The
existing airflow plugin (airflow.py) is an example implementation of this functionality.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

    Code Documentation <apidoc/modules>

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
