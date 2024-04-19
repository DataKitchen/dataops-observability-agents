## Instructions on running DataKitchen Observability Events Hub Listener Agent or Poller Agent or Both inside a Kubernetes Cluster
* These steps will create Kubernetes Secret(s) and Deployment(s) inside the Kubernetes cluster.
### Steps to Install Agent
##### Prerequisite:

1. curl and kubectl packages installed (https://everything.curl.dev/get, https://kubernetes.io/docs/tasks/tools/)
2. Kubernetes Cluster
3. Access to Kubernetes cluster using `kubectl`
4. Required Values (https://docs.datakitchen.io/articles/dataops-observability-help/observability-integration-agents)
##### Deploy DataKitchen Observability Agent

* Run the below command with right *option* from list of valid options in a command line terminal that has access to the Kubernetes cluster and provide required values gathered from Prerequisite #4 and

```bash
curl -o setup.sh https://dk-support-external.s3.amazonaws.com/files/setup.sh && bash setup.sh <option>
```
###### Valid options
* poller: Deploys DataKitchen Observability Poller Agent.
* listener: Deploys DataKitchen Observability Events Hub Listener Agent.
* all: Deploys DataKitchen Observability Poller Agent and Events Hub Listener Agent.
* cleanup: Removes any DataKitchen Observability related resources if exists.
