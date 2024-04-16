#!/bin/bash

deploy_agent() {
  # Ask user for input
  echo "Please provide connection details for $1 Agent"
  read -p "Enter EVENTS_API_HOST value: " events_api_host
  read -p "Enter EVENTS_API_KEY value: " events_api_key
  read -p "Enter EVENT_HUB_CONN_STR value: " event_hub_conn_str
  read -p "Enter EVENT_HUB_NAME value: " event_hub_name
  read -p "Enter AZURE_STORAGE_CONN_STR value: " azure_storage_conn_str
  read -p "Enter BLOB_CONTAINER_NAME value: " blob_container_name
  read -p "Enter EXTERNAL_PLUGINS_PATH value: " external_plugins_path
  read -p "Enter ENABLED_PLUGINS value: " enabled_plugins
  read -p "Enter PUBLISH_EVENTS value: " publish_events
  read -p "Enter image name and tag (registry/name:tag): " docker_image

  cat <<EOF > deploy.yaml
---
apiVersion: v1
kind: Secret
metadata:
  namespace: datakitchen
  name: $1-secret
type: Opaque
stringData:
  EVENTS_API_HOST: '$events_api_host'
  EVENTS_API_KEY: '$events_api_key'
  EVENT_HUB_CONN_STR: '$event_hub_conn_str'
  EVENT_HUB_NAME: '$event_hub_name'
  AZURE_STORAGE_CONN_STR: '$azure_storage_conn_str'
  BLOB_CONTAINER_NAME: '$blob_container_name'
  EXTERNAL_PLUGINS_PATH: '$external_plugins_path'
  ENABLED_PLUGINS: '$enabled_plugins'
  PUBLISH_EVENTS: '$publish_events'
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: $1-agent
  namespace: datakitchen
  labels:
    app: $1-agent
spec:
  replicas: 1
  selector:
    matchLabels:
      app: $1-agent
  template:
    metadata:
      labels:
        app: $1-agent
    spec:
      containers:
      - name: $1-agent
        image: $2
        imagePullPolicy: IfNotPresent
        env:
        - name: EVENTS_API_HOST
          valueFrom:
            secretKeyRef:
              name: $1-secret
              key: EVENTS_API_HOST
        - name: EVENTS_API_KEY
          valueFrom:
            secretKeyRef:
              name: $1-secret
              key: EVENTS_API_KEY
        - name: EVENT_HUB_CONN_STR
          valueFrom:
            secretKeyRef:
              name: $1-secret
              key: EVENT_HUB_CONN_STR
        - name: EVENT_HUB_NAME
          valueFrom:
            secretKeyRef:
              name: $1-secret
              key: EVENT_HUB_NAME
        - name: AZURE_STORAGE_CONN_STR
          valueFrom:
            secretKeyRef:
              name: $1-secret
              key: AZURE_STORAGE_CONN_STR
        - name: BLOB_CONTAINER_NAME
          valueFrom:
            secretKeyRef:
              name: $1-secret
              key: BLOB_CONTAINER_NAME
        - name: EXTERNAL_PLUGINS_PATH
          valueFrom:
            secretKeyRef:
              name: $1-secret
              key: EXTERNAL_PLUGINS_PATH
        - name: ENABLED_PLUGINS
          valueFrom:
            secretKeyRef:
              name: $1-secret
              key: ENABLED_PLUGINS
        - name: PUBLISH_EVENTS
          valueFrom:
            secretKeyRef:
              name: $1-secret
              key: PUBLISH_EVENTS
EOF

kubectl apply -f deploy.yaml


}

if [ "$1" == "poller" ]; then
    kubectl create ns datakitchen > /dev/null 2>&1
    image="$docker_image"
    kubectl delete secret $1-secret -n datakitchen 2> /dev/null
    kubectl delete deploy $1-agent -n datakitchen 2> /dev/null
    echo "#### Deploying DataKitchen's $1 Agent ##### "
    deploy_agent $1 $image

elif [ "$1" == "listener" ]; then
    kubectl create ns datakitchen > /dev/null 2>&1
    image="$docker_image"
    kubectl delete secret $1-secret -n datakitchen 2> /dev/null
    kubectl delete deploy $1-agent -n datakitchen 2> /dev/null
    echo "##### Deploying DataKitchen's $1 Agent ##### "
    deploy_agent $1 $image

elif [ "$1" == "cleanup" ]; then
    kubectl create ns datakitchen > /dev/null 2>&1
    kubectl delete secret poller-secret listener-secret -n datakitchen 2> /dev/null
    kubectl delete deploy poller-agent listener-agent -n datakitchen 2> /dev/null

else
    echo "Usage: $0 [poller|listener|all]"
    exit 1
fi
