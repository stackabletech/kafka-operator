This is a Kubernetes operator to manage [Apache Kafka](https://kafka.apache.org/) clusters.

It's built to play well with all other [Stackable operators](#other-operators), and play well with [many different Kubernetes environments](#supported-platforms).

## Installation

You can install the operator using [stackablectl or helm](https://docs.stackable.tech/kafka/stable/getting_started/installation.html).

## Getting Started

You can follow this [tutorial](https://docs.stackable.tech/kafka/stable/getting_started/first_steps.html) to create a Kafka cluster with three replicas.

{% with operator_name="kafka" -%}
  {% include "partials/borrowed/documentation.md" %}
{%- endwith %}

## What Does This Do?

Operator looks at custom resources in the cluster, and makes sure apps are started according to those custom resources.

If those resources change, the operator makes sure that the right actions are taken so the app is the way you want it to be.

Here's what a custom resource for a Kafka cluster can look like:

```
---
apiVersion: kafka.stackable.tech/v1alpha1
kind: KafkaCluster
metadata:
  name: simple-kafka
spec:
  version: 3.2.0-stackable0.1.0
  zookeeperConfigMapName: simple-kafka-znode
  config:
    tls: null
  brokers:
    roleGroups:
      default:
        replicas: 3
```

## Behind the Scenes

This operator is written by [Stackable](https://www.stackable.tech) in Rust.

It uses kube-rs to talk to Kubernetes.

We test it [extensively](https://ci.stackable.tech/) using [Kuttl](https://kuttl.dev/)-powered integration tests on managed Kubernetes of multiple cloud platforms and our own bare-metal clusters.