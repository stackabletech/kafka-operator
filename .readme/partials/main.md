This is a Kubernetes Operator to manage [Apache Kafka](https://kafka.apache.org/) clusters.

Looking to dive in? It's easy to get started with the Stackable data platform using `stackablectl`:

```
stackablectl spin up a demo
```

Read more in [link to fancy demo using stackablectl](#).

## Installation

To get the best experience, we recommended way to get started with Stackable is through the `stackablectl` tool.

Read [the instructions below](#) to get started.

## Getting Started

To create a Kafka cluster with three nodes, you can follow this [tutorial](https://docs.stackable.tech/kafka/stable/getting_started/first_steps.html).

Otherwise, give it a try with the [stackablectl](https://docs.stackable.tech/home/stable/getting_started.html) CLI tool!

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