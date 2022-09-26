# Stackable Operator for Apache Kafka

[![Build Actions Status](https://ci.stackable.tech/job/kafka%2doperator%2dit%2dnightly/badge/icon?subject=Integration%20Tests)](https://ci.stackable.tech/job/kafka%2doperator%2dit%2dnightly)
[![Maintenance](https://img.shields.io/badge/Maintained%3F-yes-green.svg)](https://GitHub.com/stackabletech/kafka-operator/graphs/commit-activity)

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

## Documentation

The documentation for this operator can be found at <https://docs.stackable.tech/kafka/stable/index.html>.

The documentation for all Stackable products can be found at <https://docs.stackable.tech>.

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

Want to get started? Read below.

## Behind the Scenes

It is written by [Stackable](https://www.stackable.tech) in Rust.

It uses kube-rs to talk to Kubernetes.

We test it [extensively](https://ci.stackable.tech/) using [Kuttl](https://kuttl.dev/)-powered integration tests on managed Kubernetes of multiple cloud platforms and our own bare-metal clusters.

## About the Stackable Data Platform

Stackable makes it easy to operate data applicationsin any Kubernetes cluster.

The data platform offers many operators, new ones being added continuously. All our operators are designed and built to be easily interconnected and to be consistent to work with.

Stackable GmbH is the company behind the Stackable Data Platform. Offering professional services, paid support plans and development.

We love open-source!

# Supported Platforms

We develop and test our operators for the following cloud platforms:

* Kubernetes 1.21-1.24
* Amazon EKS
* Google Kubernetes Engine (GKE) on GCP
* Microsoft AKS

We also use the following internally

* k3s

We are working to support

* OpenShift

## Our Other Operators

Stackable creates, maintains and offers support plans for all our operators, enabling you to work with your data, on your platform of choice.

* [Imagine](#)
* [Operators](#)
* [Here](#)
* [Imagine](#)
* [Apache Operators](#)
* [Apache Here](#)
* [Apache Imagine](#)
* [Apache Operators](#)
* [Apache Here](#)
* [Apache Imagine](#)
* [Apache Operators](#)
* [Apache Here](#)

## Contributing

Check out our [Contributor's Guide](https://docs.stackable.tech/home/stable/contributor/index.html). Reach out to us via contribute@stackable.tech if you have any questions, open a GitHub Issue or create a PR!

## License

[Open Software License version 3.0](./LICENSE).

## Support

Did we mention that you can pay the company for professional services? You can! Here is a [link](#).

## Related Reading

* [Something about Kuttl](#)
* [Something about working with Apache Kafka](#)
* [A cool demo using the Apache Kafka operator](#)