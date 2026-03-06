# freifunk-metrics-harvester

freifunk-metrics-harvester collects performance and operational metrics from Freifunk Gluon nodes.

A central collector runs on a server and actively retrieves metrics from nodes across the network.
The collected data is stored locally for later analysis and pushed to InfluxDB for time-series monitoring.

The goal of this project is to detect network performance bottlenecks from the end-user perspective
as early as possible across the entire Freifunk network.

This repository also contains the node-side code that runs on Gluon devices to expose or generate
the required metrics.

## Architecture

Freifunk Node (Gluon)
        │
        │ metrics / tests
        ▼
Central Collector
        │
        ├── Local metric storage
        │
        └── InfluxDB (time series metrics)

## Components

Server-side collector
- retrieves metrics from Freifunk nodes
- runs network performance tests
- stores raw metrics locally
- pushes processed metrics to InfluxDB

Node-side components
- lightweight scripts running on Gluon nodes
- expose metrics and measurement endpoints
- support network performance measurements

## Goal

Provide network-wide observability for Freifunk deployments in order to:

- identify throughput bottlenecks
- detect degraded links early
- observe network performance trends
- support future automated analysis

## Status

Early stage development.

## About

This project is developed for and used by Freifunk Nordhessen e.V.

https://www.freifunk-nordhessen.de

## License

MIT License
Copyright (c) 2026 Andreas W. Ditze
