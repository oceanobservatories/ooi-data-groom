# ooi-data-groom

## About

Data Groom is a framework for the scheduled execution of plugins that can be
created to "groom" the data. The framework provides methods for selecting and
inserting records into the Cassandra stream tables in response to updates to
the partition_metadata records of another stream. In this way, it can be used
to generate precomputed (or "groomed") virtual streams that are derived from
another stream that contains the actual raw data.

## Prerequisites

1. Create a conda virtual environment with the necessary packages:

```shell
conda create -n data_groom ion-functions cassandra-driver ooi-data apscheduler pyyaml psycopg2 pandas -c ooi -c conda-forge -y
```

## Running

The script `manage-data-groom` allows for starting and stopping the Data Groom
process for one plugin. The name of a config file is passed to this script and
that config file should contain the name of the plugin to run. The plugin
should be located the "plugins" directory.

```shell
./manage-data-groom start botpt_precompute
./manage-data-groom stop botpt_precompute
```
