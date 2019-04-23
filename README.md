opencensus_erlkaf
=====

Kafka statistics from erlkaf recorded by OpenCensus.

### Usage

Set the stats callback in `erlkaf` to the `opencensus_erlkaf` module:

``` erlang
[{erlkaf,
  [{global_client_options, [{bootstrap_servers, "localhost:9092"},
                            {client_id, <<"opencensus_erlkaf">>},
                            {delivery_report_only_error, true},
                            {delivery_report_callback, opencensus_erlkaf},
                            {statistics_interval_ms, 5000},
                            {stats_callback, opencensus_erlkaf}]}]}].
```

During your application's start up you must register the measures and subscribe to the views or nothing will be reported to OpenCensus/Prometheus:

``` erlang
opencensus_erlkaf:register_measures(),
Views = opencensus_erlkaf:default_views(),
[oc_stat_view:subscribe(View) || View <- Views],
```

Lastly, register the [OpenCensus Prometheus exporter](https://github.com/opencensus-beam/prometheus):

``` erlang
prometheus_registry:register_collector(oc_stat_exporter_prometheus)
```

### Run Tests

Docker compose is used to bring up kafka and zookeeper for the tests to use:

``` shell
$ docker-compose up
```

Then run the common test suite:

``` shell
$ rebar3 ct
```

