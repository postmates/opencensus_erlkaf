-module(opencensus_erlkaf).

-export([stats_callback/2,
         delivery_report/2,

         register_measures/0,
         default_views/0]).

-include_lib("erlkaf/include/erlkaf.hrl").

-spec stats_callback(client_id(), list()) -> ok.
stats_callback(_ClientId, _Stats) ->
    io:format("Stats: ~p ~p~n", [_ClientId, _Stats]),
    ok.

-spec delivery_report(ok | {error, any()}, #erlkaf_msg{}) -> ok.
delivery_report(_DeliveryStatus, _Message) ->
    ok.

register_measures() ->
    oc_stat_measure:new('opencensus.io/http/server/received_bytes',
                        "Total bytes received in request body (not including headers). "
                        "This is uncompressed bytes.",
                        bytes).

default_views() ->
    [#{name => "opencensus.io/http/server/completed_count",
       description => "Count of HTTP requests completed",
       tags => [http_server_method, http_server_path, http_server_status],
       measure => 'opencensus.io/http/server/server_latency',
       aggregation => oc_stat_aggregation_count}].
