-module(opencensus_erlkaf_SUITE).

-include_lib("stdlib/include/assert.hrl").

-compile([export_all]).

all() ->
    [stats_callback_test].

init_per_testcase(_, Config) ->
    application:load(opencensus),
    application:set_env(opencensus, stat, [{export_interval, 100},
                                           {exporters, [{?MODULE, self()}]}]),
    application:set_env(opencensus, send_interval_ms, 10),
    application:ensure_all_started(opencensus),

    opencensus_erlkaf:register_and_subscribe(),

    application:load(erlkaf),
    application:set_env(erlkaf, global_client_options, [{bootstrap_servers, "localhost:9092"},
                                                        {statistics_interval_ms, 100},
                                                        {stats_callback, opencensus_erlkaf}]),
    application:ensure_all_started(opencensus_erlkaf),

    Config.

end_per_testcase(_, _Config) ->
    application:stop(opencensus_erlkaf),
    ok.

stats_callback_test(_Config) ->
    ProducerConfig = [],
    Topic = re:replace(base64:encode(crypto:strong_rand_bytes(10)),"\\W","",[global,{return,binary}]),
    ok = erlkaf:create_producer(client_producer, ProducerConfig),
    ok = erlkaf:create_topic(client_producer, Topic, [{request_required_acks, 0}]),

    ClientId = client_consumer,
    GroupId = re:replace(base64:encode(crypto:strong_rand_bytes(10)),"\\W","",[global,{return,binary}]),
    ClientCfg = [],
    TopicConf = [], %% [{auto_offset_reset, beginning}],
    ok = erlkaf:create_consumer_group(ClientId, GroupId, [Topic],
                                      ClientCfg, TopicConf, oc_erlkaf_test_consumer, []),

    NumToProduce = 30,
    lists:foreach(fun(_) ->
                      ok = erlkaf:produce(client_producer, Topic,
                                          <<"key-1">>, <<"value-1">>),
                          timer:sleep(50)
                  end, lists:seq(1, NumToProduce)),

    collect(NumToProduce).

%%

collect(NumToCollect) ->
    receive
        {view_data, V} ->
            AllViewNames = lists:sort([Name || #{name := Name} <- opencensus_erlkaf:default_views()]),
            ReportedViews = lists:sort([Name || #{name := Name} <- V]),
            ?assertEqual(AllViewNames, ReportedViews)
    end.


export(ViewData, Pid) ->
    Pid ! {view_data, ViewData}.
