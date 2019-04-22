-module(opencensus_erlkaf_SUITE).

-include_lib("stdlib/include/assert.hrl").

-compile([export_all]).

-define(UNTIL(X), (fun Until(I) when I =:= 10 -> erlang:error(fail);
                       Until(I) -> case X of true -> ok; false -> timer:sleep(500), Until(I+1) end end)(0)).

all() ->
    [stats_callback_test].

init_per_testcase(_, Config) ->
    application:load(opencensus),
    application:set_env(opencensus, stat, [{export_interval, 100},
                                           {exporters, [{?MODULE, self()}]}]),
     application:set_env(opencensus, send_interval_ms, 10),
    application:ensure_all_started(opencensus),

    opencensus_erlkaf:register_measures(),
    Views = opencensus_erlkaf:default_views(),
    [oc_stat_view:subscribe(View) || View <- Views],

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
    ok = erlkaf:create_producer(client_producer, ProducerConfig),
    ok = erlkaf:create_topic(client_producer, <<"topic-1">>, [{request_required_acks, 0}]),

    ClientId = client_consumer,
    GroupId = <<"erlkaf_consumer">>,
    ClientCfg = [],
    TopicConf = [{auto_offset_reset, smallest}],
    ok = erlkaf:create_consumer_group(ClientId, GroupId, [<<"topic-1">>],
                                      ClientCfg, TopicConf, oc_erlkaf_test_consumer, []),

    NumToProduce = 30,
    lists:foreach(fun(_) ->
                      ok = erlkaf:produce(client_producer, <<"topic-1">>,
                                          <<"key-1">>, <<"value-1">>),
                      timer:sleep(50)
                  end, lists:seq(1, NumToProduce)),

    collect(NumToProduce).

%%

collect(NumToCollect) ->
    collect(NumToCollect, 0).

collect(0, _Attempts) ->
    done;
collect(_, 50) ->
    ct:fail(timeout);
collect(NumToCollect, Attempts) ->
    receive
        {view_data, V} ->
            case [Cnt || #{name := "kafka/topic/batchcnt/cnt",
                           data := #{rows := [#{value := Cnt} | _]}} <- V] of
                [Cnt] ->
                    timer:sleep(200),
                    collect(NumToCollect - Cnt, Attempts+1);
                _ ->
                    timer:sleep(200),
                    collect(NumToCollect, Attempts+1)
            end
    end.


export(ViewData, Pid) ->
    Pid ! {view_data, ViewData}.
