-module(opencensus_erlkaf).

-export([stats_callback/2,
         delivery_report/2,

         register_and_subscribe/0,
         register_measures/0,
         default_views/0]).

-include_lib("erlkaf/include/erlkaf.hrl").
-include_lib("kernel/include/logger.hrl").

%% Based on the stats found here:
%% https://github.com/postmates/pmpy/blob/master/pmpy/kafka/stats.py
%% which uses broker as a label but not sure it makes sense
-define(TOPIC_TAGS, [type, topic]). %% broker
-define(PARTITION_TAGS, [partition, leader_broker_id, fetch_state | ?TOPIC_TAGS]).

-spec stats_callback(client_id(), map()) -> ok.
stats_callback(_ClientId, #{<<"type">> := Type,
                            <<"topics">> := Topics}) ->
    maps:map(fun(Topic, Stats) ->
                 process_topic(Type, Topic, Stats)
             end, Topics),
    ok.

-spec delivery_report(ok | {error, any()}, #erlkaf_msg{}) -> ok.
delivery_report({error, Reason}, #erlkaf_msg{topic=Topic,
                                             partition=Partition}) ->
    ?LOG_ERROR("error publishing message to topic=~s partition=~p reason=~p", [Topic,
                                                                               Partition,
                                                                               Reason]),
    oc_stat:record(#{type => <<"producer">>,
                     topic => Topic,
                     partition => Partition}, [{'rdkafka/partition/delivery_error', 1}]);
delivery_report(ok, _Message) ->
    ok.

register_and_subscribe() ->
    opencensus_erlkaf:register_measures(),
    Views = opencensus_erlkaf:default_views(),
    [oc_stat_view:subscribe(View) || View <- Views].

register_measures() ->
    [oc_stat_measure:new(Name, Description, Unit) || {Name, Description, Unit} <- measurements()].

default_views() ->
    [#{name => "rdkafka/partition/delivery_error",
       description => "Number of errors reported for delivery attempts",
       tags => [partition | ?TOPIC_TAGS],
       measure => 'rdkafka/partition/delivery_error',
       aggregation => oc_stat_aggregation_count}
     | views(topic, ?TOPIC_TAGS) ++ views(partition, ?PARTITION_TAGS)].

views(Type, Tags) ->
    [#{name => atom_to_list(Name),
       description => Description,
       tags => Tags,
       measure => Name,
       aggregation => oc_stat_aggregation_latest} || {Name, Description, _} <- measurements(Type)].

process_topic(Type, Topic, #{<<"batchsize">> :=
                                 #{<<"min">> := SizeMin,
                                   <<"max">> := SizeMax,
                                   <<"avg">> := SizeAvg,
                                   <<"sum">> := SizeSum,
                                   <<"cnt">> := SizeCnt,
                                   <<"stddev">> := SizeStdDev,
                                   <<"p50">> := SizeP50,
                                   <<"p75">> := SizeP75,
                                   <<"p90">> := SizeP90,
                                   <<"p99">> := SizeP99,
                                   <<"p99_99">> := SizeP99_99},
                             <<"batchcnt">> :=
                                 #{<<"min">> := CntMin,
                                   <<"max">> := CntMax,
                                   <<"avg">> := CntAvg,
                                   <<"sum">> := CntSum,
                                   <<"cnt">> := CntCnt,
                                   <<"stddev">> := CntStdDev,
                                   <<"p50">> := CntP50,
                                   <<"p75">> := CntP75,
                                   <<"p90">> := CntP90,
                                   <<"p99">> := CntP99,
                                   <<"p99_99">> := CntP99_99},
                             <<"partitions">> := Partitions}) ->
    oc_stat:record(#{type => Type, topic => Topic},
                   [{'rdkafka/topic/batchsize/min', SizeMin},
                    {'rdkafka/topic/batchsize/max', SizeMax},
                    {'rdkafka/topic/batchsize/avg', SizeAvg},
                    {'rdkafka/topic/batchsize/sum', SizeSum},
                    {'rdkafka/topic/batchsize/cnt', SizeCnt},
                    {'rdkafka/topic/batchsize/stddev', SizeStdDev},
                    {'rdkafka/topic/batchsize/p50', SizeP50},
                    {'rdkafka/topic/batchsize/p75', SizeP75},
                    {'rdkafka/topic/batchsize/p90', SizeP90},
                    {'rdkafka/topic/batchsize/p99', SizeP99},
                    {'rdkafka/topic/batchsize/p99_99', SizeP99_99},

                    {'rdkafka/topic/batchcnt/min', CntMin},
                    {'rdkafka/topic/batchcnt/max', CntMax},
                    {'rdkafka/topic/batchcnt/avg', CntAvg},
                    {'rdkafka/topic/batchcnt/sum', CntSum},
                    {'rdkafka/topic/batchcnt/cnt', CntCnt},
                    {'rdkafka/topic/batchcnt/stddev', CntStdDev},
                    {'rdkafka/topic/batchcnt/p50', CntP50},
                    {'rdkafka/topic/batchcnt/p75', CntP75},
                    {'rdkafka/topic/batchcnt/p90', CntP90},
                    {'rdkafka/topic/batchcnt/p99', CntP99},
                    {'rdkafka/topic/batchcnt/p99_99', CntP99_99}]),
    maps:map(fun(Partition, Stats) ->
                 process_partition(Type, Topic, Partition, Stats)
             end, Partitions).

process_partition(Type, Topic, Partition, #{<<"leader">> := Leader,
                                            <<"fetch_state">> := FetchState,
                                            <<"fetchq_cnt">> := FetchQCnt,
                                            <<"fetchq_size">> := FetchQSize,
                                            <<"consumer_lag">> := ConsumerLag,
                                            <<"txmsgs">> := TxMsgs,
                                            <<"txbytes">> := TxBytes,
                                            <<"rxmsgs">> := RxMsgs,
                                            <<"rxbytes">> := RxBytes,
                                            <<"msgs">> := Msgs}) ->
    oc_stat:record(#{partition => Partition,
                     leader_broker_id => integer_to_binary(Leader),
                     fetch_state => FetchState,
                     type => Type,
                     topic => Topic},
                   [{'rdkafka/partition/fetchq_cnt', FetchQCnt},
                    {'rdkafka/partition/fetchq_size', FetchQSize},
                    {'rdkafka/partition/consumer_lag', ConsumerLag},
                    {'rdkafka/partition/txmsgs', TxMsgs},
                    {'rdkafka/partition/txbytes', TxBytes},
                    {'rdkafka/partition/rxmsgs', RxMsgs},
                    {'rdkafka/partition/rxbytes', RxBytes},
                    {'rdkafka/partition/msgs', Msgs}]).
measurements() ->
    [{'rdkafka/partition/delivery_error', "Number of errors reported for delivery attempts", none}
     | measurements(topic) ++ measurements(partition)].

measurements(topic) ->
    [{'rdkafka/topic/batchsize/min', "Smallest batch size", bytes},
     {'rdkafka/topic/batchsize/max', "Largest batch size", bytes},
     {'rdkafka/topic/batchsize/avg', "Average batch size", bytes},
     {'rdkafka/topic/batchsize/sum', "Sum of batch sizes", bytes},
     {'rdkafka/topic/batchsize/cnt', "Number of batch sizes sampled", none},
     {'rdkafka/topic/batchsize/stddev', "Standard deviation of batch sizes", bytes},
     {'rdkafka/topic/batchsize/p50', "Batch size 50th percentile", bytes},
     {'rdkafka/topic/batchsize/p75', "Batch size 75th percentile", bytes},
     {'rdkafka/topic/batchsize/p90', "Batch size 90th percentile", bytes},
     {'rdkafka/topic/batchsize/p99', "Batch size 99th percentile", bytes},
     {'rdkafka/topic/batchsize/p99_99', "Batch size 99.99th percentile", bytes},
     {'rdkafka/topic/batchcnt/min', "Smallest batch message count", bytes},
     {'rdkafka/topic/batchcnt/max', "Largest batch message count", bytes},
     {'rdkafka/topic/batchcnt/avg', "Average batch message count", bytes},
     {'rdkafka/topic/batchcnt/sum', "Sum of batch message counts", bytes},
     {'rdkafka/topic/batchcnt/cnt', "Number of batch message counts sampled", none},
     {'rdkafka/topic/batchcnt/stddev', "Standard deviation of batch message counts", bytes},
     {'rdkafka/topic/batchcnt/p50', "batch message count 50th percentile", bytes},
     {'rdkafka/topic/batchcnt/p75', "batch message count 75th percentile", bytes},
     {'rdkafka/topic/batchcnt/p90', "batch message count 90th percentile", bytes},
     {'rdkafka/topic/batchcnt/p99', "batch message count 99th percentile", bytes},
     {'rdkafka/topic/batchcnt/p99_99', "batch message count 99.99th percentile", bytes}];
measurements(partition) ->
    [{'rdkafka/partition/fetchq_cnt', "Number of pre-fetched messages in fetch queue", none},
     {'rdkafka/partition/fetchq_size', "Bytes in fetchq", bytes},
     {'rdkafka/partition/consumer_lag',
      "Difference between hi_offset - max(app_offset, committed_offset", none},
     {'rdkafka/partition/txmsgs', "Total number of messages transmitted (produced)", bytes},
     {'rdkafka/partition/txbytes', "Total number of bytes transmitted for txmsgs", bytes},
     {'rdkafka/partition/rxmsgs',
      "Total number of messages consumed, not including ignored messages (due to offset, etc).", none},
     {'rdkafka/partition/rxbytes', "Total number of bytes received for rxmsgs", bytes},
     {'rdkafka/partition/msgs', "Total number of messages received (consumer, same as rxmsgs), "
      "or total number of messages produced (possibly not yet transmitted) (producer).", none}].
