-module(opencensus_erlkaf).

-export([stats_callback/2,
         delivery_report/2,

         register_measures/0,
         default_views/0]).

-include_lib("erlkaf/include/erlkaf.hrl").

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
delivery_report(_DeliveryStatus, _Message) ->
    ok.

register_measures() ->
    [oc_stat_measure:new(Name, Description, Unit) || {Name, Description, Unit} <- measurements()].

default_views() ->
    views(topic, ?TOPIC_TAGS) ++ views(partition, ?PARTITION_TAGS).

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
                   [{'kafka/topic/batchsize/min', SizeMin},
                    {'kafka/topic/batchsize/max', SizeMax},
                    {'kafka/topic/batchsize/avg', SizeAvg},
                    {'kafka/topic/batchsize/sum', SizeSum},
                    {'kafka/topic/batchsize/cnt', SizeCnt},
                    {'kafka/topic/batchsize/stddev', SizeStdDev},
                    {'kafka/topic/batchsize/p50', SizeP50},
                    {'kafka/topic/batchsize/p75', SizeP75},
                    {'kafka/topic/batchsize/p90', SizeP90},
                    {'kafka/topic/batchsize/p99', SizeP99},
                    {'kafka/topic/batchsize/p99_99', SizeP99_99},

                    {'kafka/topic/batchcnt/min', CntMin},
                    {'kafka/topic/batchcnt/max', CntMax},
                    {'kafka/topic/batchcnt/avg', CntAvg},
                    {'kafka/topic/batchcnt/sum', CntSum},
                    {'kafka/topic/batchcnt/cnt', CntCnt},
                    {'kafka/topic/batchcnt/stddev', CntStdDev},
                    {'kafka/topic/batchcnt/p50', CntP50},
                    {'kafka/topic/batchcnt/p75', CntP75},
                    {'kafka/topic/batchcnt/p90', CntP90},
                    {'kafka/topic/batchcnt/p99', CntP99},
                    {'kafka/topic/batchcnt/p99_99', CntP99_99}]),
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
                   [{'kafka/partition/fetchq_cnt', FetchQCnt},
                    {'kafka/partition/fetchq_size', FetchQSize},
                    {'kafka/partition/consumer_lag', ConsumerLag},
                    {'kafka/partition/txmsgs', TxMsgs},
                    {'kafka/partition/txbytes', TxBytes},
                    {'kafka/partition/rxmsgs', RxMsgs},
                    {'kafka/partition/rxbytes', RxBytes},
                    {'kafka/partition/msgs', Msgs}]).
measurements() ->
    measurements(topic) ++ measurements(partition).

measurements(topic) ->
    [{'kafka/topic/batchsize/min', "Smallest batch size", bytes},
     {'kafka/topic/batchsize/max', "Largest batch size", bytes},
     {'kafka/topic/batchsize/avg', "Average batch size", bytes},
     {'kafka/topic/batchsize/sum', "Sum of batch sizes", bytes},
     {'kafka/topic/batchsize/cnt', "Number of batch sizes sampled", none},
     {'kafka/topic/batchsize/stddev', "Standard deviation of batch sizes", bytes},
     {'kafka/topic/batchsize/p50', "Batch size 50th percentile", bytes},
     {'kafka/topic/batchsize/p75', "Batch size 75th percentile", bytes},
     {'kafka/topic/batchsize/p90', "Batch size 90th percentile", bytes},
     {'kafka/topic/batchsize/p99', "Batch size 99th percentile", bytes},
     {'kafka/topic/batchsize/p99_99', "Batch size 99.99th percentile", bytes},
     {'kafka/topic/batchcnt/min', "Smallest batch message count", bytes},
     {'kafka/topic/batchcnt/max', "Largest batch message count", bytes},
     {'kafka/topic/batchcnt/avg', "Average batch message count", bytes},
     {'kafka/topic/batchcnt/sum', "Sum of batch message counts", bytes},
     {'kafka/topic/batchcnt/cnt', "Number of batch message counts sampled", none},
     {'kafka/topic/batchcnt/stddev', "Standard deviation of batch message counts", bytes},
     {'kafka/topic/batchcnt/p50', "batch message count 50th percentile", bytes},
     {'kafka/topic/batchcnt/p75', "batch message count 75th percentile", bytes},
     {'kafka/topic/batchcnt/p90', "batch message count 90th percentile", bytes},
     {'kafka/topic/batchcnt/p99', "batch message count 99th percentile", bytes},
     {'kafka/topic/batchcnt/p99_99', "batch message count 99.99th percentile", bytes}];
measurements(partition) ->
    [{'kafka/partition/fetchq_cnt', "Number of pre-fetched messages in fetch queue", none},
     {'kafka/partition/fetchq_size', "Bytes in fetchq", bytes},
     {'kafka/partition/consumer_lag',
      "Difference between hi_offset - max(app_offset, committed_offset", none},
     {'kafka/partition/txmsgs', "Total number of messages transmitted (produced)", bytes},
     {'kafka/partition/txbytes', "Total number of bytes transmitted for txmsgs", bytes},
     {'kafka/partition/rxmsgs',
      "Total number of messages consumed, not including ignored messages (due to offset, etc).", none},
     {'kafka/partition/rxbytes', "Total number of bytes received for rxmsgs", bytes},
     {'kafka/partition/msgs', "Total number of messages received (consumer, same as rxmsgs), "
      "or total number of messages produced (possibly not yet transmitted) (producer).", none}].
