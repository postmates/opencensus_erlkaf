-module(oc_erlkaf_test_consumer).

-export([init/4,
         handle_message/2]).

-include_lib("erlkaf/include/erlkaf.hrl").

-record(state, {}).

init(Topic, Partition, Offset, Args) ->
    io:format("init topic: ~p partition: ~p offset: ~p args: ~p ~n", [
        Topic,
        Partition,
        Offset,
        Args
    ]),
    {ok, #state{}}.

handle_message(#erlkaf_msg{topic = Topic, partition = Partition, offset = Offset}, State) ->
    io:format("handle_message topic: ~p partition: ~p offset: ~p state: ~p ~n", [
        Topic,
        Partition,
        Offset,
        State
    ]),
    {ok, State}.
