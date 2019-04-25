-module(oc_erlkaf_test_consumer).

-export([init/4,
         handle_message/2]).

-include_lib("erlkaf/include/erlkaf.hrl").

-record(state, {}).

init(_Topic, _Partition, _Offset, _Args) ->
    {ok, #state{}}.

handle_message(#erlkaf_msg{}, State) ->
    {ok, State}.
