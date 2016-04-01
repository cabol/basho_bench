-module(basho_bench_driver_ets).

-export([new/1,
         run/4]).

new(_Id) ->
    %EtsTable = ets:new(basho_bench, [public, named_table]),
    {ok, my_ets}.

run(get, _KeyGen, _ValueGen, EtsTable) ->
    Start = erlang:phash2(os:timestamp()),%KeyGen(),
    case ets:lookup(EtsTable, Start) of
        [] ->
            {ok, EtsTable};
        [{_Key, _Val}] ->
            {ok, EtsTable};
        Error ->
            {error, Error, EtsTable}
    end;

run(put, _KeyGen, ValueGen, EtsTable) ->
    Object = {erlang:phash2(os:timestamp()), ValueGen()},
    ets:insert(EtsTable, Object),
    {ok, EtsTable};

run(delete, _KeyGen, _ValueGen, EtsTable) ->
    Start = erlang:phash2(os:timestamp()),%KeyGen(),
    ets:delete(EtsTable, Start),
    {ok, EtsTable}.
