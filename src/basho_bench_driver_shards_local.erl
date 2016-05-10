-module(basho_bench_driver_shards_local).

-export([new/1,
  run/4]).

new(_Id) ->
  %Name = shards_owner:shard_name(shards, erlang:phash2(os:timestamp())),
  %State = shards:new(Name, [], 8),
  State = {xyz, shards:state(xyz)},
  {ok, State}.

run(get, _KeyGen, _ValueGen, {Table, S} = State) ->
  Start = erlang:phash2(os:timestamp()),%KeyGen(),
  case shards_local:lookup(Table, Start, S) of
    [] ->
      {ok, State};
    [{_Key, _Val}] ->
      {ok, State};
    Error ->
      {error, Error, State}
  end;

run(put, _KeyGen, ValueGen, {Table, S} = State) ->
  Object = {erlang:phash2(os:timestamp()), ValueGen()},
  shards_local:insert(Table, Object, S),
  {ok, State};

run(delete, _KeyGen, _ValueGen, {Table, S} = State) ->
  Start = erlang:phash2(os:timestamp()),%KeyGen(),
  shards_local:delete(Table, Start, S),
  {ok, State}.
