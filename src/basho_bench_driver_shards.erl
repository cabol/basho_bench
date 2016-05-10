-module(basho_bench_driver_shards).

-export([new/1,
  run/4]).

new(_Id) ->
  %Name = shards_owner:shard_name(shards, erlang:phash2(os:timestamp())),
  %State = shards:new(Name, [], 8),
  State = {xyz, shards:state(xyz)},
  {ok, State}.

run(get, _KeyGen, _ValueGen, {Table, _S} = State) ->
  Start = erlang:phash2(os:timestamp()),%KeyGen(),
  case shards:lookup(Table, Start) of
    [] ->
      {ok, State};
    [{_Key, _Val}] ->
      {ok, State};
    Error ->
      {error, Error, State}
  end;

run(put, _KeyGen, ValueGen, {Table, _S} = State) ->
  Object = {erlang:phash2(os:timestamp()), ValueGen()},
  shards:insert(Table, Object),
  {ok, State};

run(delete, _KeyGen, _ValueGen, {Table, _S} = State) ->
  Start = erlang:phash2(os:timestamp()),%KeyGen(),
  shards:delete(Table, Start),
  {ok, State}.
