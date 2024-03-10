-module(rabbit_amqp_management).

-include("rabbit_amqp.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-export([handle_request/4]).

-spec handle_request(binary(), rabbit_types:vhost(), rabbit_types:user(), pid()) -> iolist().
handle_request(Request, Vhost, User, ConnectionPid) ->
    ReqSections = amqp10_framing:decode_bin(Request),
    ?DEBUG("~s Inbound request:~n  ~tp",
           [?MODULE, [amqp10_framing:pprint(Section) || Section <- ReqSections]]),

    {#'v1_0.properties'{
        message_id = MessageId,
        to = {utf8, HttpRequestTarget},
        subject = {utf8, HttpMethod},
        %% see Link Pair CS 01 §2.1
        %% https://docs.oasis-open.org/amqp/linkpair/v1.0/cs01/linkpair-v1.0-cs01.html#_Toc51331305
        reply_to = {utf8, <<"$me">>}},
     ReqBody
    } = decode_req(ReqSections, {undefined, undefined}),

    {StatusCode,
     RespAppProps0,
     RespBody} = try {PathSegments, QueryMap} = parse_uri(HttpRequestTarget),
                     handle_http_req(HttpMethod,
                                     PathSegments,
                                     QueryMap,
                                     ReqBody,
                                     Vhost,
                                     User,
                                     ConnectionPid)
                 catch throw:{StatusCode0, Explanation} ->
                           {StatusCode0, [], {utf8, unicode:characters_to_binary(Explanation)}}
                 end,

    RespProps = #'v1_0.properties'{
                   subject = {utf8, StatusCode},
                   %% "To associate a response with a request, the correlation-id value of the response
                   %% properties MUST be set to the message-id value of the request properties."
                   %% [HTTP over AMQP WD 06 §5.1]
                   correlation_id = MessageId},
    RespAppProps = #'v1_0.application_properties'{
                      content = [{{utf8, <<"http:response">>}, {utf8, <<"1.1">>}} | RespAppProps0]},
    RespDataSect = #'v1_0.amqp_value'{content = RespBody},
    RespSections = [RespProps, RespAppProps, RespDataSect],
    [amqp10_framing:encode_bin(Sect) || Sect <- RespSections].

%%TODO
%% If new queue / exchange gets created, return 201 with content.
%% If queue with same fields already exists, return 200 including the queue content.
%% If queue / exchange with other fields exists, return 409 with explanation about which fields diff.
handle_http_req(<<"PUT">>,
                [<<"queues">>, QNameBinQ],
                _Query,
                ReqPayload,
                Vhost,
                #user{username = Username},
                ConnPid) ->
    #{durable := Durable,
      auto_delete := AutoDelete,
      exclusive := Exclusive,
      arguments := QArgs
     } = decode_queue(ReqPayload),
    QType = rabbit_amqqueue:get_queue_type(QArgs),
    QNameBin = uri_string:unquote(QNameBinQ),
    QName = rabbit_misc:r(Vhost, queue, QNameBin),
    Owner = case Exclusive of
                true -> ConnPid;
                false -> none
            end,
    Q0 = amqqueue:new(QName, none, Durable, AutoDelete, Owner,
                      QArgs, Vhost, #{user => Username}, QType),
    {new, _Q} = rabbit_queue_type:declare(Q0, node()),
    {<<"201">>, [], null};

handle_http_req(<<"PUT">>,
                [<<"exchanges">>, XNameBinQ],
                _Query,
                ReqPayload,
                Vhost,
                #user{username = Username},
                _ConnPid) ->
    #{type := XType,
      durable := Durable,
      auto_delete := AutoDelete,
      internal := Internal,
      arguments := XArgs
     } = decode_exchange(ReqPayload),
    XNameBin = uri_string:unquote(XNameBinQ),
    XName = rabbit_misc:r(Vhost, exchange, XNameBin),
    CheckedXType = rabbit_exchange:check_type(XType),
    _X = rabbit_exchange:declare(XName,
                                 CheckedXType,
                                 Durable,
                                 AutoDelete,
                                 Internal,
                                 XArgs,
                                 Username),
    {<<"201">>, [], null};

handle_http_req(<<"DELETE">>,
                [<<"queues">>, QNameBinQ, <<"messages">>],
                _Query,
                null,
                Vhost,
                _User,
                ConnPid) ->
    QNameBin = uri_string:unquote(QNameBinQ),
    QName = rabbit_misc:r(Vhost, queue, QNameBin),
    {ok, NumMsgs} = rabbit_amqqueue:with_exclusive_access_or_die(
                      QName, ConnPid,
                      fun (Q) ->
                              rabbit_queue_type:purge(Q)
                      end),
    RespPayload = {map, [{{utf8, <<"message_count">>}, {ulong, NumMsgs}}]},
    {<<"200">>, [], RespPayload};

handle_http_req(<<"DELETE">>,
                [<<"queues">>, QNameBinQ],
                _Query,
                null,
                Vhost,
                #user{username = Username},
                ConnPid) ->
    QNameBin = uri_string:unquote(QNameBinQ),
    QName = rabbit_misc:r(Vhost, queue, QNameBin),
    {ok, NumMsgs} = rabbit_amqqueue:delete_with(QName, ConnPid, false, false, Username, true),
    RespPayload = {map, [{{utf8, <<"message_count">>}, {ulong, NumMsgs}}]},
    {<<"200">>, [], RespPayload};

handle_http_req(<<"DELETE">>,
                [<<"exchanges">>, XNameBinQ],
                _Query,
                null,
                Vhost,
                #user{username = Username},
                _ConnPid) ->
    XNameBin = uri_string:unquote(XNameBinQ),
    XName = rabbit_misc:r(Vhost, exchange, XNameBin),
    ok = case rabbit_exchange:delete(XName, false, Username) of
             ok ->
                 ok;
             {error, not_found} ->
                 ok
                 %% %% TODO return deletion failure
                 %% {error, in_use} ->
         end,
    {<<"204">>, [], null};

handle_http_req(<<"POST">>,
                [<<"bindings">>],
                _Query,
                ReqPayload,
                Vhost,
                #user{username = Username},
                _ConnPid) ->
    #{source := SrcXNameBin,
      binding_key := BindingKey,
      arguments := Args} = BindingMap = decode_binding(ReqPayload),
    {DstKind, DstNameBin} = case BindingMap of
                                #{destination_queue := Bin} ->
                                    {queue, Bin};
                                #{destination_exchange := Bin} ->
                                    {exchange, Bin}
                            end,
    SrcXName = rabbit_misc:r(Vhost, exchange, SrcXNameBin),
    DstName = rabbit_misc:r(Vhost, DstKind, DstNameBin),
    Binding = #binding{source      = SrcXName,
                       destination = DstName,
                       key         = BindingKey,
                       args        = Args},
    %%TODO If the binding already exists, return 303 with location.
    ok = rabbit_binding:add(Binding, Username),
    Location = compose_binding_uri(SrcXNameBin, DstKind, DstNameBin, BindingKey, Args),
    AppProps = [{{utf8, <<"location">>}, {utf8, Location}}],
    {<<"201">>, AppProps, null};

handle_http_req(<<"DELETE">>,
                [<<"bindings">>, BindingSegment],
                _Query,
                null,
                Vhost,
                #user{username = Username},
                _ConnPid) ->
    {SrcXNameBin, DstKind, DstNameBin, BindingKey, ArgsHash} = decode_binding_path_segment(BindingSegment),
    SrcXName = rabbit_misc:r(Vhost, exchange, SrcXNameBin),
    DstName = rabbit_misc:r(Vhost, DstKind, DstNameBin),
    Bindings = rabbit_binding:list_for_source_and_destination(SrcXName, DstName),
    case search_binding(BindingKey, ArgsHash, Bindings) of
        {value, Binding} ->
            ok = rabbit_binding:remove(Binding, Username);
        false ->
            ok
    end,
    {<<"204">>, [], null};

handle_http_req(<<"GET">>,
                [<<"bindings">>],
                QueryMap = #{<<"src">> := SrcXNameBin,
                             <<"key">> := Key},
                null,
                Vhost,
                _User,
                _ConnPid) ->
    {DstKind, DstNameBin} = case QueryMap of
                                #{<<"dste">> := DstX} ->
                                    {exchange, DstX};
                                #{<<"dstq">> := DstQ} ->
                                    {queue, DstQ};
                                _ ->
                                    throw(<<"400">>, "missing 'dste' or 'dstq' in query: ~tp", QueryMap)
                            end,
    SrcXName = rabbit_misc:r(Vhost, exchange, SrcXNameBin),
    DstName = rabbit_misc:r(Vhost, DstKind, DstNameBin),
    Bindings0 = rabbit_binding:list_for_source_and_destination(SrcXName, DstName),
    Bindings = [B || B = #binding{key = K} <- Bindings0, K =:= Key],
    RespPayload = encode_bindings(Bindings),
    {<<"200">>, [], RespPayload}.

decode_queue({map, KVList}) ->
    M = lists:foldl(
          fun({{utf8, <<"durable">>}, V}, Acc)
                when is_boolean(V) ->
                  Acc#{durable => V};
             ({{utf8, <<"exclusive">>}, V}, Acc)
               when is_boolean(V) ->
                  Acc#{exclusive => V};
             ({{utf8, <<"auto_delete">>}, V}, Acc)
               when is_boolean(V) ->
                  Acc#{auto_delete => V};
             ({{utf8, <<"arguments">>}, {map, List}}, Acc) ->
                  Args = lists:map(fun({{utf8, Key = <<"x-", _/binary>>}, {Type, Val}})
                                         when Type =:= utf8 orelse
                                              Type =:= symbol ->
                                           {Key, longstr, Val};
                                      (Arg) ->
                                           throw(<<"400">>,
                                                 "unsupported queue argument ~tp",
                                                 [Arg])
                                   end, List),
                  Acc#{arguments => Args};
             (Prop, _Acc) ->
                  throw(<<"400">>, "bad queue property ~tp", [Prop])
          end, #{}, KVList),
    Defaults = #{durable => true,
                 exclusive => false,
                 auto_delete => false,
                 arguments => []},
    maps:merge(Defaults, M).

decode_exchange({map, KVList}) ->
    M = lists:foldl(
          fun({{utf8, <<"durable">>}, V}, Acc)
                when is_boolean(V) ->
                  Acc#{durable => V};
             ({{utf8, <<"auto_delete">>}, V}, Acc)
               when is_boolean(V) ->
                  Acc#{auto_delete => V};
             ({{utf8, <<"type">>}, {utf8, V}}, Acc) ->
                  Acc#{type => V};
             ({{utf8, <<"internal">>}, V}, Acc)
               when is_boolean(V) ->
                  Acc#{internal => V};
             ({{utf8, <<"arguments">>}, {map, List}}, Acc) ->
                  Args = lists:map(fun({{utf8, Key = <<"x-", _/binary>>}, {Type, Val}})
                                         when Type =:= utf8 orelse
                                              Type =:= symbol ->
                                           {Key, longstr, Val};
                                      (Arg) ->
                                           throw(<<"400">>,
                                                 "unsupported exchange argument ~tp",
                                                 [Arg])
                                   end, List),
                  Acc#{arguments => Args};
             (Prop, _Acc) ->
                  throw(<<"400">>, "bad exchange property ~tp", [Prop])
          end, #{}, KVList),
    Defaults = #{durable => true,
                 auto_delete => false,
                 type => <<"direct">>,
                 internal => false,
                 arguments => []},
    maps:merge(Defaults, M).

decode_binding({map, KVList}) ->
    lists:foldl(
      fun({{utf8, <<"source">>}, {utf8, V}}, Acc) ->
              Acc#{source => V};
         ({{utf8, <<"destination_queue">>}, {utf8, V}}, Acc) ->
              Acc#{destination_queue => V};
         ({{utf8, <<"destination_exchange">>}, {utf8, V}}, Acc) ->
              Acc#{destination_exchange => V};
         ({{utf8, <<"binding_key">>}, {utf8, V}}, Acc) ->
              Acc#{binding_key => V};
         ({{utf8, <<"arguments">>}, {map, List}}, Acc) ->
              Args = lists:map(fun({{T, Key}, TypeVal})
                                     when T =:= utf8 orelse
                                          T =:= symbol ->
                                       mc_amqpl:to_091(Key, TypeVal);
                                  (Arg) ->
                                       throw(<<"400">>,
                                             "unsupported binding argument ~tp",
                                             [Arg])
                               end, List),
              Acc#{arguments => Args};
         (Field, _Acc) ->
              throw(<<"400">>, "bad binding field ~tp", [Field])
      end, #{}, KVList).

encode_bindings(Bindings) ->
    Bs = lists:map(
           fun(#binding{source = #resource{name = SrcName},
                        key = BindingKey,
                        destination = #resource{kind = DstKind,
                                                name = DstName},
                        args = Args091}) ->
                   DstKindBin = case DstKind of
                                    queue -> <<"queue">>;
                                    exchange -> <<"exchange">>
                                end,
                   Args = [{{utf8, Key}, mc_amqpl:from_091(Type, Val)}
                           || {Key, Type, Val} <- Args091],
                   Location = compose_binding_uri(
                                SrcName, DstKind, DstName, BindingKey, Args091),
                   KVList = [
                             {{utf8, <<"source">>}, {utf8, SrcName}},
                             {{utf8, DstKindBin}, {utf8, DstName}},
                             {{utf8, <<"binding_key">>}, {utf8, BindingKey}},
                             {{utf8, <<"arguments">>}, {map, Args}},
                             {{utf8, <<"location">>}, {utf8, Location}}
                            ],
                   {map, KVList}
           end, Bindings),
    {list, Bs}.

decode_req([], Acc) ->
    Acc;
decode_req([#'v1_0.properties'{} = P | Rem], Acc) ->
    decode_req(Rem, setelement(1, Acc, P));
decode_req([#'v1_0.amqp_value'{content = C} | Rem], Acc) ->
    decode_req(Rem, setelement(2, Acc, C));
decode_req([_IgnoreSection | Rem], Acc) ->
    decode_req(Rem, Acc).

parse_uri(Uri) ->
    case uri_string:normalize(Uri, [return_map]) of
        UriMap = #{path := Path} ->
            [<<>> | Segments] = binary:split(Path, <<"/">>, [global]),
            QueryMap = case maps:find(query, UriMap) of
                           {ok, Query} ->
                               case uri_string:dissect_query(Query) of
                                   QueryList
                                     when is_list(QueryList) ->
                                       maps:from_list(QueryList);
                                   {error, Atom, Term} ->
                                       throw(<<"400">>,
                                             "failed to dissect query '~ts': ~s ~tp",
                                             [Query, Atom, Term])
                               end;
                           error ->
                               #{}
                       end,
            {Segments, QueryMap};
        {error, Atom, Term} ->
            throw(<<"400">>,
                  "failed to normalize URI '~ts': ~s ~tp",
                  [Uri, Atom, Term])
    end.

compose_binding_uri(Src, DstKind, Dst, Key, Args) ->
    SrcQ = uri_string:quote(Src),
    DstQ = uri_string:quote(Dst),
    KeyQ = uri_string:quote(Key),
    ArgsHash = args_hash(Args),
    DstChar = case DstKind of
                  exchange -> $e;
                  queue -> $q
              end,
    <<"/bindings/src=", SrcQ/binary,
      ";dst", DstChar, $=, DstQ/binary,
      ";key=", KeyQ/binary,
      ";args=", ArgsHash/binary>>.

decode_binding_path_segment(Segment) ->
    MP = persistent_term:get(mp_binding_uri_path_segment),
    {match, [SrcQ, DstKindChar, DstQ, KeyQ, ArgsHash]} =
    re:run(Segment, MP, [{capture, all_but_first, binary}]),
    Src = uri_string:unquote(SrcQ),
    Dst = uri_string:unquote(DstQ),
    Key = uri_string:unquote(KeyQ),
    DstKind = case DstKindChar of
                  <<$e>> -> exchange;
                  <<$q>> -> queue
              end,
    {Src, DstKind, Dst, Key, ArgsHash}.

search_binding(BindingKey, ArgsHash, Bindings) ->
    lists:search(fun(#binding{key = Key,
                              args = Args})
                       when Key =:= BindingKey ->
                         args_hash(Args) =:= ArgsHash;
                    (_) ->
                         false
                 end, Bindings).

-spec args_hash(rabbit_framing:amqp_table()) -> binary().
args_hash([]) ->
    <<>>;
args_hash(Args)
  when is_list(Args) ->
    Bin = <<(erlang:phash2(Args, 1 bsl 32)):32>>,
    base64:encode(Bin, #{mode => urlsafe,
                         padding => false}).

-spec throw(binary(), io:format(), [term()]) -> no_return().
throw(StatusCode, Format, Data) ->
    Explanation = lists:flatten(io_lib:format(Format, Data)),
    rabbit_log:warning(Explanation),
    throw({StatusCode, Explanation}).
