-module(rabbit_amqp_management).

-include("rabbit_amqp.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-export([process_request/4]).
-export([args_hash/1]).

%% An HTTP message mapped to AMQP using projected mode
%% [HTTP over AMQP Working Draft 06 §4.1]
-record(msg,
        {
         properties :: #'v1_0.properties'{},
         application_properties :: list(),
         data = [] :: [#'v1_0.data'{}]
        }).

-spec process_request(binary(), rabbit_types:vhost(), rabbit_types:user(), pid()) -> iolist().
process_request(Request, Vhost, User, ConnectionPid) ->
    ReqSections = amqp10_framing:decode_bin(Request),
    ?DEBUG("~s Inbound request:~n  ~tp",
           [?MODULE, [amqp10_framing:pprint(Section) || Section <- ReqSections]]),
    #msg{properties = #'v1_0.properties'{
                         message_id = MessageId,
                         to = {utf8, HttpRequestTarget},
                         subject = {utf8, HttpMethod},
                         %% see Link Pair CS 01 §2.1
                         %% https://docs.oasis-open.org/amqp/linkpair/v1.0/cs01/linkpair-v1.0-cs01.html#_Toc51331305
                         reply_to = {utf8, <<"$me">>},
                         content_type = MaybeContentType},
         application_properties = _OtherHttpHeaders,
         data = ReqBody
        } = decode_req(ReqSections, {undefined, undefined, []}),
    ReqPayload = amqp10_framing:decode_bin(list_to_binary(ReqBody)),
    {RespProps0,
     RespAppProps0 = #'v1_0.application_properties'{content = C},
     RespPayload} = process_http_request(HttpMethod,
                                         HttpRequestTarget,
                                         MaybeContentType,
                                         ReqPayload,
                                         Vhost,
                                         User,
                                         ConnectionPid),
    RespProps = RespProps0#'v1_0.properties'{
                             %% "To associate a response with a request, the correlation-id value of the response
                             %% properties MUST be set to the message-id value of the request properties."
                             %% [HTTP over AMQP WD 06 §5.1]
                             correlation_id = MessageId},
    RespAppProps = RespAppProps0#'v1_0.application_properties'{
                                   content = [{{utf8, <<"http:response">>}, {utf8, <<"1.1">>}} | C]},
    RespDataSect = #'v1_0.data'{content = iolist_to_binary(amqp10_framing:encode_bin(RespPayload))},
    RespSections = [RespProps, RespAppProps, RespDataSect],
    [amqp10_framing:encode_bin(Sect) || Sect <- RespSections].

process_http_request(<<"POST">>,
                     <<"/$management/entities">>,
                     {symbol, <<"application/amqp-management+amqp", _OptionalType/binary>>},
                     [ReqPayload],
                     Vhost,
                     #user{username = Username},
                     _) ->
    %%TODO
    %% If new queue / exchange gets created, return 201 with content.
    %% If queue with same fields already exists, return 200 including the queue content.
    %% If queue / exchange with other fields exists, return 409 with explanation about which fields diff.
    {Type, Id, Self1, Target} =
    case decode_entity(ReqPayload) of
        #{type := <<"queue">> = Type0,
          name := QNameBin,
          durable := Durable,
          auto_delete := AutoDelete,
          owner := Owner,
          arguments := QArgs} ->
            QType = rabbit_amqqueue:get_queue_type(QArgs),
            QName = rabbit_misc:r(Vhost, queue, QNameBin),
            Q0 = amqqueue:new(QName, none, Durable, AutoDelete, Owner,
                              QArgs, Vhost, #{user => Username}, QType),
            {new, _Q} = rabbit_queue_type:declare(Q0, node()),
            Self0 = <<"/$management/queues/", QNameBin/binary>>,
            Target0 = <<"/queue/", QNameBin/binary>>,
            {Type0, QNameBin, Self0, Target0};
        #{type := <<"exchange">> = Type0,
          name := XNameBin,
          exchange_type := XType,
          durable := Durable,
          auto_delete := AutoDelete,
          internal := Internal,
          arguments := XArgs} ->
            XName = rabbit_misc:r(Vhost, exchange, XNameBin),
            CheckedXType = rabbit_exchange:check_type(XType),
            _X = rabbit_exchange:declare(XName,
                                         CheckedXType,
                                         Durable,
                                         AutoDelete,
                                         Internal,
                                         XArgs,
                                         Username),
            Self0 = <<"/$management/exchanges/", XNameBin/binary>>,
            Target0 = <<"/exchange/", XNameBin/binary>>,
            {Type0, XNameBin, Self0, Target0}

    end,
    Self = {utf8, Self1},
    Props = #'v1_0.properties'{
               subject = {utf8, <<"201">>},
               content_type = {symbol, <<"application/amqp-management+amqp;type=entity-collection">>}
              },
    AppProps = #'v1_0.application_properties'{
                  %% TODO include vhost in URI?
                  content = [{{utf8, <<"location">>}, Self}]},
    RespPayload = {map, [{{utf8, <<"type">>}, {utf8, Type}},
                         {{utf8, <<"id">>}, {utf8, Id}},
                         {{utf8, <<"self">>}, Self},
                         {{utf8, <<"target">>}, {utf8, Target}},
                         {{utf8, <<"management">>}, {utf8, <<"$management">>}}
                        ]},
    {Props, AppProps, RespPayload};

process_http_request(<<"POST">>,
                     <<"/$management/", Path0/binary>>,
                     {symbol, <<"application/amqp-management+amqp", _OptionalType/binary>>},
                     [ReqPayload],
                     Vhost,
                     #user{username = Username},
                     _) ->
    {DstKind, Kinds, Path} = case Path0 of
                                 <<"queues/", Path1/binary>> ->
                                     {queue, <<"queues">>, Path1};
                                 <<"exchanges/", Path1/binary>> ->
                                     {exchange, <<"exchanges">>, Path1}
                             end,
    [DstNameBin, <<>>] = re:split(Path, <<"/\\$management/entities$">>, [{return, binary}]),
    #{source := SrcXNameBin,
      binding_key := BindingKey,
      arguments := Args} = decode_binding(ReqPayload),
    SrcXName = rabbit_misc:r(Vhost, exchange, SrcXNameBin),
    DstName = rabbit_misc:r(Vhost, DstKind, DstNameBin),
    Binding = #binding{source      = SrcXName,
                       destination = DstName,
                       key         = BindingKey,
                       args        = Args},
    %%TODO If the binding already exists, return 303 with location.
    ok = rabbit_binding:add(Binding, Username),
    Props = #'v1_0.properties'{
               subject = {utf8, <<"201">>},
               content_type = {symbol, <<"application/amqp-management+amqp;type=entity-collection">>}
              },
    %% TODO URI encode, which is against the HTTP over AMQP spec [3.1].
    %% How else to escape "/" in exchange names and binding keys?
    ArgsHash = args_hash(Args),
    Self = {utf8, <<"/$management/",
                    Kinds/binary, "/",
                    DstNameBin/binary, "/bindings/",
                    SrcXNameBin/binary, "/",
                    BindingKey/binary, "/",
                    ArgsHash/binary>>},
    AppProps = #'v1_0.application_properties'{
                  content = [{{utf8, <<"location">>}, Self}]},
    %% TODO Include source_exchange, binding key, and binding arguments in the response?
    RespPayload = {map, [{{utf8, <<"type">>}, {utf8, <<"binding">>}},
                         {{utf8, <<"self">>}, Self}
                        ]},
    {Props, AppProps, RespPayload};

process_http_request(<<"POST">>,
                     <<"/$management/queues/", QNamePath/binary>>,
                     undefined,
                     [],
                     Vhost,
                     _User,
                     ConnectionPid) ->
    [QNameBin, <<>>] = re:split(QNamePath, <<"/\\$management/purge$">>, [{return, binary}]),
    QName = rabbit_misc:r(Vhost, queue, QNameBin),

    {ok, NumMsgs} = rabbit_amqqueue:with_exclusive_access_or_die(
                      QName, ConnectionPid,
                      fun (Q) ->
                              rabbit_queue_type:purge(Q)
                      end),
    Props = #'v1_0.properties'{
               subject = {utf8, <<"200">>},
               content_type = {symbol, <<"application/amqp-management+amqp">>}
              },
    AppProps = #'v1_0.application_properties'{content = []},
    RespPayload = {map, [{{utf8, <<"message_count">>}, {ulong, NumMsgs}}]},
    {Props, AppProps, RespPayload};

process_http_request(<<"DELETE">>,
                     <<"/$management/queues/", Path/binary>>,
                     undefined,
                     [],
                     Vhost,
                     #user{username = Username},
                     ConnectionPid) ->
    case re:split(Path, <<"/">>, [{return, binary}]) of
        [QNameBin] ->
            QName = rabbit_misc:r(Vhost, queue, QNameBin),
            {ok, NumMsgs} = rabbit_amqqueue:delete_with(QName, ConnectionPid, false, false, Username, true),
            Props = #'v1_0.properties'{
                       subject = {utf8, <<"200">>},
                       content_type = {symbol, <<"application/amqp-management+amqp">>}
                      },
            AppProps = #'v1_0.application_properties'{content = []},
            RespPayload = {map, [{{utf8, <<"message_count">>}, {ulong, NumMsgs}}]},
            {Props, AppProps, RespPayload};
        [QNameBin, <<"bindings">>, SrcXNameBin, BindingKey, ArgsHash] ->
            SrcXName = rabbit_misc:r(Vhost, exchange, SrcXNameBin),
            QName = rabbit_misc:r(Vhost, queue, QNameBin),
            Bindings0 = rabbit_binding:list_for_source_and_destination(SrcXName, QName, true),
            case lists:search(fun(#binding{key = Key,
                                           args = Args}) ->
                                      Key =:= BindingKey andalso
                                      args_hash(Args) =:= ArgsHash
                              end, Bindings0) of
                {value, Binding} ->
                    ok = rabbit_binding:remove(Binding, Username);
                false ->
                    ok
            end,
            Props = #'v1_0.properties'{subject = {utf8, <<"204">>}},
            AppProps = #'v1_0.application_properties'{content = []},
            RespPayload = {map, []},
            {Props, AppProps, RespPayload}
    end;

process_http_request(<<"DELETE">>,
                     <<"/$management/exchanges/", Path/binary>>,
                     undefined,
                     [],
                     Vhost,
                     #user{username = Username},
                     _ConnectionPid) ->
    case re:split(Path, <<"/">>, [{return, binary}]) of
        [XNameBin] ->
            XName = rabbit_misc:r(Vhost, exchange, XNameBin),
            ok = case rabbit_exchange:delete(XName, false, Username) of
                     ok ->
                         ok;
                     {error, not_found} ->
                         ok
                         %% %% TODO return deletion failure
                         %% {error, in_use} ->
                 end,
            Props = #'v1_0.properties'{subject = {utf8, <<"204">>}},
            AppProps = #'v1_0.application_properties'{content = []},
            RespPayload = {map, []},
            {Props, AppProps, RespPayload};
        [DstXNameBin, <<"bindings">>, SrcXNameBin, BindingKey, ArgsHash] ->
            SrcXName = rabbit_misc:r(Vhost, exchange, SrcXNameBin),
            DstXName = rabbit_misc:r(Vhost, exchange, DstXNameBin),
            Bindings0 = rabbit_binding:list_for_source_and_destination(SrcXName, DstXName, true),
            case lists:search(fun(#binding{key = Key,
                                           args = Args}) ->
                                      Key =:= BindingKey andalso
                                      args_hash(Args) =:= ArgsHash
                              end, Bindings0) of
                {value, Binding} ->
                    ok = rabbit_binding:remove(Binding, Username);
                false ->
                    ok
            end,
            Props = #'v1_0.properties'{subject = {utf8, <<"204">>}},
            AppProps = #'v1_0.application_properties'{content = []},
            RespPayload = {map, []},
            {Props, AppProps, RespPayload}
    end;

process_http_request(<<"GET">>,
                     <<"/$management/", Path0/binary>>,
                     undefined,
                     [],
                     Vhost,
                     _User,
                     _) ->
    {DstKind, Path} = case Path0 of
                          <<"queues/", Path1/binary>> ->
                              {queue, Path1};
                          <<"exchanges/", Path1/binary>> ->
                              {exchange, Path1}
                      end,
    [DstNameBin, SrcXNameBin] = re:split(Path, <<"/\\$management/bindings\\?source=">>, [{return, binary}]),
    SrcXName = rabbit_misc:r(Vhost, exchange, SrcXNameBin),
    DstName = rabbit_misc:r(Vhost, DstKind, DstNameBin),
    Bindings0 = rabbit_binding:list_for_source_and_destination(SrcXName, DstName, true),
    RespPayload = encode_bindings(Bindings0),
    Props = #'v1_0.properties'{
               subject = {utf8, <<"200">>},
               content_type = {symbol, <<"application/amqp-management+amqp">>}
              },
    AppProps = #'v1_0.application_properties'{content = []},
    {Props, AppProps, RespPayload}.

decode_entity({map, KVList}) ->
    lists:foldl(
      fun({{utf8, <<"type">>}, {utf8, V}}, Acc) ->
              Acc#{type => V};
         ({{utf8, <<"name">>}, {utf8, V}}, Acc) ->
              Acc#{name => V};
         ({{utf8, <<"durable">>}, V}, Acc)
           when is_boolean(V) ->
              Acc#{durable => V};
         ({{utf8, <<"exclusive">>}, V}, Acc) ->
              Owner = case V of
                          false -> none;
                          true -> self()
                      end,
              Acc#{owner => Owner};
         ({{utf8, <<"auto_delete">>}, V}, Acc)
           when is_boolean(V) ->
              Acc#{auto_delete => V};
         ({{utf8, <<"exchange_type">>}, {utf8, V}}, Acc) ->
              Acc#{exchange_type => V};
         ({{utf8, <<"internal">>}, V}, Acc)
           when is_boolean(V) ->
              Acc#{internal => V};
         ({{utf8, <<"arguments">>}, {map, List}}, Acc) ->
              Args = [{Key, longstr, V}
                      || {{utf8, Key = <<"x-", _/binary>>},
                          {utf8, V}} <- List],
              Acc#{arguments => Args}
      end, #{}, KVList).

decode_binding({map, KVList}) ->
    lists:foldl(
      fun({{utf8, <<"type">>}, {utf8, <<"binding">>}}, Acc) ->
              Acc;
         ({{utf8, <<"source">>}, {utf8, V}}, Acc) ->
              Acc#{source => V};
         ({{utf8, <<"binding_key">>}, {utf8, V}}, Acc) ->
              Acc#{binding_key => V};
         ({{utf8, <<"arguments">>}, {map, List}}, Acc) ->
              Args = [mc_amqpl:to_091(Key, TypeVal)
                      || {{utf8, Key}, TypeVal} <- List],
              Acc#{arguments => Args}
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
                   ArgsHash = args_hash(Args091),
                   Self = <<"/$management/",
                            DstKindBin/binary, "s/",
                            DstName/binary, "/bindings/",
                            SrcName/binary, "/",
                            BindingKey/binary, "/",
                            ArgsHash/binary>>,
                   KVList = [
                             {{utf8, <<"type">>}, {utf8, <<"binding">>}},
                             {{utf8, <<"source">>}, {utf8, SrcName}},
                             {{utf8, DstKindBin}, {utf8, DstName}},
                             {{utf8, <<"binding_key">>}, {utf8, BindingKey}},
                             {{utf8, <<"arguments">>}, {map, Args}},
                             {{utf8, <<"self">>}, {utf8, Self}}
                            ],
                   {map, KVList}
           end, Bindings),
    {list, Bs}.

decode_req([], {Props, AppProps, DataRev}) ->
    #msg{properties = Props,
         application_properties = AppProps,
         data = lists:reverse(DataRev)};
decode_req([#'v1_0.properties'{} = P | Rem], Acc) ->
    decode_req(Rem, setelement(1, Acc, P));
decode_req([#'v1_0.application_properties'{content = Content} | Rem], Acc) ->
    decode_req(Rem, setelement(2, Content, Acc));
decode_req([#'v1_0.data'{content = C} | Rem], {Props, AppProps, DataRev}) ->
    decode_req(Rem, {Props, AppProps, [C | DataRev]});
decode_req([_IgnoreOtherSection | Rem], Acc) ->
    decode_req(Rem, Acc).

-spec args_hash(rabbit_framing:amqp_table()) -> binary().
args_hash(Args) ->
    list_to_binary(rabbit_misc:base64url(<<(erlang:phash2(Args, 1 bsl 32)):32>>)).
