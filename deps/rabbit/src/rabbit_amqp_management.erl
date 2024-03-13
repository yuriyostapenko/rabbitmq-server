-module(rabbit_amqp_management).

-include("rabbit_amqp.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-export([handle_request/4]).

-type resource_name() :: rabbit_types:exchange_name() | rabbit_types:rabbit_amqqueue_name().

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
     RespBody} = try {PathSegments, QueryMap} = parse_uri(HttpRequestTarget),
                     handle_http_req(HttpMethod,
                                     PathSegments,
                                     QueryMap,
                                     ReqBody,
                                     Vhost,
                                     User,
                                     ConnectionPid)
                 catch throw:{?MODULE, StatusCode0, Explanation} ->
                           rabbit_log:warning("request ~ts ~ts failed: ~ts",
                                              [HttpMethod, HttpRequestTarget, Explanation]),
                           {StatusCode0, {utf8, Explanation}}
                 end,

    RespProps = #'v1_0.properties'{
                   subject = {utf8, StatusCode},
                   %% "To associate a response with a request, the correlation-id value of the response
                   %% properties MUST be set to the message-id value of the request properties."
                   %% [HTTP over AMQP WD 06 §5.1]
                   correlation_id = MessageId},
    RespAppProps = #'v1_0.application_properties'{
                      content = [
                                 {{utf8, <<"http:response">>}, {utf8, <<"1.1">>}}
                                ]},
    RespDataSect = #'v1_0.amqp_value'{content = RespBody},
    RespSections = [RespProps, RespAppProps, RespDataSect],
    [amqp10_framing:encode_bin(Sect) || Sect <- RespSections].

%%TODO
%% If new queue / exchange gets created, return 201 with content.
%% If queue with same fields already exists, return 200 including the queue content.
%% If queue / exchange with other fields exists, return 409 with explanation about which fields diff.
handle_http_req(<<"PUT">>,
                [<<"queues">>, QNameBinQuoted],
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
    QNameBin = uri_string:unquote(QNameBinQuoted),
    QName = rabbit_misc:r(Vhost, queue, QNameBin),
    Owner = case Exclusive of
                true -> ConnPid;
                false -> none
            end,
    Q0 = amqqueue:new(QName, none, Durable, AutoDelete, Owner,
                      QArgs, Vhost, #{user => Username}, QType),
    {new, _Q} = rabbit_queue_type:declare(Q0, node()),
    {<<"201">>, null};

handle_http_req(<<"PUT">>,
                [<<"exchanges">>, XNameBinQuoted],
                _Query,
                ReqPayload,
                Vhost,
                User = #user{username = Username},
                _ConnPid) ->
    XNameBin = uri_string:unquote(XNameBinQuoted),
    #{type := XTypeBin,
      durable := Durable,
      auto_delete := AutoDelete,
      internal := Internal,
      arguments := XArgs
     } = decode_exchange(ReqPayload),
    XTypeAtom = try rabbit_exchange:check_type(XTypeBin)
                catch exit:#amqp_error{explanation = Explanation} ->
                          throw(<<"400">>, Explanation, [])
                end,
    XName = rabbit_misc:r(Vhost, exchange, XNameBin),
    ok = prohibit_default_exchange(XName),
    ok = check_resource_access(XName, configure, User),
    X = case rabbit_exchange:lookup(XName) of
            {ok, FoundX} ->
                FoundX;
            {error, not_found} ->
                ok = prohibit_cr_lf(XNameBin),
                ok = prohibit_reserved_amq(XName),
                rabbit_exchange:declare(
                  XName, XTypeAtom, Durable, AutoDelete,
                  Internal, XArgs, Username)
        end,
    try rabbit_exchange:assert_equivalence(
          X, XTypeAtom, Durable, AutoDelete, Internal, XArgs) of
        ok ->
            {<<"204">>, null}
    catch exit:#amqp_error{name = precondition_failed,
                           explanation = Expl} ->
              throw(<<"409">>, Expl, [])
    end;

handle_http_req(<<"DELETE">>,
                [<<"queues">>, QNameBinQuoted, <<"messages">>],
                _Query,
                null,
                Vhost,
                _User,
                ConnPid) ->
    QNameBin = uri_string:unquote(QNameBinQuoted),
    QName = rabbit_misc:r(Vhost, queue, QNameBin),
    {ok, NumMsgs} = rabbit_amqqueue:with_exclusive_access_or_die(
                      QName, ConnPid,
                      fun (Q) ->
                              rabbit_queue_type:purge(Q)
                      end),
    RespPayload = {map, [{{utf8, <<"message_count">>}, {ulong, NumMsgs}}]},
    {<<"200">>, RespPayload};

handle_http_req(<<"DELETE">>,
                [<<"queues">>, QNameBinQuoted],
                _Query,
                null,
                Vhost,
                #user{username = Username},
                ConnPid) ->
    QNameBin = uri_string:unquote(QNameBinQuoted),
    QName = rabbit_misc:r(Vhost, queue, QNameBin),
    {ok, NumMsgs} = rabbit_amqqueue:delete_with(QName, ConnPid, false, false, Username, true),
    RespPayload = {map, [{{utf8, <<"message_count">>}, {ulong, NumMsgs}}]},
    {<<"200">>, RespPayload};

handle_http_req(<<"DELETE">>,
                [<<"exchanges">>, XNameBinQuoted],
                _Query,
                null,
                Vhost,
                User = #user{username = Username},
                _ConnPid) ->
    XNameBin = uri_string:unquote(XNameBinQuoted),
    XName = rabbit_misc:r(Vhost, exchange, XNameBin),
    ok = prohibit_cr_lf(XNameBin),
    ok = prohibit_default_exchange(XName),
    ok = prohibit_reserved_amq(XName),
    ok = check_resource_access(XName, configure, User),
    _ = rabbit_exchange:delete(XName, false, Username),
    {<<"204">>, null};

handle_http_req(<<"POST">>,
                [<<"bindings">>],
                _Query,
                ReqPayload,
                Vhost,
                User = #user{username = Username},
                ConnPid) ->
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
    ok = binding_checks(SrcXName, DstName, BindingKey, User),
    Binding = #binding{source      = SrcXName,
                       destination = DstName,
                       key         = BindingKey,
                       args        = Args},
    ok = binding_action(add, Binding, Username, ConnPid),
    {<<"204">>, null};

handle_http_req(<<"DELETE">>,
                [<<"bindings">>, BindingSegment],
                _Query,
                null,
                Vhost,
                User = #user{username = Username},
                ConnPid) ->
    {SrcXNameBin,
     DstKind,
     DstNameBin,
     BindingKey,
     ArgsHash} = decode_binding_path_segment(BindingSegment),
    SrcXName = rabbit_misc:r(Vhost, exchange, SrcXNameBin),
    DstName = rabbit_misc:r(Vhost, DstKind, DstNameBin),
    ok = binding_checks(SrcXName, DstName, BindingKey, User),
    Bindings = rabbit_binding:list_for_source_and_destination(SrcXName, DstName),
    case search_binding(BindingKey, ArgsHash, Bindings) of
        {value, Binding} ->
            ok = binding_action(remove, Binding, Username, ConnPid);
        false ->
            ok
    end,
    {<<"204">>, null};

handle_http_req(<<"GET">>,
                [<<"bindings">>],
                QueryMap = #{<<"src">> := SrcXNameBin,
                             <<"key">> := Key},
                null,
                Vhost,
                _User,
                _ConnPid) ->
    {DstKind,
     DstNameBin} = case QueryMap of
                       #{<<"dste">> := DstX} ->
                           {exchange, DstX};
                       #{<<"dstq">> := DstQ} ->
                           {queue, DstQ};
                       _ ->
                           throw(<<"400">>,
                                 "missing 'dste' or 'dstq' in query: ~tp",
                                 QueryMap)
                   end,
    SrcXName = rabbit_misc:r(Vhost, exchange, SrcXNameBin),
    DstName = rabbit_misc:r(Vhost, DstKind, DstNameBin),
    Bindings0 = rabbit_binding:list_for_source_and_destination(SrcXName, DstName),
    Bindings = [B || B = #binding{key = K} <- Bindings0, K =:= Key],
    RespPayload = encode_bindings(Bindings),
    {<<"200">>, RespPayload}.

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
    %% Args is already sorted.
    Bin = <<(erlang:phash2(Args, 1 bsl 32)):32>>,
    base64:encode(Bin, #{mode => urlsafe,
                         padding => false}).

-spec binding_checks(rabbit_types:exchange_name(),
                     resource_name(),
                     rabbit_types:binding_key(),
                     rabbit_types:user()) -> ok.
binding_checks(SrcXName, DstName, BindingKey, User) ->
    lists:foreach(fun(#resource{name = NameBin} = Name) ->
                          ok = prohibit_default_exchange(Name),
                          ok = prohibit_cr_lf(NameBin)
                  end, [SrcXName, DstName]),
    ok = check_resource_access(DstName, write, User),
    ok = check_resource_access(SrcXName, read, User),
    case rabbit_exchange:lookup(SrcXName) of
        {ok, SrcX} ->
            rabbit_amqp_session:check_read_permitted_on_topic(SrcX, User, BindingKey);
        {error, not_found} ->
            ok
    end.

binding_action(Action, Binding, Username, ConnPid) ->
    try rabbit_channel:binding_action(Action, Binding, Username, ConnPid)
    catch exit:#amqp_error{explanation = Explanation} ->
              throw(<<"400">>, Explanation, [])
    end.

prohibit_cr_lf(NameBin) ->
    case binary:match(NameBin, [<<"\n">>, <<"\r">>]) of
        nomatch ->
            ok;
        _Found ->
            throw(<<"400">>,
                  <<"Bad name '~ts': line feed and carriage return characters not allowed">>,
                  [NameBin])
    end.

prohibit_default_exchange(#resource{kind = exchange,
                                    name = <<"">>}) ->
    throw(<<"403">>, <<"operation not permitted on the default exchange">>, []);
prohibit_default_exchange(_) ->
    ok.

-spec prohibit_reserved_amq(resource_name()) -> ok.
prohibit_reserved_amq(Res = #resource{name = <<"amq.", _/binary>>}) ->
    throw(<<"403">>,
          "~ts starts with reserved prefix 'amq.'",
          [rabbit_misc:rs(Res)]);
prohibit_reserved_amq(#resource{}) ->
    ok.

-spec check_resource_access(resource_name(),
                            rabbit_types:permission_atom(),
                            rabbit_types:user()) -> ok.
check_resource_access(Resource, Perm, User) ->
    try rabbit_access_control:check_resource_access(User, Resource, Perm, #{})
    catch exit:#amqp_error{name = access_refused,
                           explanation = Explanation} ->
              %% For authorization failures, let's be more strict: Close the entire
              %% AMQP session instead of only returning an HTTP Status Code 403.
              rabbit_amqp_util:protocol_error(
                ?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS, Explanation, [])
    end.

-spec throw(binary(), io:format(), [term()]) -> no_return().
throw(StatusCode, Format, Data) ->
    Reason0 = lists:flatten(io_lib:format(Format, Data)),
    Reason = unicode:characters_to_binary(Reason0),
    throw({?MODULE, StatusCode, Reason}).
