%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(rabbitmq_amqp_client).

-feature(maybe_expr, enable).

-include_lib("amqp10_common/include/amqp10_framing.hrl").

-export[attach_management_link_pair_sync/2,
        detach_management_link_pair_sync/1,
        declare_queue/2,
        declare_exchange/2,
        bind_queue/5,
        bind_exchange/5,
        unbind_queue/5,
        unbind_exchange/5,
        purge_queue/2,
        delete_queue/2,
        delete_exchange/2
       ].

-define(TIMEOUT, 15_000).
-define(MANAGEMENT_NODE_ADDRESS, <<"/management/v2">>).

-record(link_pair, {outgoing_link :: amqp10_client:link_ref(),
                    incoming_link :: amqp10_client:link_ref()}).
-type link_pair() :: #link_pair{}.

-type x_args() :: #{binary() => {atom(), term()}}.

-type queue_properties() :: #{name => binary(),
                              durable => boolean(),
                              exclusive => boolean(),
                              auto_delete => boolean(),
                              arguments => x_args()}.

-type exchange_properties() :: #{name := binary(),
                                 type => binary(),
                                 durable => boolean(),
                                 auto_delete => boolean(),
                                 internal => boolean(),
                                 arguments => x_args()}.

-type amqp10_prim() :: amqp10_binary_generator:amqp10_prim().

-export_type([link_pair/0]).

-spec attach_management_link_pair_sync(pid(), binary()) ->
    {ok, link_pair()} | {error, term()}.
attach_management_link_pair_sync(Session, Name) ->
    Terminus = #{address => ?MANAGEMENT_NODE_ADDRESS,
                 durable => none},
    OutgoingAttachArgs = #{name => Name,
                           role => {sender, Terminus},
                           snd_settle_mode => settled,
                           rcv_settle_mode => first,
                           properties => #{<<"paired">> => true}},
    IncomingAttachArgs = OutgoingAttachArgs#{role := {receiver, Terminus, self()},
                                             filter => #{}},
    maybe
        {ok, OutgoingRef} ?= attach(Session, OutgoingAttachArgs),
        {ok, IncomingRef} ?= attach(Session, IncomingAttachArgs),
        ok ?= await_attached(OutgoingRef),
        ok ?= await_attached(IncomingRef),
        {ok, #link_pair{outgoing_link = OutgoingRef,
                        incoming_link = IncomingRef}}
    end.

-spec attach(pid(), amqp10_client:attach_args()) ->
    {ok, amqp10_client:link_ref()} | {error, term()}.
attach(Session, AttachArgs) ->
    try amqp10_client:attach_link(Session, AttachArgs)
    catch exit:Reason ->
              {error, Reason}
    end.

-spec await_attached(amqp10_client:link_ref()) ->
    ok | {error, term()}.
await_attached(Ref) ->
    receive
        {amqp10_event, {link, Ref, attached}} ->
            ok;
        {amqp10_event, {link, Ref, {detached, Err}}} ->
            {error, Err}
    after ?TIMEOUT ->
              {error, timeout}
    end.

-spec detach_management_link_pair_sync(link_pair()) ->
    ok | {error, term()}.
detach_management_link_pair_sync(
  #link_pair{outgoing_link = OutgoingLink,
             incoming_link = IncomingLink}) ->
    maybe
        ok ?= detach(OutgoingLink),
        ok ?= detach(IncomingLink),
        ok ?= await_detached(OutgoingLink),
        await_detached(IncomingLink)
    end.

-spec detach(amqp10_client:link_ref()) ->
    ok | {error, term()}.
detach(Ref) ->
    try amqp10_client:detach_link(Ref)
    catch exit:Reason ->
              {error, Reason}
    end.

-spec await_detached(amqp10_client:link_ref()) ->
    ok | {error, term()}.
await_detached(Ref) ->
    receive
        {amqp10_event, {link, Ref, {detached, normal}}} ->
            ok;
        {amqp10_event, {link, Ref, {detached, Err}}} ->
            {error, Err}
    after ?TIMEOUT ->
              {error, timeout}
    end.

-spec declare_queue(link_pair(), queue_properties()) ->
    {ok, map()} | {error, term()}.
declare_queue(LinkPair, QueueProperties) ->
    {QName, Body0} = maps:fold(
                       fun(name, V, {undefined, L}) when is_binary(V) ->
                               {V, L};
                          (durable, V, {N, L}) when is_boolean(V) ->
                               {N, [{{utf8, <<"durable">>}, {boolean, V}} | L]};
                          (exclusive, V, {N, L}) when is_boolean(V) ->
                               {N, [{{utf8, <<"exclusive">>}, {boolean, V}} | L]};
                          (auto_delete, V, {N, L}) when is_boolean(V) ->
                               {N, [{{utf8, <<"auto_delete">>}, {boolean, V}} | L]};
                          (arguments, V, {N, L0}) ->
                               KVList = maps:fold(
                                          fun(K = <<"x-", _/binary>>, TaggedVal = {T, _}, L)
                                                when is_atom(T) ->
                                                  [{{utf8, K}, TaggedVal} | L]
                                          end, [], V),
                               {N, [{{utf8, <<"arguments">>}, {map, KVList}} | L0]}
                       end, {undefined, []}, QueueProperties),
    Body = {map, Body0},
    QNameQuoted = uri_string:quote(QName),
    Props = #{subject => <<"PUT">>,
              to => <<"/queues/", QNameQuoted/binary>>},

    case request(LinkPair, Props, Body) of
        {ok, Resp} ->
            case amqp10_msg:properties(Resp) of
                #{subject := <<"201">>} ->
                    #'v1_0.amqp_value'{content = null} = amqp10_msg:body(Resp),
                    % {ok, proplists:to_map(KVList)};
                    {ok, #{}};
                Other ->
                    {error, Other}
            end;
        Err ->
            Err
    end.

-spec bind_queue(link_pair(), binary(), binary(), binary(), #{binary() => amqp10_prim()}) ->
    ok | {error, term()}.
bind_queue(LinkPair, QueueName, ExchangeName, BindingKey, BindingArguments) ->
    bind(<<"destination_queue">>, LinkPair, QueueName, ExchangeName, BindingKey, BindingArguments).

-spec bind_exchange(link_pair(), binary(), binary(), binary(), #{binary() => amqp10_prim()}) ->
    ok | {error, term()}.
bind_exchange(LinkPair, Destination, Source, BindingKey, BindingArguments) ->
    bind(<<"destination_exchange">>, LinkPair, Destination, Source, BindingKey, BindingArguments).

-spec bind(binary(), link_pair(), binary(), binary(), binary(), #{binary() => amqp10_prim()}) ->
    ok | {error, term()}.
bind(DestinationKind, LinkPair, Destination, Source, BindingKey, BindingArguments) ->
    KVList = maps:fold(
               fun(Key, TaggedVal, L)
                     when is_binary(Key) ->
                       [{{utf8, Key}, TaggedVal} | L]
               end, [], BindingArguments),
    Body = {map, [
                  {{utf8, <<"source">>}, {utf8, Source}},
                  {{utf8, DestinationKind}, {utf8, Destination}},
                  {{utf8, <<"binding_key">>}, {utf8, BindingKey}},
                  {{utf8, <<"arguments">>}, {map, KVList}}
                 ]},
    Props = #{subject => <<"POST">>,
              to => <<"/bindings">>},

    case request(LinkPair, Props, Body) of
        {ok, Resp} ->
            case amqp10_msg:properties(Resp) of
                #{subject := <<"201">>} ->
                    ok;
                _ ->
                    {error, Resp}
            end;
        Err ->
            Err
    end.

-spec unbind_queue(link_pair(), binary(), binary(), binary(), #{binary() => amqp10_prim()}) ->
    ok | {error, term()}.
unbind_queue(LinkPair, QueueName, ExchangeName, BindingKey, BindingArguments) ->
    unbind($q, LinkPair, QueueName, ExchangeName, BindingKey, BindingArguments).

-spec unbind_exchange(link_pair(), binary(), binary(), binary(), #{binary() => amqp10_prim()}) ->
    ok | {error, term()}.
unbind_exchange(LinkPair, DestinationExchange, SourceExchange, BindingKey, BindingArguments) ->
    unbind($e, LinkPair, DestinationExchange, SourceExchange, BindingKey, BindingArguments).

-spec unbind(byte(), link_pair(), binary(), binary(), binary(), #{binary() => amqp10_prim()}) ->
    ok | {error, term()}.
unbind(DestinationChar, LinkPair, Destination, Source, BindingKey, BindingArguments)
  when map_size(BindingArguments) =:= 0 ->
    SrcQ = uri_string:quote(Source),
    DstQ = uri_string:quote(Destination),
    KeyQ = uri_string:quote(BindingKey),
    Uri = <<"/bindings/src=", SrcQ/binary,
            ";dst", DestinationChar, $=, DstQ/binary,
            ";key=", KeyQ/binary,
            ";args=">>,
    delete_binding(LinkPair, Uri);
unbind(DestinationChar, LinkPair, Destination, Source, BindingKey, BindingArguments) ->
    Path = <<"/bindings">>,
    Query = uri_string:compose_query(
              [{<<"src">>, Source},
               {<<"dst", DestinationChar>>, Destination},
               {<<"key">>, BindingKey}]),
    Uri0 = uri_string:recompose(#{path => Path,
                                  query => Query}),
    Props = #{subject => <<"GET">>,
              to => Uri0},

    case request(LinkPair, Props, null) of
        {ok, Resp} ->
            case amqp10_msg:properties(Resp) of
                #{subject := <<"200">>} ->
                    #'v1_0.amqp_value'{content = {list, Bindings}} = amqp10_msg:body(Resp),
                    case search_binding_uri(BindingArguments, Bindings) of
                        {ok, Uri} ->
                            delete_binding(LinkPair, Uri);
                        not_found ->
                            ok
                    end;
                _ ->
                    {error, Resp}
            end;
        Err ->
            Err
    end.

search_binding_uri(_, []) ->
    not_found;
search_binding_uri(BindingArguments, [{map, Binding} | Bindings]) ->
    case maps:from_list(Binding) of
        #{{utf8, <<"arguments">>} := {map, Args0},
          {utf8, <<"location">>} := {utf8, Uri}} ->
            Args = lists:map(fun({{utf8, Key}, TypeVal}) ->
                                     {Key, TypeVal}
                             end, Args0),
            case maps:from_list(Args) =:= BindingArguments of
                true ->
                    {ok, Uri};
                false ->
                    search_binding_uri(BindingArguments, Bindings)
            end;
        _ ->
            search_binding_uri(BindingArguments, Bindings)
    end.

-spec delete_binding(link_pair(), binary()) ->
    ok | {error, term()}.
delete_binding(LinkPair, BindingUri) ->
    Props = #{subject => <<"DELETE">>,
              to => BindingUri},
    case request(LinkPair, Props, null) of
        {ok, Resp} ->
            case amqp10_msg:properties(Resp) of
                #{subject := <<"204">>} ->
                    ok;
                _ ->
                    {error, Resp}
            end;
        Err ->
            Err
    end.

-spec delete_queue(link_pair(), binary()) ->
    {ok, map()} | {error, term()}.
delete_queue(LinkPair, QueueName) ->
    purge_or_delete_queue(LinkPair, QueueName, <<>>).

-spec purge_queue(link_pair(), binary()) ->
    {ok, map()} | {error, term()}.
purge_queue(LinkPair, QueueName) ->
    purge_or_delete_queue(LinkPair, QueueName, <<"/messages">>).

-spec purge_or_delete_queue(link_pair(), binary(), binary()) ->
    {ok, map()} | {error, term()}.
purge_or_delete_queue(LinkPair, QueueName, PathSuffix) ->
    QNameQuoted = uri_string:quote(QueueName),
    HttpRequestTarget = <<"/queues/", QNameQuoted/binary, PathSuffix/binary>>,
    Props = #{subject => <<"DELETE">>,
              to => HttpRequestTarget},
    case request(LinkPair, Props, null) of
        {ok, Resp} ->
            case amqp10_msg:properties(Resp) of
                #{subject := <<"200">>} ->
                    #'v1_0.amqp_value'{content = Content} = amqp10_msg:body(Resp),
                    {map, [
                           {{utf8, <<"message_count">>}, {ulong, Count}}
                          ]
                    } = Content,
                    {ok, #{message_count => Count}};
                _ ->
                    {error, Resp}
            end;
        Err ->
            Err
    end.

-spec declare_exchange(link_pair(), exchange_properties()) ->
    ok | {error, term()}.
declare_exchange(LinkPair, ExchangeProperties) ->
    {XName, Body0} = maps:fold(
                       fun(name, V, {undefined, L}) when is_binary(V) ->
                               {V, L};
                          (type, V, {N, L}) when is_binary(V) ->
                               {N, [{{utf8, <<"type">>}, {utf8, V}} | L]};
                          (durable, V, {N, L}) when is_boolean(V) ->
                               {N, [{{utf8, <<"durable">>}, {boolean, V}} | L]};
                          (auto_delete, V, {N, L}) when is_boolean(V) ->
                               {N, [{{utf8, <<"auto_delete">>}, {boolean, V}} | L]};
                          (internal, V, {N, L}) when is_boolean(V) ->
                               {N, [{{utf8, <<"internal">>}, {boolean, V}} | L]};
                          (arguments, V, {N, L0}) ->
                               KVList = maps:fold(
                                          fun(K = <<"x-", _/binary>>, TaggedVal = {T, _}, L)
                                                when is_atom(T) ->
                                                  [{{utf8, K}, TaggedVal} | L]
                                          end, [], V),
                               {N, [{{utf8, <<"arguments">>}, {map, KVList}} | L0]}
                       end, {undefined, []}, ExchangeProperties),
    Body = {map, Body0},

    XNameQuoted = uri_string:quote(XName),
    Props = #{subject => <<"PUT">>,
              to => <<"/exchanges/", XNameQuoted/binary>>},

    case request(LinkPair, Props, Body) of
        {ok, Resp} ->
            case amqp10_msg:properties(Resp) of
                #{subject := <<"201">>} ->
                    #'v1_0.amqp_value'{content = null} = amqp10_msg:body(Resp),
                    ok;
                _ ->
                    {error, Resp}
            end;
        Err ->
            Err
    end.

-spec delete_exchange(link_pair(), binary()) ->
    ok | {error, term()}.
delete_exchange(LinkPair, ExchangeName) ->
    XNameQuoted = uri_string:quote(ExchangeName),
    Props = #{subject => <<"DELETE">>,
              to => <<"/exchanges/", XNameQuoted/binary>>},
    case request(LinkPair, Props, null) of
        {ok, Resp} ->
            case amqp10_msg:properties(Resp) of
                #{subject := <<"204">>} ->
                    ok;
                _ ->
                    {error, Resp}
            end;
        Err ->
            Err
    end.

-spec request(link_pair(), amqp10_msg:amqp10_properties(), amqp10_prim()) ->
    {ok, Response :: amqp10_msg:amqp10_msg()} | {error, term()}.
request(#link_pair{outgoing_link = OutgoingLink,
                   incoming_link = IncomingLink}, Properties, Body) ->
    MessageId = message_id(),
    Properties1 = Properties#{message_id => {binary, MessageId},
                              reply_to => <<"$me">>},
    Request = amqp10_msg:new(<<>>, #'v1_0.amqp_value'{content = Body}, true),
    Request1 =  amqp10_msg:set_properties(Properties1, Request),
    ok = amqp10_client:flow_link_credit(IncomingLink, 1, never),
    case amqp10_client:send_msg(OutgoingLink, Request1) of
        ok ->
            receive {amqp10_msg, IncomingLink, Response} ->
                        #{correlation_id := MessageId} = amqp10_msg:properties(Response),
                        {ok, Response}
            after ?TIMEOUT ->
                      {error, response_timeout}
            end;
        Err ->
            Err
    end.

%% "The message producer is usually responsible for setting the message-id in
%% such a way that it is assured to be globally unique." [3.2.4]
-spec message_id() -> binary().
message_id() ->
    rand:bytes(8).
