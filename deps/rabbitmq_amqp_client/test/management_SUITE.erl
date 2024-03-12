%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(management_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include_lib("rabbitmq_amqp_client.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-compile([export_all,
          nowarn_export_all]).

-import(rabbit_ct_helpers,
        [eventually/1,
         eventually/3]).

-import(rabbit_ct_broker_helpers,
        [rpc/4,
         rpc/5]).

suite() ->
    [{timetrap, {minutes, 3}}].


all() ->
    [{group, tests}].

groups() ->
    [
     {tests, [shuffle],
      [all_management_operations,
       queue_binding_args,
       queue_defaults,
       queue_properties,
       exchange_defaults,
       bad_uri,
       bad_queue_property,
       bad_exchange_property,
       bad_exchange_type,
       declare_default_exchange,
       declare_exchange_amq_prefix,
       declare_exchange_line_feed,
       declare_exchange_inequivalent_fields,
       delete_default_exchange,
       delete_exchange_amq_prefix,
       delete_exchange_carriage_return
      ]}
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(_Group, Config) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(
                Config, [{rmq_nodes_count, 1},
                         {rmq_nodename_suffix, Suffix}]),
    rabbit_ct_helpers:run_setup_steps(
      Config1,
      rabbit_ct_broker_helpers:setup_steps()).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    %% Assert that every testcase cleaned up.
    eventually(?_assertEqual([], rpc(Config, rabbit_amqqueue, list, []))),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

all_management_operations(Config) ->
    Init = {_, Session, LinkPair} = init(Config),
    QName = <<"my 🐇"/utf8>>,
    QProps = #{durable => true,
               exclusive => false,
               auto_delete => false,
               arguments => #{<<"x-queue-type">> => {symbol, <<"quorum">>}}},
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, QProps),

    [Q] = rpc(Config, rabbit_amqqueue, list, []),
    ?assert(rpc(Config, amqqueue, is_durable, [Q])),
    ?assertNot(rpc(Config, amqqueue, is_exclusive, [Q])),
    ?assertNot(rpc(Config, amqqueue, is_auto_delete, [Q])),
    ?assertEqual(rabbit_quorum_queue, rpc(Config, amqqueue, get_type, [Q])),

    TargetAddr1 = <<"/amq/queue/", QName/binary>>,
    {ok, Sender1} = amqp10_client:attach_sender_link(Session, <<"sender 1">>, TargetAddr1),
    ok = wait_for_credit(Sender1),
    flush(credited),
    DTag1 = <<"tag 1">>,
    Msg1 = amqp10_msg:new(DTag1, <<"m1">>, false),
    ok = amqp10_client:send_msg(Sender1, Msg1),
    ok = wait_for_accepted(DTag1),

    RoutingKey1 = BindingKey1 = <<"🗝️ 1"/utf8>>,
    SourceExchange = <<"amq.direct">>,
    ?assertEqual(ok, rabbitmq_amqp_client:bind_queue(LinkPair, QName, SourceExchange, BindingKey1, #{})),
    TargetAddr2 = <<"/exchange/", SourceExchange/binary, "/", RoutingKey1/binary>>,

    {ok, Sender2} = amqp10_client:attach_sender_link(Session, <<"sender 2">>, TargetAddr2),
    ok = wait_for_credit(Sender2),
    flush(credited),
    DTag2 = <<"tag 2">>,
    Msg2 = amqp10_msg:new(DTag2, <<"m2">>, false),
    ok = amqp10_client:send_msg(Sender2, Msg2),
    ok = wait_for_accepted(DTag2),

    ?assertEqual(ok, rabbitmq_amqp_client:unbind_queue(LinkPair, QName, SourceExchange, BindingKey1, #{})),
    DTag3 = <<"tag 3">>,
    ok = amqp10_client:send_msg(Sender2, amqp10_msg:new(DTag3, <<"not routed">>, false)),
    ok = wait_for_settlement(DTag3, released),

    XName = <<"my fanout exchange 🥳"/utf8>>,
    XProps = #{type => <<"fanout">>,
               durable => false,
               auto_delete => true,
               internal => false,
               arguments => #{<<"x-📥"/utf8>> => {utf8, <<"📮"/utf8>>}}},
    ?assertEqual(ok, rabbitmq_amqp_client:declare_exchange(LinkPair, XName, XProps)),

    {ok, Exchange} = rpc(Config, rabbit_exchange, lookup, [rabbit_misc:r(<<"/">>, exchange, XName)]),
    ?assertMatch(#exchange{type = fanout,
                           durable = false,
                           auto_delete = true,
                           internal = false,
                           arguments = [{<<"x-📥"/utf8>>, longstr, <<"📮"/utf8>>}]},
                 Exchange),

    TargetAddr3 = <<"/exchange/", XName/binary>>,
    SourceExchange = <<"amq.direct">>,
    ?assertEqual(ok, rabbitmq_amqp_client:bind_queue(LinkPair, QName, XName, <<"ignored">>, #{})),

    {ok, Sender3} = amqp10_client:attach_sender_link(Session, <<"sender 3">>, TargetAddr3),
    ok = wait_for_credit(Sender3),
    flush(credited),
    DTag4 = <<"tag 4">>,
    Msg3 = amqp10_msg:new(DTag4, <<"m3">>, false),
    ok = amqp10_client:send_msg(Sender3, Msg3),
    ok = wait_for_accepted(DTag4),

    RoutingKey2 = BindingKey2 = <<"key 2">>,
    BindingArgs = #{<<" 😬 "/utf8>> => {utf8, <<" 😬 "/utf8>>}},
    ?assertEqual(ok, rabbitmq_amqp_client:bind_exchange(LinkPair, XName, SourceExchange, BindingKey2, BindingArgs)),
    TargetAddr4 = <<"/exchange/", SourceExchange/binary, "/", RoutingKey2/binary>>,

    {ok, Sender4} = amqp10_client:attach_sender_link(Session, <<"sender 4">>, TargetAddr4),
    ok = wait_for_credit(Sender4),
    flush(credited),
    DTag5 = <<"tag 5">>,
    Msg4 = amqp10_msg:new(DTag5, <<"m4">>, false),
    ok = amqp10_client:send_msg(Sender4, Msg4),
    ok = wait_for_accepted(DTag5),

    ?assertEqual(ok, rabbitmq_amqp_client:unbind_exchange(LinkPair, XName, SourceExchange, BindingKey2, BindingArgs)),
    DTag6 = <<"tag 6">>,
    ok = amqp10_client:send_msg(Sender4, amqp10_msg:new(DTag6, <<"not routed">>, false)),
    ok = wait_for_settlement(DTag6, released),

    ?assertEqual(ok, rabbitmq_amqp_client:delete_exchange(LinkPair, XName)),
    %% When we publish the next message, we expect:
    %% 1. that the message is released because the exchange doesn't exist anymore, and
    DTag7 = <<"tag 7">>,
    ok = amqp10_client:send_msg(Sender3, amqp10_msg:new(DTag7, <<"not routed">>, false)),
    ok = wait_for_settlement(DTag7, released),
    %% 2. that the server closes the link, i.e. sends us a DETACH frame.
    ExpectedError = #'v1_0.error'{condition = ?V_1_0_AMQP_ERROR_RESOURCE_DELETED},
    receive {amqp10_event, {link, Sender3, {detached, ExpectedError}}} -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,

    ?assertEqual({ok, #{message_count => 4}},
                 rabbitmq_amqp_client:purge_queue(LinkPair, QName)),

    ?assertEqual({ok, #{message_count => 0}},
                 rabbitmq_amqp_client:delete_queue(LinkPair, QName)),

    ok = cleanup(Init).

queue_defaults(Config) ->
    Init = {_, _, LinkPair} = init(Config),
    QName = atom_to_binary(?FUNCTION_NAME),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{}),
    [Q] = rpc(Config, rabbit_amqqueue, list, []),
    ?assert(rpc(Config, amqqueue, is_durable, [Q])),
    ?assertNot(rpc(Config, amqqueue, is_exclusive, [Q])),
    ?assertNot(rpc(Config, amqqueue, is_auto_delete, [Q])),
    ?assertEqual([], rpc(Config, amqqueue, get_arguments, [Q])),

    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = cleanup(Init).

queue_properties(Config) ->
    Init = {_, _, LinkPair} = init(Config),
    QName = atom_to_binary(?FUNCTION_NAME),
    {ok, _} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, #{durable => false,
                                                                    exclusive => true,
                                                                    auto_delete => true}),
    [Q] = rpc(Config, rabbit_amqqueue, list, []),
    ?assertNot(rpc(Config, amqqueue, is_durable, [Q])),
    ?assert(rpc(Config, amqqueue, is_exclusive, [Q])),
    ?assert(rpc(Config, amqqueue, is_auto_delete, [Q])),

    {ok, _} = rabbitmq_amqp_client:delete_queue(LinkPair, QName),
    ok = cleanup(Init).

exchange_defaults(Config) ->
    Init = {_, _, LinkPair} = init(Config),
    XName = atom_to_binary(?FUNCTION_NAME),
    ok = rabbitmq_amqp_client:declare_exchange(LinkPair, XName, #{}),
    {ok, Exchange} = rpc(Config, rabbit_exchange, lookup, [rabbit_misc:r(<<"/">>, exchange, XName)]),
    ?assertMatch(#exchange{type = direct,
                           durable = true,
                           auto_delete = false,
                           internal = false,
                           arguments = []},
                 Exchange),

    ok = rabbitmq_amqp_client:delete_exchange(LinkPair, XName),
    ok = cleanup(Init).

queue_binding_args(Config) ->
    Init = {_, Session, LinkPair} = init(Config),
    QName = <<"my queue ~!@#$%^&*()_+🙈`-=[]\;',./"/utf8>>,
    Q = #{durable => false,
          exclusive => true,
          auto_delete => false,
          arguments => #{<<"x-queue-type">> => {symbol, <<"classic">>}}},
    {ok, #{}} = rabbitmq_amqp_client:declare_queue(LinkPair, QName, Q),

    Exchange = <<"amq.headers">>,
    BindingKey = <<>>,
    BindingArgs = #{<<"key 1">> => {utf8, <<"👏"/utf8>>},
                    <<"key 2">> => {uint, 3},
                    <<"key 3">> => true,
                    <<"x-match">> => {utf8, <<"all">>}},
    ?assertEqual(ok, rabbitmq_amqp_client:bind_queue(LinkPair, QName, Exchange, BindingKey, BindingArgs)),

    TargetAddr = <<"/exchange/amq.headers">>,
    {ok, Sender} = amqp10_client:attach_sender_link(Session, <<"sender">>, TargetAddr),
    ok = wait_for_credit(Sender),
    flush(credited),
    DTag1 = <<"tag 1">>,
    Msg1 = amqp10_msg:new(DTag1, <<"m1">>, false),
    AppProps = #{<<"key 1">> => <<"👏"/utf8>>,
                 <<"key 2">> => 3,
                 <<"key 3">> => true},
    ok = amqp10_client:send_msg(Sender, amqp10_msg:set_application_properties(AppProps, Msg1)),
    ok = wait_for_accepted(DTag1),

    DTag2 = <<"tag 2">>,
    Msg2 = amqp10_msg:new(DTag2, <<"m2">>, false),
    ok = amqp10_client:send_msg(Sender,
                                amqp10_msg:set_application_properties(
                                  maps:remove(<<"key 2">>, AppProps),
                                  Msg2)),
    ok = wait_for_settlement(DTag2, released),

    ?assertEqual(ok, rabbitmq_amqp_client:unbind_queue(LinkPair, QName, Exchange, BindingKey, BindingArgs)),

    DTag3 = <<"tag 3">>,
    Msg3 = amqp10_msg:new(DTag3, <<"m3">>, false),
    ok = amqp10_client:send_msg(Sender, amqp10_msg:set_application_properties(AppProps, Msg3)),
    ok = wait_for_settlement(DTag3, released),

    ?assertEqual({ok, #{message_count => 1}},
                 rabbitmq_amqp_client:delete_queue(LinkPair, QName)),

    ok = amqp10_client:detach_link(Sender),
    ok = cleanup(Init).

bad_uri(Config) ->
    Init = {_, _, #link_pair{outgoing_link = OutgoingLink,
                             incoming_link = IncomingLink}} = init(Config),
    BadUri = <<"👎"/utf8>>,
    Correlation = <<1, 2, 3>>,
    Properties = #{subject => <<"GET">>,
                   to => BadUri,
                   message_id => {binary, Correlation},
                   reply_to => <<"$me">>},
    Body = null,
    Request0 = amqp10_msg:new(<<>>, #'v1_0.amqp_value'{content = Body}, true),
    Request =  amqp10_msg:set_properties(Properties, Request0),
    ok = amqp10_client:flow_link_credit(IncomingLink, 1, never),
    ok =  amqp10_client:send_msg(OutgoingLink, Request),

    receive {amqp10_msg, IncomingLink, Response} ->
                ?assertEqual(
                   #{subject => <<"400">>,
                     correlation_id => Correlation},
                   amqp10_msg:properties(Response)),
                ?assertEqual(
                   #'v1_0.amqp_value'{content = {utf8, <<"failed to normalize URI '👎': invalid_uri \"👎\""/utf8>>}},
                   amqp10_msg:body(Response))
    after 5000 -> ct:fail({missing_message, ?LINE})
    end,
    ok = cleanup(Init).

bad_queue_property(Config) ->
    bad_property(<<"queue">>, Config).

bad_exchange_property(Config) ->
    bad_property(<<"exchange">>, Config).

bad_property(Kind, Config) ->
    Init = {_, _, #link_pair{outgoing_link = OutgoingLink,
                             incoming_link = IncomingLink}} = init(Config),
    Correlation = <<1>>,
    Properties = #{subject => <<"PUT">>,
                   to => <<$/, Kind/binary, "s/my-object">>,
                   message_id => {binary, Correlation},
                   reply_to => <<"$me">>},
    Body = {map, [{{utf8, <<"unknown">>}, {utf8, <<"bla">>}}]},
    Request0 = amqp10_msg:new(<<>>, #'v1_0.amqp_value'{content = Body}, true),
    Request =  amqp10_msg:set_properties(Properties, Request0),
    ok = amqp10_client:flow_link_credit(IncomingLink, 1, never),
    ok =  amqp10_client:send_msg(OutgoingLink, Request),

    receive {amqp10_msg, IncomingLink, Response} ->
                ?assertEqual(
                   #{subject => <<"400">>,
                     correlation_id => Correlation},
                   amqp10_msg:properties(Response)),
                ?assertEqual(
                   #'v1_0.amqp_value'{
                      content = {utf8, <<"bad ", Kind/binary, " property {{utf8,<<\"unknown\">>},{utf8,<<\"bla\">>}}">>}},
                   amqp10_msg:body(Response))
    after 5000 -> ct:fail({missing_message, ?LINE})
    end,
    ok = cleanup(Init).

bad_exchange_type(Config) ->
    Init = {_, _, LinkPair} = init(Config),
    UnknownXType = <<"🤷"/utf8>>,
    {error, Resp} = rabbitmq_amqp_client:declare_exchange(LinkPair, <<"e1">>, #{type => UnknownXType}),
    ?assertMatch(#{subject := <<"400">>}, amqp10_msg:properties(Resp)),
    ?assertEqual(#'v1_0.amqp_value'{content = {utf8, <<"unknown exchange type '", UnknownXType/binary, "'">>}},
                 amqp10_msg:body(Resp)),
    ok = cleanup(Init).

declare_default_exchange(Config) ->
    Init = {_, _, LinkPair} = init(Config),
    DefaultX = <<"">>,
    {error, Resp} = rabbitmq_amqp_client:declare_exchange(LinkPair, DefaultX, #{}),
    ?assertMatch(#{subject := <<"403">>}, amqp10_msg:properties(Resp)),
    ?assertEqual(#'v1_0.amqp_value'{content = {utf8, <<"operation not permitted on the default exchange">>}},
                 amqp10_msg:body(Resp)),
    ok = cleanup(Init).

declare_exchange_amq_prefix(Config) ->
    Init = {_, _, LinkPair} = init(Config),
    XName = <<"amq.🎇"/utf8>>,
    {error, Resp} = rabbitmq_amqp_client:declare_exchange(LinkPair, XName, #{}),
    ?assertMatch(#{subject := <<"403">>}, amqp10_msg:properties(Resp)),
    ?assertEqual(#'v1_0.amqp_value'{
                    content = {utf8, <<"exchange '", XName/binary, "' in vhost '/' "
                                       "starts with reserved prefix 'amq.'">>}},
                 amqp10_msg:body(Resp)),
    ok = cleanup(Init).

declare_exchange_line_feed(Config) ->
    Init = {_, _, LinkPair} = init(Config),
    XName = <<"🤠\n😱"/utf8>>,
    {error, Resp} = rabbitmq_amqp_client:declare_exchange(LinkPair, XName, #{}),
    ?assertMatch(#{subject := <<"400">>}, amqp10_msg:properties(Resp)),
    ?assertEqual(#'v1_0.amqp_value'{content = {utf8, <<"Bad name '", XName/binary, "': \n and \r not allowed">>}},
                 amqp10_msg:body(Resp)),
    ok = cleanup(Init).

declare_exchange_inequivalent_fields(Config) ->
    Init = {_, _, LinkPair} = init(Config),
    XName = <<"👌"/utf8>>,
    ok = rabbitmq_amqp_client:declare_exchange(LinkPair, XName, #{type => <<"direct">>}),
    {error, Resp} = rabbitmq_amqp_client:declare_exchange(LinkPair, XName, #{type => <<"fanout">>}),
    ?assertMatch(#{subject := <<"409">>}, amqp10_msg:properties(Resp)),
    ?assertEqual(#'v1_0.amqp_value'{
                    content = {utf8, <<"inequivalent arg 'type' for exchange '", XName/binary,
                                       "' in vhost '/': received 'fanout' but current is 'direct'">>}},
                 amqp10_msg:body(Resp)),
    ok = cleanup(Init).

delete_default_exchange(Config) ->
    Init = {_, _, LinkPair} = init(Config),
    DefaultX = <<"">>,
    {error, Resp} = rabbitmq_amqp_client:delete_exchange(LinkPair, DefaultX),
    ?assertMatch(#{subject := <<"403">>}, amqp10_msg:properties(Resp)),
    ?assertEqual(#'v1_0.amqp_value'{content = {utf8, <<"operation not permitted on the default exchange">>}},
                 amqp10_msg:body(Resp)),
    ok = cleanup(Init).

delete_exchange_amq_prefix(Config) ->
    Init = {_, _, LinkPair} = init(Config),
    XName = <<"amq.fanout">>,
    {error, Resp} = rabbitmq_amqp_client:delete_exchange(LinkPair, XName),
    ?assertMatch(#{subject := <<"403">>}, amqp10_msg:properties(Resp)),
    ?assertEqual(#'v1_0.amqp_value'{
                    content = {utf8, <<"exchange '", XName/binary, "' in vhost '/' "
                                       "starts with reserved prefix 'amq.'">>}},
                 amqp10_msg:body(Resp)),
    ok = cleanup(Init).

delete_exchange_carriage_return(Config) ->
    Init = {_, _, LinkPair} = init(Config),
    XName = <<"x\rx">>,
    {error, Resp} = rabbitmq_amqp_client:delete_exchange(LinkPair, XName),
    ?assertMatch(#{subject := <<"400">>}, amqp10_msg:properties(Resp)),
    ?assertEqual(#'v1_0.amqp_value'{content = {utf8, <<"Bad name '", XName/binary, "': \n and \r not allowed">>}},
                 amqp10_msg:body(Resp)),
    ok = cleanup(Init).

init(Config) ->
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, LinkPair} = rabbitmq_amqp_client:attach_management_link_pair_sync(Session, <<"my link pair">>),
    {Connection, Session, LinkPair}.

cleanup({Connection, Session, LinkPair}) ->
    ok = rabbitmq_amqp_client:detach_management_link_pair_sync(LinkPair),
    ok = amqp10_client:end_session(Session),
    ok = amqp10_client:close_connection(Connection).

connection_config(Config) ->
    Host = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    #{address => Host,
      port => Port,
      container_id => <<"my container">>,
      sasl => {plain, <<"guest">>, <<"guest">>}}.

wait_for_credit(Sender) ->
    receive
        {amqp10_event, {link, Sender, credited}} ->
            ok
    after 5000 ->
              flush(?FUNCTION_NAME),
              ct:fail(?FUNCTION_NAME)
    end.

flush(Prefix) ->
    receive
        Msg ->
            ct:pal("~p flushed: ~p~n", [Prefix, Msg]),
            flush(Prefix)
    after 1 ->
              ok
    end.

wait_for_accepted(Tag) ->
    wait_for_settlement(Tag, accepted).

wait_for_settlement(Tag, State) ->
    receive
        {amqp10_disposition, {State, Tag}} ->
            ok
    after 5000 ->
              Reason = {?FUNCTION_NAME, Tag},
              flush(Reason),
              ct:fail(Reason)
    end.
