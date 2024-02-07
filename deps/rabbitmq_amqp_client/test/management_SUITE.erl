%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(management_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include_lib("amqp10_common/include/amqp10_framing.hrl").

-compile([export_all,
          nowarn_export_all]).

suite() ->
    [{timetrap, {minutes, 3}}].


all() ->
    [{group, tests}].

groups() ->
    [
     {tests, [shuffle], [all_management_operations]}
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
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

all_management_operations(Config) ->
    OpnConf = connection_config(Config),
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    receive {amqp10_event, {connection, C, opened}}
              when C =:= Connection -> ok
    after 5000 -> ct:fail({missing_event, ?LINE})
    end,
    {ok, Session} = amqp10_client:begin_session_sync(Connection),
    {ok, LinkPair} = rabbitmq_amqp_client:attach_management_link_pair_sync(
                       Session, <<"my-link-pair">>),

    QName = <<"q1">>,
    Q = #{name => QName,
          durable => true,
          exclusive => false,
          auto_delete => false,
          arguments => #{<<"x-queue-type">> => {symbol, <<"quorum">>}}},
    {ok, #{{utf8, <<"target">>} := {utf8, TargetAddr1}} } = rabbitmq_amqp_client:declare_queue(LinkPair, Q),

    {ok, Sender1} = amqp10_client:attach_sender_link(Session, <<"sender 1">>, TargetAddr1),
    ok = wait_for_credit(Sender1),
    flush(credited),
    DTag1 = <<"tag 1">>,
    Msg1 = amqp10_msg:new(DTag1, <<"m1">>, false),
    ok = amqp10_client:send_msg(Sender1, Msg1),
    ok = wait_for_accepted(DTag1),

    RoutingKey1 = BindingKey1 = <<"key 1">>,
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

    XName = <<"my-fanout-exchange">>,
    X = #{name => XName,
          type => <<"fanout">>,
          durable => true,
          auto_delete => false,
          internal => false,
          arguments => #{}},
    {ok, #{{utf8, <<"target">>} := {utf8, TargetAddr3}} } = rabbitmq_amqp_client:declare_exchange(LinkPair, X),

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
    ?assertEqual(ok, rabbitmq_amqp_client:bind_exchange(LinkPair, XName, SourceExchange, BindingKey2, #{})),
    TargetAddr4 = <<"/exchange/", SourceExchange/binary, "/", RoutingKey2/binary>>,

    {ok, Sender4} = amqp10_client:attach_sender_link(Session, <<"sender 4">>, TargetAddr4),
    ok = wait_for_credit(Sender4),
    flush(credited),
    DTag5 = <<"tag 5">>,
    Msg4 = amqp10_msg:new(DTag5, <<"m4">>, false),
    ok = amqp10_client:send_msg(Sender4, Msg4),
    ok = wait_for_accepted(DTag5),

    ?assertEqual(ok, rabbitmq_amqp_client:unbind_exchange(LinkPair, XName, SourceExchange, BindingKey2, #{})),
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
