%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(rabbit_amqp_session).

-compile({inline, [maps_update_with/4]}).

-behaviour(gen_server).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("amqp10_common/include/amqp10_types.hrl").
-include("rabbit_amqp.hrl").
-include("mc.hrl").

-define(PROTOCOL, amqp10).
-define(HIBERNATE_AFTER, 6_000).
-define(CREDIT_REPLY_TIMEOUT, 30_000).
-define(UINT_OUTGOING_WINDOW, {uint, ?UINT_MAX}).
-define(MAX_INCOMING_WINDOW, 400).
%% "The next-outgoing-id MAY be initialized to an arbitrary value" [2.5.6]
-define(INITIAL_OUTGOING_TRANSFER_ID, ?UINT_MAX - 3).
%% "Note that, despite its name, the delivery-count is not a count but a
%% sequence number initialized at an arbitrary point by the sender." [2.6.7]
-define(INITIAL_DELIVERY_COUNT, ?UINT_MAX - 4).
-define(INITIAL_OUTGOING_DELIVERY_ID, 0).
-define(DEFAULT_MAX_HANDLE, ?UINT_MAX).
%% [3.4]
-define(OUTCOMES, [?V_1_0_SYMBOL_ACCEPTED,
                   ?V_1_0_SYMBOL_REJECTED,
                   ?V_1_0_SYMBOL_RELEASED,
                   ?V_1_0_SYMBOL_MODIFIED]).
-define(MAX_PERMISSION_CACHE_SIZE, 12).
-define(TOPIC_PERMISSION_CACHE, topic_permission_cache).
-define(PROCESS_GROUP_NAME, amqp_sessions).
-define(UINT(N), {uint, N}).
%% This is the link credit that we grant to sending clients.
%% We are free to choose whatever we want, sending clients must obey.
%% Default soft limits / credits in deps/rabbit/Makefile are:
%% 32 for quorum queues
%% 256 for streams
%% 400 for classic queues
%% If link target is a queue (rather than an exchange), we could use one of these depending
%% on target queue type. For the time being just use a static value that's something in between.
%% An even better approach in future would be to dynamically grow (or shrink) the link credit
%% we grant depending on how fast target queue(s) actually confirm messages.
-define(LINK_CREDIT_RCV, 128).

%% Pure HTTP clients of a future HTTP API (v2) would call endpoints as follows:
%% GET /v2/vhosts/:vhost/queues/:queue
%%
%% Here, we use the terminus address /management/v2 so that AMQP 1.0 clients declare the HTTP API version
%% at link attachment time. The vhost is already determined at AMQP connection open time.
%% Therefore, there is no need to send the HTTP API version and the vhost in every HTTP over AMQP request.
-define(MANAGEMENT_NODE_ADDRESS, <<"/management/v2">>).

-export([start_link/8,
         process_frame/2,
         list_local/0,
         conserve_resources/3,
         check_read_permitted_on_topic/3
        ]).

-export([init/1,
         terminate/2,
         handle_call/3,
         handle_cast/2, 
         handle_info/2,
         format_status/1]).

-import(rabbit_amqp_util,
        [protocol_error/3]).
-import(serial_number,
        [add/2,
         diff/2,
         compare/2]).

%% incoming multi transfer delivery [2.6.14]
-record(multi_transfer_msg, {
          payload_fragments_rev :: [binary(),...],
          delivery_id :: delivery_number(),
          settled :: boolean()
         }).

%% For AMQP management operations, we require a link pair as described in
%% https://docs.oasis-open.org/amqp/linkpair/v1.0/cs01/linkpair-v1.0-cs01.html
-record(management_link_pair, {
          client_terminus_address,
          incoming_half :: unattached | link_handle(),
          outgoing_half :: unattached | link_handle()
         }).

%% Incoming or outgoing half of the link pair.
-record(management_link, {
          name :: binary(),
          delivery_count :: sequence_no(),
          %% Credit on an incoming management link is always 1 since management
          %% requests are synchronous and we grant 1 credit when sending a response.
          credit :: non_neg_integer(),
          max_message_size :: unlimited | pos_integer()
         }).

-record(incoming_link, {
          exchange :: rabbit_types:exchange() | rabbit_exchange:name(),
          routing_key :: undefined | rabbit_types:routing_key(),
          %% queue_name_bin is only set if the link target address refers to a queue.
          queue_name_bin :: undefined | rabbit_misc:resource_name(),
          delivery_count :: sequence_no(),
          credit :: non_neg_integer(),
          %% TRANSFER delivery IDs published to queues but not yet confirmed by queues
          incoming_unconfirmed_map = #{} :: #{delivery_number() =>
                                              {#{rabbit_amqqueue:name() := ok},
                                               IsTransferSettled :: boolean(),
                                               AtLeastOneQueueConfirmed :: boolean()}},
          multi_transfer_msg :: undefined | #multi_transfer_msg{}
         }).

-record(outgoing_link, {
          %% Although the source address of a link might be an exchange name and binding key
          %% or a topic filter, an outgoing link will always consume from a queue.
          queue_name_bin :: rabbit_misc:resource_name(),
          queue_type :: rabbit_queue_type:queue_type(),
          send_settled :: boolean(),
          max_message_size :: unlimited | pos_integer(),
          %% When credit API v1 is used, our session process holds the delivery-count
          %% When credit API v2 is used, the queue type implementation holds the delivery-count
          %% When feature flag credit_api_v2 becomes required, this field should be deleted.
          delivery_count :: {credit_api_v1, sequence_no()} | credit_api_v2
         }).

-record(outgoing_unsettled, {
          %% The queue sent us this consumer scoped sequence number.
          msg_id :: rabbit_amqqueue:msg_id(),
          consumer_tag :: rabbit_types:ctag(),
          queue_name :: rabbit_amqqueue:name()
         }).

-record(pending_transfer, {
          frames :: iolist(),
          queue_ack_required :: boolean(),
          delivery_id :: delivery_number(),
          outgoing_unsettled :: #outgoing_unsettled{}
         }).

-record(pending_management_transfer, {
          frames :: iolist()
         }).

-record(cfg, {
          outgoing_max_frame_size :: unlimited | pos_integer(),
          reader_pid :: rabbit_types:connection(),
          writer_pid :: pid(),
          user :: rabbit_types:user(),
          vhost :: rabbit_types:vhost(),
          %% We just use the incoming (AMQP 1.0) channel number.
          channel_num :: non_neg_integer(),
          %% We tolerate our incoming_window to be violated by up to this number of
          %% excess TRANSFERs. If the client sends us even more TRANSFERs, we will
          %% close the session with session error window-violation.
          %% Unless we decrease our incoming_window dynamically, we are strict by
          %% default and don't allow for any excess TRANSFERs.
          incoming_window_margin = 0 :: non_neg_integer(),
          resource_alarms :: sets:set(rabbit_alarm:resource_alarm_source()),
          trace_state :: rabbit_trace:state(),
          conn_name :: binary()
         }).

-record(state, {
          cfg :: #cfg{},

          %% The following 5 fields are state for session flow control.
          %% See section 2.5.6.
          %%
          %% We omit outgoing-window. We keep the outgoing-window always large and don't
          %% restrict ourselves delivering messages fast to AMQP clients because keeping an
          %% #outgoing_unsettled{} entry in the outgoing_unsettled_map requires far less
          %% memory than holding the message payload in the outgoing_pending queue.
          %%
          %% expected implicit transfer-id of next incoming TRANSFER
          next_incoming_id :: transfer_number(),
          %% Defines the maximum number of incoming transfer frames that we can currently receive.
          %% This value is chosen by us.
          %% Purpose:
          %% 1. It protects our session process from being overloaded, and
          %% 2. Since frames have a maximum size for a given connection, this provides flow control based
          %% on the number of bytes transmitted, and therefore protects our platform, i.e. RabbitMQ as a
          %% whole. We will set this window to 0 if a cluster wide memory or disk alarm occurs (see module
          %% rabbit_alarm) to stop receiving incoming TRANSFERs.
          %% (It's an optional feature: If we wanted we could always keep that window huge, i.e. not
          %% shrinking the window when we receive a TRANSFER. However, we do want to use that feature
          %% due to aforementioned purposes.)
          %% Can become negative up to -incoming_window_margin when client overshoots our window.
          incoming_window :: integer(),
          %% implicit transfer-id of our next outgoing TRANSFER
          next_outgoing_id :: transfer_number(),
          %% Defines the maximum number of outgoing transfer frames that we are
          %% currently allowed to send. This value is chosen by the AMQP client.
          remote_incoming_window :: non_neg_integer(),
          %% This field is informational.
          %% It reflects the maximum number of incoming TRANSFERs that may arrive without exceeding
          %% the AMQP client's own outgoing-window.
          %% When this window shrinks, it is an indication of outstanding transfers (from AMQP client
          %% to us) which we need to settle (after receiving confirmations from target queues) for
          %% the window to grow again.
          remote_outgoing_window :: non_neg_integer(),

          %% These messages were received from queues thanks to sufficient link credit.
          %% However, they are buffered here due to session flow control
          %% (when remote_incoming_window <= 0) before being sent to the AMQP client.
          %%
          %% FLOW frames are stored here as well because for a specific outgoing link the order
          %% in which we send TRANSFER and FLOW frames is important. An outgoing FLOW frame with link flow
          %% control information must not overtake a TRANSFER frame for the same link just because
          %% we are throttled by session flow control. (However, we can still send outgoing FLOW frames
          %% that contain only session flow control information, i.e. where the FLOW's 'handle' field is not set.)
          %% Example:
          %% A receiver grants our queue 2 credits with drain=true and the queue only has 1 message available.
          %% Even when we are limited by session flow control, we must make sure to first send the TRANSFER to the
          %% client (once the remote_incoming_window got opened) followed by the FLOW with drain=true and credit=0
          %% and advanced delivery count. Otherwise, we would violate the AMQP protocol spec.
          outgoing_pending = queue:new() :: queue:queue(#pending_transfer{} |
                                                        #pending_management_transfer{} |
                                                        #'v1_0.flow'{}),

          %% The link or session endpoint assigns each message a unique delivery-id
          %% from a session scoped sequence number.
          %%
          %% Do not confuse this field with next_outgoing_id:
          %% Both are session scoped sequence numbers, but initialised at different arbitrary values.
          %%
          %% next_outgoing_id is an implicit ID, i.e. not sent in the TRANSFER frame.
          %% outgoing_delivery_id is an explicit ID, i.e. sent in the TRANSFER frame.
          %%
          %% next_outgoing_id is incremented per TRANSFER frame.
          %% outgoing_delivery_id is incremented per message.
          %% Remember that a large message can be split up into multiple TRANSFER frames.
          outgoing_delivery_id :: delivery_number(),

          %% Links are unidirectional.
          %% We receive messages from clients on incoming links.
          incoming_links = #{} :: #{link_handle() => #incoming_link{}},
          %% We send messages to clients on outgoing links.
          outgoing_links = #{} :: #{link_handle() => #outgoing_link{}},

          management_link_pairs = #{} :: #{LinkName :: binary() => #management_link_pair{}},
          incoming_management_links = #{} :: #{link_handle() => #management_link{}},
          outgoing_management_links = #{} :: #{link_handle() => #management_link{}},

          %% TRANSFER delivery IDs published to consuming clients but not yet acknowledged by clients.
          outgoing_unsettled_map = #{} :: #{delivery_number() => #outgoing_unsettled{}},

          %% Queue actions that we will process later such that we can confirm and reject
          %% delivery IDs in ranges to reduce the number of DISPOSITION frames sent to the client.
          stashed_rejected = [] :: [{rejected, rabbit_amqqueue:name(), [delivery_number(),...]}],
          stashed_settled = [] :: [{settled, rabbit_amqqueue:name(), [delivery_number(),...]}],
          %% Classic queues that are down.
          stashed_down = []:: [rabbit_amqqueue:name()],
          %% Queues that got deleted.
          stashed_eol = [] :: [rabbit_amqqueue:name()],

          queue_states = rabbit_queue_type:init() :: rabbit_queue_type:state()
         }).

-type state() :: #state{}.

start_link(ReaderPid, WriterPid, ChannelNum, FrameMax, User, Vhost, ConnName, BeginFrame) ->
    Args = {ReaderPid, WriterPid, ChannelNum, FrameMax, User, Vhost, ConnName, BeginFrame},
    Opts = [{hibernate_after, ?HIBERNATE_AFTER}],
    gen_server:start_link(?MODULE, Args, Opts).

process_frame(Pid, Frame) ->
    gen_server:cast(Pid, {frame, Frame}).

init({ReaderPid, WriterPid, ChannelNum, MaxFrameSize, User, Vhost, ConnName,
      #'v1_0.begin'{next_outgoing_id = ?UINT(RemoteNextOutgoingId),
                    incoming_window = ?UINT(RemoteIncomingWindow),
                    outgoing_window = ?UINT(RemoteOutgoingWindow),
                    handle_max = HandleMax0}}) ->
    process_flag(trap_exit, true),
    process_flag(message_queue_data, off_heap),
    ok = pg:join(node(), ?PROCESS_GROUP_NAME, self()),

    Alarms0 = rabbit_alarm:register(self(), {?MODULE, conserve_resources, []}),
    Alarms = sets:from_list(Alarms0, [{version, 2}]),

    NextOutgoingId = ?INITIAL_OUTGOING_TRANSFER_ID,
    IncomingWindow = case sets:is_empty(Alarms) of
                         true -> ?MAX_INCOMING_WINDOW;
                         false -> 0
                     end,

    HandleMax = case HandleMax0 of
                    ?UINT(Max) -> Max;
                    _ -> ?DEFAULT_MAX_HANDLE
                end,
    Reply = #'v1_0.begin'{remote_channel = {ushort, ChannelNum},
                          handle_max = ?UINT(HandleMax),
                          next_outgoing_id = ?UINT(NextOutgoingId),
                          incoming_window = ?UINT(IncomingWindow),
                          outgoing_window = ?UINT_OUTGOING_WINDOW},
    rabbit_amqp_writer:send_command(WriterPid, ChannelNum, Reply),

    {ok, #state{next_incoming_id = RemoteNextOutgoingId,
                next_outgoing_id = NextOutgoingId,
                incoming_window = IncomingWindow,
                remote_incoming_window = RemoteIncomingWindow,
                remote_outgoing_window = RemoteOutgoingWindow,
                outgoing_delivery_id = ?INITIAL_OUTGOING_DELIVERY_ID,
                cfg = #cfg{reader_pid = ReaderPid,
                           writer_pid = WriterPid,
                           outgoing_max_frame_size = MaxFrameSize,
                           user = User,
                           vhost = Vhost,
                           channel_num = ChannelNum,
                           resource_alarms = Alarms,
                           trace_state = rabbit_trace:init(Vhost),
                           conn_name = ConnName
                          }}}.

terminate(_Reason, #state{incoming_links = IncomingLinks,
                          outgoing_links = OutgoingLinks,
                          queue_states = QStates}) ->
    maps:foreach(
      fun (_, _) ->
              rabbit_global_counters:publisher_deleted(?PROTOCOL)
      end, IncomingLinks),
    maps:foreach(
      fun (_, _) ->
              rabbit_global_counters:consumer_deleted(?PROTOCOL)
      end, OutgoingLinks),
    ok = rabbit_queue_type:close(QStates).

-spec list_local() -> [pid()].
list_local() ->
    pg:get_local_members(node(), ?PROCESS_GROUP_NAME).

-spec conserve_resources(pid(),
                         rabbit_alarm:resource_alarm_source(),
                         rabbit_alarm:resource_alert()) -> ok.
conserve_resources(Pid, Source, {_, Conserve, _}) ->
    gen_server:cast(Pid, {conserve_resources, Source, Conserve}).

handle_call(Msg, _From, State) ->
    Reply = {error, {not_understood, Msg}},
    reply(Reply, State).

handle_info(timeout, State) ->
    noreply(State);
handle_info({{'DOWN', QName}, _MRef, process, QPid, Reason},
            #state{queue_states = QStates0,
                   stashed_eol = Eol} = State0) ->
    case rabbit_queue_type:handle_down(QPid, QName, Reason, QStates0) of
        {ok, QStates, Actions} ->
            State1 = State0#state{queue_states = QStates},
            State = handle_queue_actions(Actions, State1),
            noreply(State);
        {eol, QStates, QRef} ->
            State = State0#state{queue_states = QStates,
                                 stashed_eol = [QRef | Eol]},
            noreply(State)
    end.

handle_cast({frame, Frame},
            #state{cfg = #cfg{writer_pid = WriterPid,
                              channel_num = Ch}} = State0) ->
    try handle_control(Frame, State0) of
        {reply, Replies, State} when is_list(Replies) ->
            lists:foreach(fun (Reply) ->
                                  rabbit_amqp_writer:send_command(WriterPid, Ch, Reply)
                          end, Replies),
            noreply(State);
        {reply, Reply, State} ->
            rabbit_amqp_writer:send_command(WriterPid, Ch, Reply),
            noreply(State);
        {noreply, State} ->
            noreply(State);
        {stop, _, _} = Stop ->
            Stop
    catch exit:#'v1_0.error'{} = Error ->
              log_error_and_close_session(Error, State0);
          exit:normal ->
              {stop, normal, State0};
          _:Reason:Stacktrace ->
              {stop, {Reason, Stacktrace}, State0}
    end;
handle_cast({queue_event, _, _} = QEvent, State0) ->
    try handle_queue_event(QEvent, State0) of
        State ->
            noreply_coalesce(State)
    catch exit:#'v1_0.error'{} = Error ->
              log_error_and_close_session(Error, State0)
    end;
handle_cast({conserve_resources, Alarm, Conserve},
            #state{incoming_window = IncomingWindow0,
                   cfg = #cfg{resource_alarms = Alarms0,
                              incoming_window_margin = Margin0,
                              writer_pid = WriterPid,
                              channel_num = Ch} = Cfg
                  } = State0) ->
    Alarms = case Conserve of
                 true -> sets:add_element(Alarm, Alarms0);
                 false -> sets:del_element(Alarm, Alarms0)
             end,
    {SendFlow, IncomingWindow, Margin} =
    case {sets:is_empty(Alarms0), sets:is_empty(Alarms)} of
        {true, false} ->
            %% Alarm kicked in.
            %% Notify the client to not send us any more TRANSFERs. Since we decrase
            %% our incoming window dynamically, there might be incoming in-flight
            %% TRANSFERs. So, let's be lax and allow for some excess TRANSFERs.
            {true, 0, ?MAX_INCOMING_WINDOW};
        {false, true} ->
            %% All alarms cleared.
            %% Notify the client that it can resume sending us TRANSFERs.
            {true, ?MAX_INCOMING_WINDOW, 0};
        _ ->
            {false, IncomingWindow0, Margin0}
    end,
    State = State0#state{incoming_window = IncomingWindow,
                         cfg = Cfg#cfg{resource_alarms = Alarms,
                                       incoming_window_margin = Margin}},
    case SendFlow of
        true ->
            Flow = session_flow_fields(#'v1_0.flow'{}, State),
            rabbit_amqp_writer:send_command(WriterPid, Ch, Flow);
        false ->
            ok
    end,
    noreply(State);
handle_cast(refresh_config, #state{cfg = #cfg{vhost = Vhost} = Cfg} = State0) ->
    State = State0#state{cfg = Cfg#cfg{trace_state = rabbit_trace:init(Vhost)}},
    noreply(State).

log_error_and_close_session(
  Error, State = #state{cfg = #cfg{reader_pid = ReaderPid,
                                   writer_pid = WriterPid,
                                   channel_num = Ch}}) ->
    End = #'v1_0.end'{error = Error},
    rabbit_log:warning("Closing session for connection ~p: ~tp",
                       [ReaderPid, Error]),
    ok = rabbit_amqp_writer:send_command_sync(WriterPid, Ch, End),
    {stop, {shutdown, Error}, State}.

%% Batch confirms / rejects to publishers.
noreply_coalesce(#state{stashed_rejected = [],
                        stashed_settled = [],
                        stashed_down = [],
                        stashed_eol = []} = State) ->
    noreply(State);
noreply_coalesce(State) ->
    Timeout = 0,
    {noreply, State, Timeout}.

noreply(State0) ->
    State = send_buffered(State0),
    {noreply, State}.

reply(Reply, State0) ->
    State = send_buffered(State0),
    {reply, Reply, State}.

send_buffered(State0) ->
    State = send_delivery_state_changes(State0),
    send_pending(State).

%% Send confirms / rejects to publishers.
send_delivery_state_changes(#state{stashed_rejected = [],
                                   stashed_settled = [],
                                   stashed_down = [],
                                   stashed_eol = []} = State) ->
    State;
send_delivery_state_changes(State0 = #state{cfg = #cfg{writer_pid = Writer,
                                                       channel_num = ChannelNum}}) ->
    %% Order is important:
    %% 1. Process queue rejections.
    {RejectedIds, GrantCredits0, State1} = handle_stashed_rejected(State0),
    send_dispositions(RejectedIds, #'v1_0.rejected'{}, Writer, ChannelNum),
    %% 2. Process queue confirmations.
    {AcceptedIds0, GrantCredits1, State2} = handle_stashed_settled(GrantCredits0, State1),
    %% 3. Process unavailable classic queues.
    {DetachFrames0, State3} = handle_stashed_down(State2),
    %% 4. Process queue deletions.
    {ReleasedIds, AcceptedIds1, DetachFrames, GrantCredits, State} = handle_stashed_eol(DetachFrames0, GrantCredits1, State3),
    send_dispositions(ReleasedIds, #'v1_0.released'{}, Writer, ChannelNum),
    AcceptedIds = AcceptedIds1 ++ AcceptedIds0,
    send_dispositions(AcceptedIds, #'v1_0.accepted'{}, Writer, ChannelNum),
    rabbit_global_counters:messages_confirmed(?PROTOCOL, length(AcceptedIds)),
    %% Send DETACH frames after DISPOSITION frames such that
    %% clients can handle DISPOSITIONs before closing their links.
    lists:foreach(fun(Frame) ->
                          rabbit_amqp_writer:send_command(Writer, ChannelNum, Frame)
                  end, DetachFrames),
    maps:foreach(fun(HandleInt, DeliveryCount) ->
                         F0 = flow(?UINT(HandleInt), DeliveryCount),
                         F = session_flow_fields(F0, State),
                         rabbit_amqp_writer:send_command(Writer, ChannelNum, F)
                 end, GrantCredits),
    State.

handle_stashed_rejected(#state{stashed_rejected = []} = State) ->
    {[], #{}, State};
handle_stashed_rejected(#state{stashed_rejected = Actions,
                               incoming_links = Links} = State0) ->
    {Ids, GrantCredits, Ls} =
    lists:foldl(
      fun({rejected, _QName, Correlations}, Accum) ->
              lists:foldl(
                fun({HandleInt, DeliveryId}, {Ids0, GrantCreds0, Links0} = Acc) ->
                        case Links0 of
                            #{HandleInt := Link0 = #incoming_link{incoming_unconfirmed_map = U0}} ->
                                case maps:take(DeliveryId, U0) of
                                    {{_, Settled, _}, U} ->
                                        Ids1 = case Settled of
                                                   true -> Ids0;
                                                   false -> [DeliveryId | Ids0]
                                               end,
                                        Link1 = Link0#incoming_link{incoming_unconfirmed_map = U},
                                        {Link, GrantCreds} = maybe_grant_link_credit(
                                                               HandleInt, Link1, GrantCreds0),
                                        {Ids1, GrantCreds, maps:update(HandleInt, Link, Links0)};
                                    error ->
                                        Acc
                                end;
                            _ ->
                                Acc
                        end
                end, Accum, Correlations)
      end, {[], #{}, Links}, Actions),

    State = State0#state{stashed_rejected = [],
                         incoming_links = Ls},
    {Ids, GrantCredits, State}.

handle_stashed_settled(GrantCredits, #state{stashed_settled = []} = State) ->
    {[], GrantCredits, State};
handle_stashed_settled(GrantCredits0, #state{stashed_settled = Actions,
                                             incoming_links = Links} = State0) ->
    {Ids, GrantCredits, Ls} =
    lists:foldl(
      fun({settled, QName, Correlations}, Accum) ->
              lists:foldl(
                fun({HandleInt, DeliveryId}, {Ids0, GrantCreds0, Links0} = Acc) ->
                        case Links0 of
                            #{HandleInt := Link0 = #incoming_link{incoming_unconfirmed_map = U0}} ->
                                case maps:take(DeliveryId, U0) of
                                    {{#{QName := _} = Qs, Settled, _}, U1} ->
                                        UnconfirmedQs = map_size(Qs),
                                        {Ids2, U} =
                                        if UnconfirmedQs =:= 1 ->
                                               %% last queue confirmed
                                               Ids1 = case Settled of
                                                          true -> Ids0;
                                                          false -> [DeliveryId | Ids0]
                                                      end,
                                               {Ids1, U1};
                                           UnconfirmedQs > 1 ->
                                               U2 = maps:update(
                                                      DeliveryId,
                                                      {maps:remove(QName, Qs), Settled, true},
                                                      U0),
                                               {Ids0, U2}
                                        end,
                                        Link1 = Link0#incoming_link{incoming_unconfirmed_map = U},
                                        {Link, GrantCreds} = maybe_grant_link_credit(
                                                               HandleInt, Link1, GrantCreds0),
                                        {Ids2, GrantCreds, maps:update(HandleInt, Link, Links0)};
                                    _ ->
                                        Acc
                                end;
                            _ ->
                                Acc
                        end
                end, Accum, Correlations)
      end, {[], GrantCredits0, Links}, Actions),

    State = State0#state{stashed_settled = [],
                         incoming_links = Ls},
    {Ids, GrantCredits, State}.

handle_stashed_down(#state{stashed_down = []} = State) ->
    {[], State};
handle_stashed_down(#state{stashed_down = QNames,
                           outgoing_links = OutgoingLinks0} = State0) ->
    %% We already processed queue actions settled and rejected for classic queues that are down.
    %% Here, we destroy any outgoing links that consume from unavailable classic queues.
    %% (This roughly corresponds to consumer_cancel_notify sent from server to client in AMQP 0.9.1.)
    {DetachFrames, OutgoingLinks} =
    lists:foldl(fun(#resource{name = QNameBinDown}, Acc = {_, OutgoingLinks1}) ->
                        maps:fold(fun(Handle, Link = #outgoing_link{queue_name_bin = QNameBin}, {Frames0, Links0})
                                        when QNameBin =:= QNameBinDown ->
                                          Detach = detach(Handle, Link, ?V_1_0_AMQP_ERROR_ILLEGAL_STATE),
                                          Frames = [Detach | Frames0],
                                          Links = maps:remove(Handle, Links0),
                                          {Frames, Links};
                                     (_, _, Accum) ->
                                          Accum
                                  end, Acc, OutgoingLinks1)
                end, {[], OutgoingLinks0}, QNames),
    State = State0#state{stashed_down = [],
                         outgoing_links = OutgoingLinks},
    {DetachFrames, State}.

handle_stashed_eol(DetachFrames, GrantCredits, #state{stashed_eol = []} = State) ->
    {[], [], DetachFrames, GrantCredits, State};
handle_stashed_eol(DetachFrames0, GrantCredits0, #state{stashed_eol = Eols} = State0) ->
    {ReleasedIs, AcceptedIds, DetachFrames, GrantCredits, State1} =
    lists:foldl(fun(QName, {RIds0, AIds0, DetachFrames1, GrantCreds0, S0 = #state{incoming_links = Links0,
                                                                                  queue_states = QStates0}}) ->
                        {RIds, AIds, GrantCreds1, Links} = settle_eol(QName, {RIds0, AIds0, GrantCreds0, Links0}),
                        QStates = rabbit_queue_type:remove(QName, QStates0),
                        S1 = S0#state{incoming_links = Links,
                                      queue_states = QStates},
                        {DetachFrames2, GrantCreds, S} = destroy_links(QName, DetachFrames1, GrantCreds1, S1),
                        {RIds, AIds, DetachFrames2, GrantCreds, S}
                end, {[], [], DetachFrames0, GrantCredits0, State0}, Eols),

    State = State1#state{stashed_eol = []},
    {ReleasedIs, AcceptedIds, DetachFrames, GrantCredits, State}.

settle_eol(QName, {_ReleasedIds, _AcceptedIds, _GrantCredits, Links} = Acc) ->
    maps:fold(fun(HandleInt,
                  #incoming_link{incoming_unconfirmed_map = U0} = Link0,
                  {RelIds0, AcceptIds0, GrantCreds0, Links0}) ->
                      {RelIds, AcceptIds, U} = settle_eol0(QName, {RelIds0, AcceptIds0, U0}),
                      Link1 = Link0#incoming_link{incoming_unconfirmed_map = U},
                      {Link, GrantCreds} = maybe_grant_link_credit(
                                             HandleInt, Link1, GrantCreds0),
                      Links1 = maps:update(HandleInt,
                                           Link,
                                           Links0),
                      {RelIds, AcceptIds, GrantCreds, Links1}
              end, Acc, Links).

settle_eol0(QName, {_ReleasedIds, _AcceptedIds, UnconfirmedMap} = Acc) ->
    maps:fold(
      fun(DeliveryId,
          {#{QName := _} = Qs, Settled, AtLeastOneQueueConfirmed},
          {RelIds, AcceptIds, U0}) ->
              UnconfirmedQs = map_size(Qs),
              if UnconfirmedQs =:= 1 ->
                     %% The last queue that this delivery ID was waiting a confirm for got deleted.
                     U = maps:remove(DeliveryId, U0),
                     case Settled of
                         true ->
                             {RelIds, AcceptIds, U};
                         false ->
                             case AtLeastOneQueueConfirmed of
                                 true ->
                                     %% Since at least one queue confirmed this message, we reply to
                                     %% the client with ACCEPTED. This allows e.g. for large fanout
                                     %% scenarios where temporary target queues are deleted
                                     %% (think about an MQTT subscriber disconnects).
                                     {RelIds, [DeliveryId | AcceptIds], U};
                                 false ->
                                     %% Since no queue confirmed this message, we reply to the client
                                     %% with RELEASED. (The client can then re-publish this message.)
                                     {[DeliveryId | RelIds], AcceptIds, U}
                             end
                     end;
                 UnconfirmedQs > 1 ->
                     U = maps:update(DeliveryId,
                                     {maps:remove(QName, Qs), Settled, AtLeastOneQueueConfirmed},
                                     U0),
                     {RelIds, AcceptIds, U}
              end;
         (_, _, A) ->
              A
      end, Acc, UnconfirmedMap).

destroy_links(#resource{kind = queue,
                        name = QNameBin},
              Frames0,
              GrantCredits0,
              #state{incoming_links = IncomingLinks0,
                     outgoing_links = OutgoingLinks0,
                     outgoing_unsettled_map = Unsettled0} = State0) ->
    {Frames1,
     GrantCredits,
     IncomingLinks} = maps:fold(fun(Handle, Link, Acc) ->
                                        destroy_incoming_link(Handle, Link, QNameBin, Acc)
                                end, {Frames0, GrantCredits0, IncomingLinks0}, IncomingLinks0),
    {Frames,
     Unsettled,
     OutgoingLinks} = maps:fold(fun(Handle, Link, Acc) ->
                                        destroy_outgoing_link(Handle, Link, QNameBin, Acc)
                                end, {Frames1, Unsettled0, OutgoingLinks0}, OutgoingLinks0),
    State = State0#state{incoming_links = IncomingLinks,
                         outgoing_links = OutgoingLinks,
                         outgoing_unsettled_map = Unsettled},
    {Frames, GrantCredits, State}.

destroy_incoming_link(Handle, Link = #incoming_link{queue_name_bin = QNameBin}, QNameBin, {Frames, GrantCreds, Links}) ->
    {[detach(Handle, Link, ?V_1_0_AMQP_ERROR_RESOURCE_DELETED) | Frames],
     %% Don't grant credits for a link that we destroy.
     maps:remove(Handle, GrantCreds),
     maps:remove(Handle, Links)};
destroy_incoming_link(_, _, _, Acc) ->
    Acc.

destroy_outgoing_link(Handle, Link = #outgoing_link{queue_name_bin = QNameBin}, QNameBin, {Frames, Unsettled0, Links}) ->
    {Unsettled, _RemovedMsgIds} = remove_link_from_outgoing_unsettled_map(Handle, Unsettled0),
    {[detach(Handle, Link, ?V_1_0_AMQP_ERROR_RESOURCE_DELETED) | Frames],
     Unsettled,
     maps:remove(Handle, Links)};
destroy_outgoing_link(_, _, _, Acc) ->
    Acc.

detach(Handle, Link, ErrorCondition) ->
    rabbit_log:warning("Detaching link handle ~b due to error condition: ~tp",
                       [Handle, ErrorCondition]),
    publisher_or_consumer_deleted(Link),
    #'v1_0.detach'{handle = ?UINT(Handle),
                   closed = true,
                   error = #'v1_0.error'{condition = ErrorCondition}}.

send_dispositions(Ids, DeliveryState, Writer, ChannelNum) ->
    Ranges = serial_number:ranges(Ids),
    lists:foreach(fun({First, Last}) ->
                          Disposition = disposition(DeliveryState, First, Last),
                          rabbit_amqp_writer:send_command(Writer, ChannelNum, Disposition)
                  end, Ranges).

disposition(DeliveryState, First, Last) ->
    Last1 = case First of
                Last ->
                    %% "If not set, this is taken to be the same as first." [2.7.6]
                    %% Save a few bytes.
                    undefined;
                _ ->
                    ?UINT(Last)
            end,
    #'v1_0.disposition'{
       role = ?AMQP_ROLE_RECEIVER,
       settled = true,
       state = DeliveryState,
       first = ?UINT(First),
       last = Last1}.

handle_control(#'v1_0.attach'{
                  role = ?AMQP_ROLE_SENDER,
                  snd_settle_mode = SndSettleMode,
                  name = Name = {utf8, LinkName},
                  handle = Handle = ?UINT(HandleInt),
                  source = Source = #'v1_0.source'{address = ClientTerminusAddress},
                  target = Target = #'v1_0.target'{address = {utf8, ?MANAGEMENT_NODE_ADDRESS}},
                  initial_delivery_count = DeliveryCount = ?UINT(DeliveryCountInt),
                  properties = Properties
                 } = Attach,
               #state{management_link_pairs = Pairs0,
                      incoming_management_links = Links
                     } = State0) ->
    ok = validate_attach(Attach),
    ok = check_paired(Properties),
    Pairs = case Pairs0 of
                #{LinkName := #management_link_pair{
                                 client_terminus_address = ClientTerminusAddress,
                                 incoming_half = unattached,
                                 outgoing_half = H} = Pair}
                  when is_integer(H) ->
                    maps:update(LinkName,
                                Pair#management_link_pair{incoming_half = HandleInt},
                                Pairs0);
                #{LinkName := Other} ->
                    protocol_error(?V_1_0_AMQP_ERROR_PRECONDITION_FAILED,
                                   "received invalid attach ~p for management link pair ~p",
                                   [Attach, Other]);
                _ ->
                    maps:put(LinkName,
                             #management_link_pair{client_terminus_address = ClientTerminusAddress,
                                                   incoming_half = HandleInt,
                                                   outgoing_half = unattached},
                             Pairs0)
            end,
    Credit = 1,
    MaxMessageSize = persistent_term:get(max_message_size),
    Link = #management_link{name = LinkName,
                            delivery_count = DeliveryCountInt,
                            credit = Credit,
                            max_message_size = MaxMessageSize},
    State = State0#state{management_link_pairs = Pairs,
                         incoming_management_links = maps:put(HandleInt, Link, Links)},
    Reply = #'v1_0.attach'{
               name = Name,
               handle = Handle,
               %% We are the receiver.
               role = ?AMQP_ROLE_RECEIVER,
               snd_settle_mode = SndSettleMode,
               rcv_settle_mode = ?V_1_0_RECEIVER_SETTLE_MODE_FIRST,
               source = Source,
               target = Target,
               max_message_size = {ulong, MaxMessageSize},
               properties = Properties},
    Flow = #'v1_0.flow'{handle = Handle,
                        delivery_count = DeliveryCount,
                        link_credit = ?UINT(Credit)},
    reply0([Reply, Flow], State);

handle_control(#'v1_0.attach'{
                  role = ?AMQP_ROLE_RECEIVER,
                  name = Name = {utf8, LinkName},
                  handle = Handle = ?UINT(HandleInt),
                  source = Source = #'v1_0.source'{address = {utf8, ?MANAGEMENT_NODE_ADDRESS}},
                  target = Target = #'v1_0.target'{address = ClientTerminusAddress},
                  rcv_settle_mode = RcvSettleMode,
                  max_message_size = MaybeMaxMessageSize,
                  properties = Properties
                 } = Attach,
               #state{management_link_pairs = Pairs0,
                      outgoing_management_links = Links
                     } = State0) ->
    ok = validate_attach(Attach),
    ok = check_paired(Properties),
    Pairs = case Pairs0 of
                #{LinkName := #management_link_pair{
                                 client_terminus_address = ClientTerminusAddress,
                                 incoming_half = H,
                                 outgoing_half = unattached} = Pair}
                  when is_integer(H) ->
                    maps:update(LinkName,
                                Pair#management_link_pair{outgoing_half = HandleInt},
                                Pairs0);
                #{LinkName := Other} ->
                    protocol_error(?V_1_0_AMQP_ERROR_PRECONDITION_FAILED,
                                   "received invalid attach ~p for management link pair ~p",
                                   [Attach, Other]);
                _ ->
                    maps:put(LinkName,
                             #management_link_pair{client_terminus_address = ClientTerminusAddress,
                                                   incoming_half = unattached,
                                                   outgoing_half = HandleInt},
                             Pairs0)
            end,
    MaxMessageSize = max_message_size(MaybeMaxMessageSize),
    Link = #management_link{name = LinkName,
                            delivery_count = ?INITIAL_DELIVERY_COUNT,
                            credit = 0,
                            max_message_size = MaxMessageSize},
    State = State0#state{management_link_pairs = Pairs,
                         outgoing_management_links = maps:put(HandleInt, Link, Links)},
    Reply = #'v1_0.attach'{
               name = Name,
               handle = Handle,
               role = ?AMQP_ROLE_SENDER,
               snd_settle_mode = ?V_1_0_SENDER_SETTLE_MODE_SETTLED,
               rcv_settle_mode = RcvSettleMode,
               source = Source,
               target = Target,
               initial_delivery_count = ?UINT(?INITIAL_DELIVERY_COUNT),
               %% Echo back that we will respect the client's requested max-message-size.
               max_message_size = MaybeMaxMessageSize,
               properties = Properties},
    reply0(Reply, State);

handle_control(#'v1_0.attach'{role = ?AMQP_ROLE_SENDER,
                              name = LinkName,
                              handle = Handle = ?UINT(HandleInt),
                              source = Source,
                              snd_settle_mode = SndSettleMode,
                              target = Target,
                              initial_delivery_count = DeliveryCount = ?UINT(DeliveryCountInt)
                             } = Attach,
               State0 = #state{incoming_links = IncomingLinks0,
                               cfg = #cfg{vhost = Vhost,
                                          user = User}}) ->
    ok = validate_attach(Attach),
    case ensure_target(Target, Vhost, User) of
        {ok, Exchange, RoutingKey, QNameBin} ->
            IncomingLink = #incoming_link{
                              exchange = Exchange,
                              routing_key = RoutingKey,
                              queue_name_bin = QNameBin,
                              delivery_count = DeliveryCountInt,
                              credit = ?LINK_CREDIT_RCV},
            _Outcomes = outcomes(Source),
            Reply = #'v1_0.attach'{
                       name = LinkName,
                       handle = Handle,
                       source = Source,
                       snd_settle_mode = SndSettleMode,
                       rcv_settle_mode = ?V_1_0_RECEIVER_SETTLE_MODE_FIRST,
                       target = Target,
                       %% We are the receiver.
                       role = ?AMQP_ROLE_RECEIVER,
                       max_message_size = {ulong, persistent_term:get(max_message_size)}},
            Flow = #'v1_0.flow'{handle = Handle,
                                delivery_count = DeliveryCount,
                                link_credit = ?UINT(?LINK_CREDIT_RCV)},
            %%TODO check that handle is not in use for any other open links.
            %%"The handle MUST NOT be used for other open links. An attempt to attach
            %% using a handle which is already associated with a link MUST be responded to
            %% with an immediate close carrying a handle-in-use session-error."
            IncomingLinks = IncomingLinks0#{HandleInt => IncomingLink},
            State = State0#state{incoming_links = IncomingLinks},
            rabbit_global_counters:publisher_created(?PROTOCOL),
            reply0([Reply, Flow], State);
        {error, Reason} ->
            %% TODO proper link establishment protocol here?
            protocol_error(?V_1_0_AMQP_ERROR_INVALID_FIELD,
                           "Attach rejected: ~tp",
                           [Reason])
    end;

handle_control(#'v1_0.attach'{role = ?AMQP_ROLE_RECEIVER,
                              name = LinkName,
                              handle = Handle = ?UINT(HandleInt),
                              source = Source,
                              snd_settle_mode = SndSettleMode,
                              rcv_settle_mode = RcvSettleMode,
                              max_message_size = MaybeMaxMessageSize} = Attach,
               State0 = #state{queue_states = QStates0,
                               outgoing_links = OutgoingLinks0,
                               cfg = #cfg{vhost = Vhost,
                                          user = User = #user{username = Username},
                                          reader_pid = ReaderPid}}) ->
    ok = validate_attach(Attach),
    {SndSettled, EffectiveSndSettleMode} = case SndSettleMode of
                                               ?V_1_0_SENDER_SETTLE_MODE_SETTLED ->
                                                   {true, SndSettleMode};
                                               _ ->
                                                   %% In the future, we might want to support sender settle
                                                   %% mode mixed where we would expect a settlement from the
                                                   %% client only for durable messages.
                                                   {false, ?V_1_0_SENDER_SETTLE_MODE_UNSETTLED}
                                           end,
    case ensure_source(Source, Vhost, User) of
        {error, Reason} ->
            protocol_error(?V_1_0_AMQP_ERROR_INVALID_FIELD, "Attach rejected: ~tp", [Reason]);
        {ok, QNameBin} ->
            QName = rabbit_misc:r(Vhost, queue, QNameBin),
            check_read_permitted(QName, User),
            case rabbit_amqqueue:with(
                   QName,
                   fun(Q) ->
                           try rabbit_amqqueue:check_exclusive_access(Q, ReaderPid)
                           catch exit:#amqp_error{name = resource_locked} ->
                                     %% An exclusive queue can only be consumed from by its declaring connection.
                                     protocol_error(
                                       ?V_1_0_AMQP_ERROR_RESOURCE_LOCKED,
                                       "cannot obtain exclusive access to locked ~s",
                                       [rabbit_misc:rs(QName)])
                           end,
                           QType = amqqueue:get_type(Q),
                           %% Whether credit API v1 or v2 is used is decided only here at link attachment time.
                           %% This decision applies to the whole life time of the link.
                           %% This means even when feature flag credit_api_v2 will be enabled later, this consumer will
                           %% continue to use credit API v1. This is the safest and easiest solution avoiding
                           %% transferring link flow control state (the delivery-count) at runtime from this session
                           %% process to the queue process.
                           %% Eventually, after feature flag credit_api_v2 gets enabled and a subsequent rolling upgrade,
                           %% all consumers will use credit API v2.
                           %% Streams always use credit API v2 since the stream client (rabbit_stream_queue) holds the link
                           %% flow control state. Hence, credit API mixed version isn't an issue for streams.
                           {Mode,
                            DeliveryCount} = case rabbit_feature_flags:is_enabled(credit_api_v2) orelse
                                                  QType =:= rabbit_stream_queue of
                                                 true ->
                                                     {{credited, ?INITIAL_DELIVERY_COUNT}, credit_api_v2};
                                                 false ->
                                                     {{credited, credit_api_v1}, {credit_api_v1, ?INITIAL_DELIVERY_COUNT}}
                                             end,
                           Spec = #{no_ack => SndSettled,
                                    channel_pid => self(),
                                    limiter_pid => none,
                                    limiter_active => false,
                                    mode => Mode,
                                    consumer_tag => handle_to_ctag(HandleInt),
                                    exclusive_consume => false,
                                    args => source_filters_to_consumer_args(Source),
                                    ok_msg => undefined,
                                    acting_user => Username},
                           case rabbit_queue_type:consume(Q, Spec, QStates0) of
                               {ok, QStates} ->
                                   A = #'v1_0.attach'{
                                          name = LinkName,
                                          handle = Handle,
                                          initial_delivery_count = ?UINT(?INITIAL_DELIVERY_COUNT),
                                          snd_settle_mode = EffectiveSndSettleMode,
                                          rcv_settle_mode = RcvSettleMode,
                                          %% The queue process monitors our session process. When our session process
                                          %% terminates (abnormally) any messages checked out to our session process
                                          %% will be requeued. That's why the we only support RELEASED as the default outcome.
                                          source = Source#'v1_0.source'{
                                                            default_outcome = #'v1_0.released'{},
                                                            outcomes = outcomes(Source)},
                                          role = ?AMQP_ROLE_SENDER,
                                          %% Echo back that we will respect the client's requested max-message-size.
                                          max_message_size = MaybeMaxMessageSize},
                                   MaxMessageSize = max_message_size(MaybeMaxMessageSize),
                                   Link = #outgoing_link{queue_name_bin = QNameBin,
                                                         queue_type = QType,
                                                         send_settled = SndSettled,
                                                         max_message_size = MaxMessageSize,
                                                         delivery_count = DeliveryCount},
                                   OutgoingLinks = OutgoingLinks0#{HandleInt => Link},
                                   State1 = State0#state{queue_states = QStates,
                                                         outgoing_links = OutgoingLinks},
                                   rabbit_global_counters:consumer_created(?PROTOCOL),
                                   {ok, [A], State1};
                               {error, Reason} ->
                                   protocol_error(
                                     ?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                                     "Consuming from ~s failed: ~tp",
                                     [rabbit_misc:rs(QName), Reason]);
                               {protocol_error, _Type, Reason, Args} ->
                                   protocol_error(
                                     ?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                                     Reason, Args)
                           end
                   end) of
                {ok, Reply, State} ->
                    reply0(Reply, State);
                {error, Reason} ->
                    protocol_error(
                      ?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                      "Could not operate on ~s: ~tp",
                      [rabbit_misc:rs(QName), Reason])
            end
    end;

handle_control({Txfr = #'v1_0.transfer'{handle = ?UINT(Handle)}, MsgPart},
               State0 = #state{incoming_links = IncomingLinks}) ->
    {Flows, State1} = session_flow_control_received_transfer(State0),

    {Reply, State} =
    case IncomingLinks of
        #{Handle := Link0} ->
            case incoming_link_transfer(Txfr, MsgPart, Link0, State1) of
                {ok, Reply0, Link, State2} ->
                    {Reply0, State2#state{incoming_links = IncomingLinks#{Handle := Link}}};
                {error, Reply0} ->
                    %% "When an error occurs at a link endpoint, the endpoint MUST be detached
                    %% with appropriate error information supplied in the error field of the
                    %% detach frame. The link endpoint MUST then be destroyed." [2.6.5]
                    {Reply0, State1#state{incoming_links = maps:remove(Handle, IncomingLinks)}}
            end;
        _ ->
            incoming_mgmt_link_transfer(Txfr, MsgPart, State1)
    end,
    reply0(Reply ++ Flows, State);


%% Although the AMQP message format [3.2] requires a body, it is valid to send a transfer frame without payload.
%% For example, when a large multi transfer message is streamed using the ProtonJ2 client, the client could send
%% a final #'v1_0.transfer'{more=false} frame without a payload.
handle_control(Txfr = #'v1_0.transfer'{}, State) ->
    handle_control({Txfr, <<>>}, State);

%% Flow control. These frames come with two pieces of information:
%% the session window, and optionally, credit for a particular link.
%% We'll deal with each of them separately.
handle_control(#'v1_0.flow'{handle = Handle} = Flow,
               #state{incoming_links = IncomingLinks,
                      outgoing_links = OutgoingLinks,
                      incoming_management_links = IncomingMgmtLinks,
                      outgoing_management_links = OutgoingMgmtLinks
                     } = State0) ->
    State = session_flow_control_received_flow(Flow, State0),
    S = case Handle of
            undefined ->
                %% "If not set, the flow frame is carrying only information
                %% pertaining to the session endpoint." [2.7.4]
                State;
            ?UINT(HandleInt) ->
                %% "If set, indicates that the flow frame carries flow state information
                %% for the local link endpoint associated with the given handle." [2.7.4]
                case OutgoingLinks of
                    #{HandleInt := OutgoingLink} ->
                        handle_outgoing_link_flow_control(OutgoingLink, Flow, State);
                    _ ->
                        case OutgoingMgmtLinks of
                            #{HandleInt := OutgoingMgmtLink} ->
                                handle_outgoing_mgmt_link_flow_control(OutgoingMgmtLink, Flow, State);
                            _ when is_map_key(HandleInt, IncomingLinks) orelse
                                   is_map_key(HandleInt, IncomingMgmtLinks) ->
                                %% We're being told about available messages at the sender.
                                State;
                            _ ->
                                %% "If set to a handle that is not currently associated with
                                %% an attached link, the recipient MUST respond by ending the
                                %% session with an unattached-handle session error." [2.7.4]
                                rabbit_log:warning(
                                  "Received Flow frame for unknown link handle: ~tp", [Flow]),
                                protocol_error(
                                  ?V_1_0_SESSION_ERROR_UNATTACHED_HANDLE,
                                  "Unattached link handle: ~b", [HandleInt])
                        end
                end
        end,
    {noreply, S};

handle_control(Detach = #'v1_0.detach'{handle = ?UINT(HandleInt)},
               State0 = #state{queue_states = QStates0,
                               incoming_links = IncomingLinks,
                               outgoing_links = OutgoingLinks0,
                               outgoing_unsettled_map = Unsettled0,
                               cfg = #cfg{
                                        vhost = Vhost,
                                        user = #user{username = Username}}}) ->
    Ctag = handle_to_ctag(HandleInt),
    %% TODO delete queue if closed flag is set to true? see 2.6.6
    %% TODO keep the state around depending on the lifetime
    {QStates, Unsettled, OutgoingLinks}
    = case maps:take(HandleInt, OutgoingLinks0) of
          {#outgoing_link{queue_name_bin = QNameBin}, OutgoingLinks1} ->
              QName = rabbit_misc:r(Vhost, queue, QNameBin),
              case rabbit_amqqueue:lookup(QName) of
                  {ok, Q} ->
                      %%TODO Consider adding a new rabbit_queue_type:remove_consumer API that - from the point of view of
                      %% the queue process - behaves as if our session process terminated: All messages checked out
                      %% to this consumer should be re-queued automatically instead of us requeueing them here after cancelling
                      %% consumption.
                      %% For AMQP legacy (and STOMP / MQTT) consumer cancellation not requeueing messages is a good approach as
                      %% clients may want to ack any in-flight messages.
                      %% For AMQP however, the consuming client can stop cancellations via link-credit=0 and drain=true being
                      %% sure that no messages are in flight before detaching the link. Hence, AMQP doesn't need the
                      %% rabbit_queue_type:cancel API semantics.
                      %% A rabbit_queue_type:remove_consumer API has also the advantage to simplify reasoning about clients
                      %% first detaching and then re-attaching to the same session with the same link handle (the handle
                      %% becomes available for re-use once a link is closed): This will result in the same consumer tag,
                      %% and we ideally disallow "updating" an AMQP consumer.
                      case rabbit_queue_type:cancel(Q, Ctag, undefined, Username, QStates0) of
                          {ok, QStates1} ->
                              {Unsettled1, MsgIds} = remove_link_from_outgoing_unsettled_map(Ctag, Unsettled0),
                              case MsgIds of
                                  [] ->
                                      {QStates1, Unsettled0, OutgoingLinks1};
                                  _ ->
                                      case rabbit_queue_type:settle(QName, requeue, Ctag, MsgIds, QStates1) of
                                          {ok, QStates2, _Actions = []} ->
                                              {QStates2, Unsettled1, OutgoingLinks1};
                                          {protocol_error, _ErrorType, Reason, ReasonArgs} ->
                                              protocol_error(?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                                                             Reason, ReasonArgs)
                                      end
                              end;
                          {error, Reason} ->
                              protocol_error(
                                ?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                                "Failed to cancel consuming from ~s: ~tp",
                                [rabbit_misc:rs(amqqueue:get_name(Q)), Reason])
                      end;
                  {error, not_found} ->
                      {Unsettled1, _RemovedMsgIds} = remove_link_from_outgoing_unsettled_map(Ctag, Unsettled0),
                      {QStates0, Unsettled1, OutgoingLinks1}
              end;
          error ->
              {Unsettled1, _RemovedMsgIds} = remove_link_from_outgoing_unsettled_map(Ctag, Unsettled0),
              {QStates0, Unsettled1, OutgoingLinks0}
      end,
    State1 = State0#state{queue_states = QStates,
                          incoming_links = maps:remove(HandleInt, IncomingLinks),
                          outgoing_links = OutgoingLinks,
                          outgoing_unsettled_map = Unsettled},
    State = maybe_detach_mgmt_link(HandleInt, State1),
    maybe_detach_reply(Detach, State, State0),
    publisher_or_consumer_deleted(State, State0),
    {noreply, State};

handle_control(#'v1_0.end'{},
               State0 = #state{cfg = #cfg{writer_pid = WriterPid,
                                          channel_num = Ch}}) ->
    State = send_delivery_state_changes(State0),
    ok = try rabbit_amqp_writer:send_command_sync(WriterPid, Ch, #'v1_0.end'{})
         catch exit:{Reason, {gen_server, call, _ArgList}}
                 when Reason =:= shutdown orelse
                      Reason =:= noproc ->
                   %% AMQP connection and therefore the writer process got already terminated
                   %% before we had the chance to synchronously end the session.
                   ok
         end,
    {stop, normal, State};

handle_control(#'v1_0.disposition'{role = ?AMQP_ROLE_RECEIVER,
                                   first = ?UINT(First),
                                   last = Last0,
                                   state = Outcome,
                                   settled = DispositionSettled} = Disposition,
               #state{outgoing_unsettled_map = UnsettledMap,
                      queue_states = QStates0} = State0) ->
    Last = case Last0 of
               ?UINT(L) ->
                   L;
               undefined ->
                   %% "If not set, this is taken to be the same as first." [2.7.6]
                   First
           end,
    UnsettledMapSize = map_size(UnsettledMap),
    case UnsettledMapSize of
        0 ->
            {noreply, State0};
        _ ->
            DispositionRangeSize = diff(Last, First) + 1,
            {Settled, UnsettledMap1} =
            case DispositionRangeSize =< UnsettledMapSize of
                true ->
                    %% It is cheaper to iterate over the range of settled delivery IDs.
                    serial_number:foldl(fun settle_delivery_id/2, {#{}, UnsettledMap}, First, Last);
                false ->
                    %% It is cheaper to iterate over the outgoing unsettled map.
                    maps:fold(
                      fun (DeliveryId,
                           #outgoing_unsettled{queue_name = QName,
                                               consumer_tag = Ctag,
                                               msg_id = MsgId} = Unsettled,
                           {SettledAcc, UnsettledAcc}) ->
                              case serial_number:in_range(DeliveryId, First, Last) of
                                  true ->
                                      SettledAcc1 = maps_update_with(
                                                      {QName, Ctag},
                                                      fun(MsgIds) -> [MsgId | MsgIds] end,
                                                      [MsgId],
                                                      SettledAcc),
                                      {SettledAcc1, UnsettledAcc};
                                  false ->
                                      {SettledAcc, UnsettledAcc#{DeliveryId => Unsettled}}
                              end
                      end,
                      {#{}, #{}}, UnsettledMap)
            end,

            SettleOp = settle_op_from_outcome(Outcome),
            {QStates, Actions} =
            maps:fold(
              fun({QName, Ctag}, MsgIds, {QS0, ActionsAcc}) ->
                      case rabbit_queue_type:settle(QName, SettleOp, Ctag, MsgIds, QS0) of
                          {ok, QS, Actions0} ->
                              messages_acknowledged(SettleOp, QName, QS, MsgIds),
                              {QS, ActionsAcc ++ Actions0};
                          {protocol_error, _ErrorType, Reason, ReasonArgs} ->
                              protocol_error(?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                                             Reason, ReasonArgs)
                      end
              end, {QStates0, []}, Settled),

            State1 = State0#state{outgoing_unsettled_map = UnsettledMap1,
                                  queue_states = QStates},
            Reply = case DispositionSettled of
                        true  -> [];
                        false -> [Disposition#'v1_0.disposition'{settled = true,
                                                                 role = ?AMQP_ROLE_SENDER}]
                    end,
            State = handle_queue_actions(Actions, State1),
            reply0(Reply, State)
    end;

handle_control(Frame, _State) ->
    protocol_error(?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                   "Unexpected frame ~tp",
                   [amqp10_framing:pprint(Frame)]).

send_pending(#state{remote_incoming_window = Space,
                    outgoing_pending = Buf0,
                    cfg = #cfg{writer_pid = WriterPid,
                               channel_num = Ch}} = State0) ->
    case queue:out(Buf0) of
        {empty, _} ->
            State0;
        {{value, #'v1_0.flow'{} = Flow0}, Buf} ->
            Flow = session_flow_fields(Flow0, State0),
            rabbit_amqp_writer:send_command(WriterPid, Ch, Flow),
            send_pending(State0#state{outgoing_pending = Buf});
        {{value, #pending_transfer{frames = Frames} = Pending}, Buf1}
          when Space > 0 ->
            {NumTransfersSent, Buf, State1} =
            case send_frames(WriterPid, Ch, Frames, Space) of
                {sent_all, SpaceLeft} ->
                    {Space - SpaceLeft,
                     Buf1,
                     record_outgoing_unsettled(Pending, State0)};
                {sent_some, Rest} ->
                    {Space,
                     queue:in_r(Pending#pending_transfer{frames = Rest}, Buf1),
                     State0}
            end,
            State2 = session_flow_control_sent_transfers(NumTransfersSent, State1),
            State = State2#state{outgoing_pending = Buf},
            send_pending(State);
        {{value, Pending = #pending_management_transfer{frames = Frames}}, Buf1}
          when Space > 0 ->
            {NumTransfersSent, Buf} =
            case send_frames(WriterPid, Ch, Frames, Space) of
                {sent_all, SpaceLeft} ->
                    {Space - SpaceLeft, Buf1};
                {sent_some, Rest} ->
                    {Space, queue:in_r(Pending#pending_management_transfer{frames = Rest}, Buf1)}
            end,
            State1 = session_flow_control_sent_transfers(NumTransfersSent, State0),
            State = State1#state{outgoing_pending = Buf},
            send_pending(State);
        _ when Space =:= 0 ->
            State0
    end.

send_frames(_, _, [], SpaceLeft) ->
    {sent_all, SpaceLeft};
send_frames(_, _, Rest, 0) ->
    {sent_some, Rest};
send_frames(Writer, Ch, [[Transfer, Sections] | Rest], SpaceLeft) ->
    rabbit_amqp_writer:send_command(Writer, Ch, Transfer, Sections),
    send_frames(Writer, Ch, Rest, SpaceLeft - 1).

record_outgoing_unsettled(#pending_transfer{queue_ack_required = true,
                                            delivery_id = DeliveryId,
                                            outgoing_unsettled = Unsettled},
                          #state{outgoing_unsettled_map = Map0} = State) ->
    %% Record by DeliveryId such that we will ack this message to the queue
    %% once we receive the DISPOSITION from the AMQP client.
    Map = Map0#{DeliveryId => Unsettled},
    State#state{outgoing_unsettled_map = Map};
record_outgoing_unsettled(#pending_transfer{queue_ack_required = false}, State) ->
    %% => 'snd-settle-mode' at attachment must have been 'settled'.
    %% => 'settled' field in TRANSFER must have been 'true'.
    %% => AMQP client won't ack this message.
    %% Also, queue client already acked to queue on behalf of us.
    State.

reply0([], State) ->
    {noreply, State};
reply0(Reply, State) ->
    {reply, session_flow_fields(Reply, State), State}.

%% Implements section "receiving a transfer" in 2.5.6
session_flow_control_received_transfer(
  #state{next_incoming_id = NextIncomingId,
         incoming_window = InWindow0,
         remote_outgoing_window = RemoteOutgoingWindow,
         cfg = #cfg{incoming_window_margin = Margin,
                    resource_alarms = Alarms}
        } = State) ->
    InWindow1 = InWindow0 - 1,
    case InWindow1 < -Margin of
        true ->
            protocol_error(
              ?V_1_0_SESSION_ERROR_WINDOW_VIOLATION,
              "incoming window violation (tolerated excess tranfers: ~b)",
              [Margin]);
        false ->
            ok
    end,
    {Flows, InWindow} = case InWindow1 =< (?MAX_INCOMING_WINDOW div 2) andalso
                             sets:is_empty(Alarms) of
                            true ->
                                %% We've reached halfway and there are no
                                %% disk or memory alarm, open the window.
                                {[#'v1_0.flow'{}], ?MAX_INCOMING_WINDOW};
                            false ->
                                {[], InWindow1}
                        end,
    {Flows, State#state{incoming_window = InWindow,
                        next_incoming_id = add(NextIncomingId, 1),
                        remote_outgoing_window = RemoteOutgoingWindow - 1}}.

%% Implements section "sending a transfer" in 2.5.6
session_flow_control_sent_transfers(
  NumTransfers,
  #state{remote_incoming_window = RemoteIncomingWindow,
         next_outgoing_id = NextOutgoingId} = State) ->
    State#state{remote_incoming_window = RemoteIncomingWindow - NumTransfers,
                next_outgoing_id = add(NextOutgoingId, NumTransfers)}.

settle_delivery_id(Current, {Settled, Unsettled} = Acc) ->
    case maps:take(Current, Unsettled) of
        {#outgoing_unsettled{queue_name = QName,
                             consumer_tag = Ctag,
                             msg_id = MsgId}, Unsettled1} ->
            Settled1 = maps_update_with(
                         {QName, Ctag},
                         fun(MsgIds) -> [MsgId | MsgIds] end,
                         [MsgId],
                         Settled),
            {Settled1, Unsettled1};
        error ->
            Acc
    end.

settle_op_from_outcome(#'v1_0.accepted'{}) ->
    complete;
settle_op_from_outcome(#'v1_0.rejected'{}) ->
    discard;
settle_op_from_outcome(#'v1_0.released'{}) ->
    requeue;
%% Keep the same Modified behaviour as in RabbitMQ 3.x
settle_op_from_outcome(#'v1_0.modified'{delivery_failed = true,
                                        undeliverable_here = UndelHere})
  when UndelHere =/= true ->
    requeue;
settle_op_from_outcome(#'v1_0.modified'{}) ->
    %% If delivery_failed is not true, we can't increment its delivery_count.
    %% So, we will have to reject without requeue.
    %%
    %% If undeliverable_here is true, this is not quite correct because
    %% undeliverable_here refers to the link, and not the message in general.
    %% However, we cannot filter messages from being assigned to individual consumers.
    %% That's why we will have to reject it without requeue.
    discard;
settle_op_from_outcome(Outcome) ->
    protocol_error(
      ?V_1_0_AMQP_ERROR_INVALID_FIELD,
      "Unrecognised state: ~tp in DISPOSITION",
      [Outcome]).

-spec flow({uint, link_handle()}, sequence_no()) -> #'v1_0.flow'{}.
flow(Handle, DeliveryCount) ->
    flow(Handle, DeliveryCount, ?LINK_CREDIT_RCV).

-spec flow({uint, link_handle()}, sequence_no(), non_neg_integer()) -> #'v1_0.flow'{}.
flow(Handle, DeliveryCount, LinkCredit) ->
    #'v1_0.flow'{handle = Handle,
                 delivery_count = ?UINT(DeliveryCount),
                 link_credit = ?UINT(LinkCredit)}.

session_flow_fields(Frames, State)
  when is_list(Frames) ->
    [session_flow_fields(F, State) || F <- Frames];
session_flow_fields(Flow = #'v1_0.flow'{},
                    #state{next_outgoing_id = NextOutgoingId,
                           next_incoming_id = NextIncomingId,
                           incoming_window = IncomingWindow}) ->
    Flow#'v1_0.flow'{
           next_outgoing_id = ?UINT(NextOutgoingId),
           outgoing_window = ?UINT_OUTGOING_WINDOW,
           next_incoming_id = ?UINT(NextIncomingId),
           incoming_window = ?UINT(IncomingWindow)};
session_flow_fields(Frame, _State) ->
    Frame.

%% Implements section "receiving a flow" in 2.5.6
session_flow_control_received_flow(
  #'v1_0.flow'{next_incoming_id = FlowNextIncomingId,
               incoming_window = ?UINT(FlowIncomingWindow),
               next_outgoing_id = ?UINT(FlowNextOutgoingId),
               outgoing_window = ?UINT(FlowOutgoingWindow)},
  #state{next_outgoing_id = NextOutgoingId} = State) ->

    Seq = case FlowNextIncomingId of
              ?UINT(Id) ->
                  case compare(Id, NextOutgoingId) of
                      greater ->
                          protocol_error(
                            ?V_1_0_SESSION_ERROR_WINDOW_VIOLATION,
                            "next-incoming-id from FLOW (~b) leads next-outgoing-id (~b)",
                            [Id, NextOutgoingId]);
                      _ ->
                          Id
                  end;
              undefined ->
                  %% The AMQP client might not have yet received our #begin.next_outgoing_id
                  ?INITIAL_OUTGOING_TRANSFER_ID
          end,

    RemoteIncomingWindow0 = diff(add(Seq, FlowIncomingWindow), NextOutgoingId),
    %% RemoteIncomingWindow0 can be negative, for example if we sent a TRANSFER to the
    %% client between the point in time the client sent us a FLOW with updated
    %% incoming_window=0 and we received that FLOW. Whether 0 or negative doesn't matter:
    %% In both cases we're blocked sending more TRANSFERs to the client until it sends us
    %% a new FLOW with a positive incoming_window. For better understandibility
    %% across the code base, we ensure a floor of 0 here.
    RemoteIncomingWindow = max(0, RemoteIncomingWindow0),

    State#state{next_incoming_id = FlowNextOutgoingId,
                remote_outgoing_window = FlowOutgoingWindow,
                remote_incoming_window = RemoteIncomingWindow}.

% TODO: validate effective settle modes against
%       those declared during attach

% TODO: handle aborted transfers

handle_queue_event({queue_event, QRef, Evt},
                   #state{queue_states = QStates0} = S0) ->
    case rabbit_queue_type:handle_event(QRef, Evt, QStates0) of
        {ok, QStates1, Actions} ->
            S = S0#state{queue_states = QStates1},
            handle_queue_actions(Actions, S);
        {eol, Actions} ->
            S = handle_queue_actions(Actions, S0),
            S#state{stashed_eol = [QRef | S#state.stashed_eol]};
        {protocol_error, _Type, Reason, ReasonArgs} ->
            protocol_error(?V_1_0_AMQP_ERROR_INTERNAL_ERROR, Reason, ReasonArgs)
    end.

handle_queue_actions(Actions, State) ->
    lists:foldl(
      fun ({settled, _QName, _DelIds} = Action, S = #state{stashed_settled = As}) ->
              S#state{stashed_settled = [Action | As]};
          ({rejected, _QName, _DelIds} = Action, S = #state{stashed_rejected = As}) ->
              S#state{stashed_rejected = [Action | As]};
          ({deliver, CTag, AckRequired, Msgs}, S0) ->
              lists:foldl(fun(Msg, S) ->
                                  handle_deliver(CTag, AckRequired, Msg, S)
                          end, S0, Msgs);
          ({credit_reply, Ctag, DeliveryCount, Credit, Available,  Drain},
           S = #state{outgoing_pending = Pending}) ->
              %% credit API v2
              Handle = ctag_to_handle(Ctag),
              Flow = #'v1_0.flow'{
                        handle = ?UINT(Handle),
                        delivery_count = ?UINT(DeliveryCount),
                        link_credit = ?UINT(Credit),
                        available = ?UINT(Available),
                        drain = Drain},
              S#state{outgoing_pending = queue:in(Flow, Pending)};
          ({credit_reply_v1, Ctag, Credit0, Available, Drain},
           S0 = #state{outgoing_links = OutgoingLinks0,
                       outgoing_pending = Pending}) ->
              %% credit API v1
              %% Delete this branch when feature flag credit_api_v2 becomes required.
              Handle = ctag_to_handle(Ctag),
              Link = #outgoing_link{delivery_count = {credit_api_v1, Count0}} = maps:get(Handle, OutgoingLinks0),
              {Count, Credit, S} = case Drain of
                                       true ->
                                           Count1 = add(Count0, Credit0),
                                           OutgoingLinks = maps:update(
                                                             Handle,
                                                             Link#outgoing_link{delivery_count = {credit_api_v1,  Count1}},
                                                             OutgoingLinks0),
                                           S1 = S0#state{outgoing_links = OutgoingLinks},
                                           {Count1, 0, S1};
                                       false ->
                                           {Count0, Credit0, S0}
                                   end,
              Flow = #'v1_0.flow'{
                        handle = ?UINT(Handle),
                        delivery_count = ?UINT(Count),
                        link_credit = ?UINT(Credit),
                        available = ?UINT(Available),
                        drain = Drain},
              S#state{outgoing_pending = queue:in(Flow, Pending)};
          ({queue_down, QName}, S = #state{stashed_down = L}) ->
              S#state{stashed_down = [QName | L]};
          ({Action, _QName}, S)
            when Action =:= block orelse
                 Action =:= unblock ->
              %% Ignore since we rely on our own mechanism to detect if a client sends to fast
              %% into a link: If the number of outstanding queue confirmations grows,
              %% we won't grant new credits to publishers.
              S
      end, State, Actions).

handle_deliver(ConsumerTag, AckRequired,
               Msg = {QName, _QPid, MsgId, Redelivered, Mc0},
               State = #state{outgoing_pending = Pending,
                              outgoing_delivery_id = DeliveryId,
                              outgoing_links = OutgoingLinks0,
                              cfg = #cfg{outgoing_max_frame_size = MaxFrameSize,
                                         conn_name = ConnName,
                                         channel_num = ChannelNum,
                                         user = #user{username = Username},
                                         trace_state = Trace}}) ->
    Handle = ctag_to_handle(ConsumerTag),
    case OutgoingLinks0 of
        #{Handle := #outgoing_link{queue_type = QType,
                                   send_settled = SendSettled,
                                   max_message_size = MaxMessageSize,
                                   delivery_count = DelCount} = Link0} ->
            Dtag = delivery_tag(MsgId, SendSettled),
            Transfer = #'v1_0.transfer'{
                          handle = ?UINT(Handle),
                          delivery_id = ?UINT(DeliveryId),
                          delivery_tag = {binary, Dtag},
                          message_format = ?UINT(?MESSAGE_FORMAT),
                          settled = SendSettled},
            Mc1 = mc:convert(mc_amqp, Mc0),
            Mc = mc:set_annotation(redelivered, Redelivered, Mc1),
            Sections0 = mc:protocol_state(Mc),
            Sections = mc_amqp:serialize(Sections0),
            ?DEBUG("~s Outbound content:~n  ~tp~n",
                   [?MODULE, [amqp10_framing:pprint(Section) ||
                              Section <- amqp10_framing:decode_bin(iolist_to_binary(Sections))]]),
            validate_message_size(Sections, MaxMessageSize),
            Frames = transfer_frames(Transfer, Sections, MaxFrameSize),
            messages_delivered(Redelivered, QType),
            rabbit_trace:tap_out(Msg, ConnName, ChannelNum, Username, Trace),
            OutgoingLinks = case DelCount of
                                credit_api_v2 ->
                                    OutgoingLinks0;
                                {credit_api_v1, C} ->
                                    Link = Link0#outgoing_link{delivery_count = {credit_api_v1, add(C, 1)}},
                                    maps:update(Handle, Link, OutgoingLinks0)
                            end,
            Del = #outgoing_unsettled{
                     msg_id = MsgId,
                     consumer_tag = ConsumerTag,
                     queue_name = QName},
            PendingTransfer = #pending_transfer{
                                 frames = Frames,
                                 queue_ack_required = AckRequired,
                                 delivery_id = DeliveryId,
                                 outgoing_unsettled = Del},
            State#state{outgoing_pending = queue:in(PendingTransfer, Pending),
                        outgoing_delivery_id = add(DeliveryId, 1),
                        outgoing_links = OutgoingLinks};
        _ ->
            %% TODO handle missing link -- why does the queue think it's there?
            rabbit_log:warning(
              "No link handle ~b exists for delivery with consumer tag ~p from queue ~tp",
              [Handle, ConsumerTag, QName]),
            State
    end.

%% "The delivery-tag MUST be unique amongst all deliveries that could be
%% considered unsettled by either end of the link." [2.6.12]
delivery_tag(MsgId, _)
  when is_integer(MsgId) ->
    %% We use MsgId (the consumer scoped sequence number from the queue) as
    %% delivery-tag since delivery-tag must be unique only per link (not per session).
    %% "A delivery-tag can be up to 32 octets of binary data." [2.8.7]
    case MsgId =< ?UINT_MAX of
        true -> <<MsgId:32>>;
        false -> <<MsgId:64>>
    end;
delivery_tag(undefined, true) ->
    %% Both ends of the link will always consider this message settled because
    %% "the sender will send all deliveries settled to the receiver" [3.8.2].
    %% Hence, the delivery tag does not have to be unique on this link.
    %% However, the spec still mandates to send a delivery tag.
    <<>>;
%% Message comes from a (classic) priority queue.
delivery_tag({Priority, undefined}, true)
  when is_integer(Priority) ->
    <<>>;
delivery_tag(MsgId = {Priority, Seq}, _)
  when is_integer(Priority) andalso
       is_integer(Seq) ->
    term_to_binary(MsgId).

%%%%%%%%%%%%%%%%%%%%%
%%% Incoming Link %%%
%%%%%%%%%%%%%%%%%%%%%

incoming_mgmt_link_transfer(
  #'v1_0.transfer'{
     %%TODO confirm or reject incoming message if incoming settled=false
     delivery_id = _MaybeIncomingDeliveryId,
     settled = MaybeSettled,
     %% In the current implementation, we disallow large incoming management request messages.
     more = false,
     rcv_settle_mode = RcvSettleMode,
     handle = IncomingHandle = ?UINT(IncomingHandleInt)},
  Request,
  #state{management_link_pairs = LinkPairs,
         incoming_management_links = IncomingLinks,
         outgoing_management_links = OutgoingLinks,
         outgoing_pending = Pending,
         outgoing_delivery_id = OutgoingDeliveryId,
         cfg = #cfg{outgoing_max_frame_size = MaxFrameSize,
                    vhost = Vhost,
                    user = User,
                    reader_pid = ReaderPid}
        } = State0) ->

    IncomingLink0 = case maps:find(IncomingHandleInt, IncomingLinks) of
                        {ok, Link} ->
                            Link;
                        error ->
                            protocol_error(?V_1_0_AMQP_ERROR_ILLEGAL_STATE,
                                           "Unknown link handle: ~p", [IncomingHandleInt])
                    end,
    #management_link{name = Name,
                     delivery_count = IncomingDeliveryCount0,
                     credit = 1,
                     max_message_size = IncomingMaxMessageSize
                    } = IncomingLink0,
    #management_link_pair{
       incoming_half = IncomingHandleInt,
       outgoing_half = OutgoingHandleInt
      } = maps:get(Name, LinkPairs),
    OutgoingLink0 = case OutgoingHandleInt of
                        unattached ->
                            protocol_error(?V_1_0_AMQP_ERROR_PRECONDITION_FAILED,
                                           "received transfer on half open management link pair", []);
                        _ ->
                            maps:get(OutgoingHandleInt, OutgoingLinks)
                    end,
    #management_link{name = Name,
                     delivery_count = OutgoingDeliveryCount,
                     credit = OutgoingCredit,
                     max_message_size = OutgoingMaxMessageSize} = OutgoingLink0,
    case OutgoingCredit > 0 of
        true ->
            ok;
        false ->
            protocol_error(?V_1_0_AMQP_ERROR_PRECONDITION_FAILED,
                           "insufficient credit (~b) for management link from server to client",
                           [OutgoingCredit])
    end,
    Settled = default(MaybeSettled, false),
    validate_transfer_rcv_settle_mode(RcvSettleMode, Settled),
    validate_message_size(Request, IncomingMaxMessageSize),
    Response = rabbit_amqp_management:handle_request(Request, Vhost, User, ReaderPid),

    Transfer = #'v1_0.transfer'{
                  handle = ?UINT(OutgoingHandleInt),
                  delivery_id = ?UINT(OutgoingDeliveryId),
                  delivery_tag = {binary, <<>>},
                  message_format = ?UINT(?MESSAGE_FORMAT),
                  settled = true},
    ?DEBUG("~s Outbound content:~n  ~tp~n",
           [?MODULE, [amqp10_framing:pprint(Section) ||
                      Section <- amqp10_framing:decode_bin(iolist_to_binary(Response))]]),
    validate_message_size(Response, OutgoingMaxMessageSize),
    Frames = transfer_frames(Transfer, Response, MaxFrameSize),
    PendingTransfer = #pending_management_transfer{frames = Frames},
    IncomingDeliveryCount = add(IncomingDeliveryCount0, 1),
    IncomingLink = IncomingLink0#management_link{delivery_count = IncomingDeliveryCount},
    OutgoingLink = OutgoingLink0#management_link{delivery_count = add(OutgoingDeliveryCount, 1),
                                                 credit = OutgoingCredit - 1},
    State = State0#state{
              outgoing_delivery_id = add(OutgoingDeliveryId, 1),
              outgoing_pending = queue:in(PendingTransfer, Pending),
              incoming_management_links = maps:update(IncomingHandleInt, IncomingLink, IncomingLinks),
              outgoing_management_links = maps:update(OutgoingHandleInt, OutgoingLink, OutgoingLinks)},

    Flow = flow(IncomingHandle, IncomingDeliveryCount, 1),
    {[Flow], State}.

incoming_link_transfer(
  #'v1_0.transfer'{more = true,
                   %% "The delivery-id MUST be supplied on the first transfer of a multi-transfer delivery."
                   delivery_id = ?UINT(DeliveryId),
                   settled = Settled},
  MsgPart,
  Link0 = #incoming_link{multi_transfer_msg = undefined},
  State) ->
    %% This is the first transfer of a multi-transfer message.
    Link = Link0#incoming_link{
             multi_transfer_msg = #multi_transfer_msg{
                                     payload_fragments_rev = [MsgPart],
                                     delivery_id = DeliveryId,
                                     %% "If not set on the first (or only) transfer for a (multi-transfer)
                                     %% delivery, then the settled flag MUST be interpreted as being false."
                                     settled = default(Settled, false)}},
    {ok, [], Link, State};
incoming_link_transfer(
  #'v1_0.transfer'{more = true,
                   delivery_id = DeliveryId,
                   settled = Settled},
  MsgPart,
  Link0 = #incoming_link{
             multi_transfer_msg = Multi = #multi_transfer_msg{
                                             payload_fragments_rev = PFR0,
                                             delivery_id = FirstDeliveryId,
                                             settled = FirstSettled}},
  State) ->
    %% This is a continuation transfer with even more transfers to come.
    validate_multi_transfer_delivery_id(DeliveryId, FirstDeliveryId),
    validate_multi_transfer_settled(Settled, FirstSettled),
    PFR = [MsgPart | PFR0],
    validate_incoming_message_size(PFR),
    Link = Link0#incoming_link{multi_transfer_msg = Multi#multi_transfer_msg{payload_fragments_rev = PFR}},
    {ok, [], Link, State};
incoming_link_transfer(
  #'v1_0.transfer'{handle = ?UINT(HandleInt)},
  _,
  #incoming_link{credit = Credit} = Link,
  _)
  when Credit =< 0 ->
    Detach = detach(HandleInt, Link, ?V_1_0_LINK_ERROR_TRANSFER_LIMIT_EXCEEDED),
    {error, [Detach]};
incoming_link_transfer(
  #'v1_0.transfer'{delivery_id = MaybeDeliveryId,
                   delivery_tag = DeliveryTag,
                   settled = MaybeSettled,
                   rcv_settle_mode = RcvSettleMode,
                   handle = Handle = ?UINT(HandleInt)},
  MsgPart,
  #incoming_link{exchange = Exchange,
                 routing_key = LinkRKey,
                 delivery_count = DeliveryCount0,
                 incoming_unconfirmed_map = U0,
                 credit = Credit0,
                 multi_transfer_msg = MultiTransfer
                } = Link0,
  State0 = #state{queue_states = QStates0,
                  cfg = #cfg{user = User = #user{username = Username},
                             trace_state = Trace,
                             conn_name = ConnName,
                             channel_num = ChannelNum}}) ->

    {MsgBin, DeliveryId, Settled} =
    case MultiTransfer of
        undefined ->
            ?UINT(DeliveryId0) = MaybeDeliveryId,
            {MsgPart, DeliveryId0, default(MaybeSettled, false)};
        #multi_transfer_msg{payload_fragments_rev = PFR,
                            delivery_id = FirstDeliveryId,
                            settled = FirstSettled} ->
            MsgBin0 = iolist_to_binary(lists:reverse([MsgPart | PFR])),
            ok = validate_multi_transfer_delivery_id(MaybeDeliveryId, FirstDeliveryId),
            ok = validate_multi_transfer_settled(MaybeSettled, FirstSettled),
            {MsgBin0, FirstDeliveryId, FirstSettled}
    end,
    validate_transfer_rcv_settle_mode(RcvSettleMode, Settled),
    validate_incoming_message_size(MsgBin),

    Sections = amqp10_framing:decode_bin(MsgBin),
    ?DEBUG("~s Inbound content:~n  ~tp",
           [?MODULE, [amqp10_framing:pprint(Section) || Section <- Sections]]),
    case rabbit_exchange_lookup(Exchange) of
        {ok, X = #exchange{name = #resource{name = XNameBin}}} ->
            Anns = #{?ANN_EXCHANGE => XNameBin},
            Mc0 = mc:init(mc_amqp, Sections, Anns),
            {RoutingKey, Mc1} = ensure_routing_key(LinkRKey, Mc0),
            Mc = rabbit_message_interceptor:intercept(Mc1),
            check_user_id(Mc, User),
            messages_received(Settled),
            check_write_permitted_on_topic(X, User, RoutingKey),
            QNames = rabbit_exchange:route(X, Mc, #{return_binding_keys => true}),
            rabbit_trace:tap_in(Mc, QNames, ConnName, ChannelNum, Username, Trace),
            Opts = #{correlation => {HandleInt, DeliveryId}},
            Qs0 = rabbit_amqqueue:lookup_many(QNames),
            Qs = rabbit_amqqueue:prepend_extra_bcc(Qs0),
            case rabbit_queue_type:deliver(Qs, Mc, Opts, QStates0) of
                {ok, QStates, Actions} ->
                    State1 = State0#state{queue_states = QStates},
                    %% Confirms must be registered before processing actions
                    %% because actions may contain rejections of publishes.
                    {U, Reply0} = process_routing_confirm(
                                    Qs, Settled, DeliveryId, U0),
                    State = handle_queue_actions(Actions, State1),
                    DeliveryCount = add(DeliveryCount0, 1),
                    Credit1 = Credit0 - 1,
                    {Credit, Reply1} = maybe_grant_link_credit(
                                         Credit1, DeliveryCount, map_size(U), Handle),
                    Reply = Reply0 ++ Reply1,
                    Link = Link0#incoming_link{
                             delivery_count = DeliveryCount,
                             credit = Credit,
                             incoming_unconfirmed_map = U,
                             multi_transfer_msg = undefined},
                    {ok, Reply, Link, State};
                {error, Reason} ->
                    protocol_error(?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                                   "Failed to deliver message to queues, "
                                   "delivery_tag=~p, delivery_id=~p, reason=~p",
                                   [DeliveryTag, DeliveryId, Reason])
            end;
        {error, not_found} ->
            Disposition = released(DeliveryId),
            Detach = detach(HandleInt, Link0, ?V_1_0_AMQP_ERROR_RESOURCE_DELETED),
            {error, [Disposition, Detach]}
    end.

ensure_routing_key(LinkRKey, Mc0) ->
    RKey = case LinkRKey of
               undefined ->
                   case mc:property(subject, Mc0) of
                       undefined ->
                           %% Set the default routing key of AMQP 0.9.1 'basic.publish'{}.
                           %% For example, when the client attached to target /exchange/amq.fanout and sends a
                           %% message without setting a 'subject' in the message properties, the routing key is
                           %% ignored during routing, but receiving code paths still expect some routing key to be set.
                           <<"">>;
                       {utf8, Subject} ->
                           Subject
                   end;
               _ ->
                   LinkRKey
           end,
    Mc = mc:set_annotation(?ANN_ROUTING_KEYS, [RKey], Mc0),
    {RKey, Mc}.

rabbit_exchange_lookup(X = #exchange{}) ->
    {ok, X};
rabbit_exchange_lookup(XName = #resource{}) ->
    rabbit_exchange:lookup(XName).

process_routing_confirm([], _SenderSettles = true, _, U) ->
    rabbit_global_counters:messages_unroutable_dropped(?PROTOCOL, 1),
    {U, []};
process_routing_confirm([], _SenderSettles = false, DeliveryId, U) ->
    rabbit_global_counters:messages_unroutable_returned(?PROTOCOL, 1),
    Disposition = released(DeliveryId),
    {U, [Disposition]};
process_routing_confirm([_|_] = Qs, SenderSettles, DeliveryId, U0) ->
    QNames = rabbit_amqqueue:queue_names(Qs),
    false = maps:is_key(DeliveryId, U0),
    Map = maps:from_keys(QNames, ok),
    U = U0#{DeliveryId => {Map, SenderSettles, false}},
    rabbit_global_counters:messages_routed(?PROTOCOL, map_size(Map)),
    {U, []}.

released(DeliveryId) ->
    #'v1_0.disposition'{role = ?AMQP_ROLE_RECEIVER,
                        first = ?UINT(DeliveryId),
                        settled = true,
                        state = #'v1_0.released'{}}.

maybe_grant_link_credit(Credit, DeliveryCount, NumUnconfirmed, Handle) ->
    case grant_link_credit(Credit, NumUnconfirmed) of
        true ->
            {?LINK_CREDIT_RCV, [flow(Handle, DeliveryCount)]};
        false ->
            {Credit, []}
    end.

maybe_grant_link_credit(
  HandleInt,
  Link = #incoming_link{credit = Credit,
                        incoming_unconfirmed_map = U,
                        delivery_count = DeliveryCount},
  AccMap) ->
    case grant_link_credit(Credit, map_size(U)) of
        true ->
            {Link#incoming_link{credit = ?LINK_CREDIT_RCV},
             AccMap#{HandleInt => DeliveryCount}};
        false ->
            {Link, AccMap}
    end.

grant_link_credit(Credit, NumUnconfirmed) ->
    Credit =< ?LINK_CREDIT_RCV / 2 andalso
    NumUnconfirmed < ?LINK_CREDIT_RCV.

%% TODO default-outcome and outcomes, dynamic lifetimes
ensure_target(#'v1_0.target'{dynamic = true}, _, _) ->
    protocol_error(?V_1_0_AMQP_ERROR_NOT_IMPLEMENTED,
                   "Dynamic targets not supported", []);
ensure_target(#'v1_0.target'{address = Address,
                             durable = Durable}, Vhost, User) ->
    case Address of
        {utf8, Destination} ->
            case rabbit_routing_parser:parse_endpoint(Destination, true) of
                {ok, Dest} ->
                    QNameBin = ensure_terminus(target, Dest, Vhost, User, Durable),
                    {XNameList1, RK} = rabbit_routing_parser:parse_routing(Dest),
                    XNameBin = unicode:characters_to_binary(XNameList1),
                    XName = rabbit_misc:r(Vhost, exchange, XNameBin),
                    {ok, X} = rabbit_exchange:lookup(XName),
                    check_internal_exchange(X),
                    check_write_permitted(XName, User),
                    %% Pre-declared exchanges are protected against deletion and modification.
                    %% Let's cache the whole #exchange{} record to save a
                    %% rabbit_exchange:lookup(XName) call each time we receive a message.
                    Exchange = case XNameBin of
                                   <<>> -> X;
                                   <<"amq.", _/binary>> -> X;
                                   _ -> XName
                               end,
                    RoutingKey = case RK of
                                     undefined -> undefined;
                                     []        -> undefined;
                                     _         -> unicode:characters_to_binary(RK)
                                 end,
                    {ok, Exchange, RoutingKey, QNameBin};
                {error, _} = E ->
                    E
            end;
        _Else ->
            {error, {address_not_utf8_string, Address}}
    end.

handle_outgoing_mgmt_link_flow_control(
  #management_link{delivery_count = DeliveryCountSnd} = Link0,
  #'v1_0.flow'{handle = Handle = ?UINT(HandleInt),
               delivery_count = MaybeDeliveryCountRcv,
               link_credit = ?UINT(LinkCreditRcv),
               drain = Drain0,
               echo = Echo0},
  #state{outgoing_management_links = Links0,
         outgoing_pending = Pending
        } = State0) ->
    Drain = default(Drain0, false),
    Echo = default(Echo0, false),
    DeliveryCountRcv = delivery_count_rcv(MaybeDeliveryCountRcv),
    LinkCreditSnd = link_credit_snd(DeliveryCountRcv, LinkCreditRcv, DeliveryCountSnd),
    {Count, Credit} = case Drain of
                          true -> {add(DeliveryCountSnd, LinkCreditSnd), 0};
                          false -> {DeliveryCountSnd, LinkCreditSnd}
                      end,
    State = case Echo orelse Drain of
                true ->
                    Flow = #'v1_0.flow'{
                              handle = Handle,
                              delivery_count = ?UINT(Count),
                              link_credit = ?UINT(Credit),
                              available = ?UINT(0),
                              drain = Drain},
                    State0#state{outgoing_pending = queue:in(Flow, Pending)};
                false ->
                    State0
            end,
    Link = Link0#management_link{delivery_count = Count,
                                 credit = Credit},
    Links = maps:update(HandleInt, Link, Links0),
    State#state{outgoing_management_links = Links}.

handle_outgoing_link_flow_control(
  #outgoing_link{queue_name_bin = QNameBin,
                 delivery_count = MaybeDeliveryCountSnd},
  #'v1_0.flow'{handle = ?UINT(HandleInt),
               delivery_count = MaybeDeliveryCountRcv,
               link_credit = ?UINT(LinkCreditRcv),
               drain = Drain0,
               echo = Echo0},
  State0 = #state{queue_states = QStates0,
                  cfg = #cfg{vhost = Vhost}}) ->
    QName = rabbit_misc:r(Vhost, queue, QNameBin),
    Ctag = handle_to_ctag(HandleInt),
    DeliveryCountRcv = delivery_count_rcv(MaybeDeliveryCountRcv),
    Drain = default(Drain0, false),
    Echo = default(Echo0, false),
    case MaybeDeliveryCountSnd of
        credit_api_v2 ->
            {ok, QStates, Actions} = rabbit_queue_type:credit(
                                       QName, Ctag, DeliveryCountRcv, LinkCreditRcv, Drain, Echo, QStates0),
            State1 = State0#state{queue_states = QStates},
            State = handle_queue_actions(Actions, State1),
            %% We'll handle the credit_reply queue event async later
            %% thanks to the queue event containing the consumer tag.
            State;
        {credit_api_v1, DeliveryCountSnd} ->
            LinkCreditSnd = link_credit_snd(DeliveryCountRcv, LinkCreditRcv, DeliveryCountSnd),
            {ok, QStates, Actions} = rabbit_queue_type:credit_v1(QName, Ctag, LinkCreditSnd, Drain, QStates0),
            State1 = State0#state{queue_states = QStates},
            State = handle_queue_actions(Actions, State1),
            process_credit_reply_sync(Ctag, QName, LinkCreditSnd, State)
    end.

delivery_count_rcv(?UINT(DeliveryCount)) ->
    DeliveryCount;
delivery_count_rcv(undefined) ->
    %% "In the event that the receiver does not yet know the delivery-count,
    %% i.e., delivery-countrcv is unspecified, the sender MUST assume that the
    %% delivery-countrcv is the first delivery-countsnd sent from sender to
    %% receiver, i.e., the delivery-countsnd specified in the flow state carried
    %% by the initial attach frame from the sender to the receiver." [2.6.7]
    ?INITIAL_DELIVERY_COUNT.

link_credit_snd(DeliveryCountRcv, LinkCreditRcv, DeliveryCountSnd) ->
    diff(add(DeliveryCountRcv, LinkCreditRcv), DeliveryCountSnd).

%% The AMQP 0.9.1 credit extension was poorly designed because a consumer granting
%% credits to a queue has to synchronously wait for a credit reply from the queue:
%% https://github.com/rabbitmq/rabbitmq-server/blob/b9566f4d02f7ceddd2f267a92d46affd30fb16c8/deps/rabbitmq_codegen/credit_extension.json#L43
%% This blocks our entire AMQP 1.0 session process. Since the credit reply from the
%% queue did not contain the consumr tag prior to feature flag credit_api_v2, we
%% must behave here the same way as non-native AMQP 1.0: We wait until the queue
%% sends us a credit reply sucht that we can correlate that reply with our consumer tag.
process_credit_reply_sync(
  Ctag, QName, Credit, State = #state{queue_states = QStates}) ->
    case rabbit_queue_type:module(QName, QStates) of
        {ok, rabbit_classic_queue} ->
            receive {'$gen_cast',
                     {queue_event,
                      QName,
                      {send_credit_reply, Avail}}} ->
                        %% Convert to credit_api_v2 action.
                        Action = {credit_reply_v1, Ctag, Credit, Avail, false},
                        handle_queue_actions([Action], State)
            after ?CREDIT_REPLY_TIMEOUT ->
                      credit_reply_timeout(classic, QName)
            end;
        {ok, rabbit_quorum_queue} ->
            process_credit_reply_sync_quorum_queue(Ctag, QName, Credit, State);
        {error, not_found} ->
            State
    end.

process_credit_reply_sync_quorum_queue(Ctag, QName, Credit, State0) ->
    receive {'$gen_cast',
             {queue_event,
              QName,
              {QuorumQueue,
               {applied,
                Applied0}}}} ->

                {Applied, ReceivedCreditReply}
                = lists:mapfoldl(
                    %% Convert v1 send_credit_reply to credit_reply_v1 action.
                    %% Available refers to *after* and Credit refers to *before*
                    %% quorum queue sends messages.
                    %% We therefore keep the same wrong behaviour of RabbitMQ 3.x.
                    fun({RaIdx, {send_credit_reply, Available}}, _) ->
                            Action = {credit_reply_v1, Ctag, Credit, Available, false},
                            {{RaIdx, Action}, true};
                       ({RaIdx, {multi, [{send_credit_reply, Available},
                                         {send_drained, _} = SendDrained]}}, _) ->
                            Action = {credit_reply_v1, Ctag, Credit, Available, false},
                            {{RaIdx, {multi, [Action, SendDrained]}}, true};
                       (E, Acc) ->
                            {E, Acc}
                    end, false, Applied0),

                Evt = {queue_event, QName, {QuorumQueue, {applied, Applied}}},
                %% send_drained action must be processed by
                %% rabbit_fifo_client to advance the delivery count.
                State = handle_queue_event(Evt, State0),
                case ReceivedCreditReply of
                    true ->
                        State;
                    false ->
                        process_credit_reply_sync_quorum_queue(Ctag, QName, Credit, State)
                end
    after ?CREDIT_REPLY_TIMEOUT ->
              credit_reply_timeout(quorum, QName)
    end.

-spec credit_reply_timeout(atom(), rabbit_types:rabbit_amqqueue_name()) ->
    no_return().
credit_reply_timeout(QType, QName) ->
    Fmt = "Timed out waiting for credit reply from ~s ~s. "
    "Hint: Enable feature flag credit_api_v2",
    Args = [QType, rabbit_misc:rs(QName)],
    rabbit_log:error(Fmt, Args),
    protocol_error(?V_1_0_AMQP_ERROR_INTERNAL_ERROR, Fmt, Args).

default(undefined, Default) -> Default;
default(Thing,    _Default) -> Thing.

ensure_source(#'v1_0.source'{dynamic = true}, _, _) ->
    protocol_error(?V_1_0_AMQP_ERROR_NOT_IMPLEMENTED, "Dynamic sources not supported", []);
ensure_source(#'v1_0.source'{address = Address,
                             durable = Durable},
              Vhost,
              User = #user{username = Username}) ->
    case Address of
        {utf8, SourceAddr} ->
            case rabbit_routing_parser:parse_endpoint(SourceAddr, false) of
                {ok, Src} ->
                    QNameBin = ensure_terminus(source, Src, Vhost, User, Durable),
                    case rabbit_routing_parser:parse_routing(Src) of
                        {"", QNameList} ->
                            true = string:equal(QNameList, QNameBin),
                            {ok, QNameBin};
                        {XNameList, RoutingKeyList} ->
                            RoutingKey = unicode:characters_to_binary(RoutingKeyList),
                            XNameBin = unicode:characters_to_binary(XNameList),
                            XName = rabbit_misc:r(Vhost, exchange, XNameBin),
                            QName = rabbit_misc:r(Vhost, queue, QNameBin),
                            Binding = #binding{source = XName,
                                               destination = QName,
                                               key = RoutingKey},
                            check_write_permitted(QName, User),
                            check_read_permitted(XName, User),
                            {ok, X} = rabbit_exchange:lookup(XName),
                            check_read_permitted_on_topic(X, User, RoutingKey),
                            case rabbit_binding:add(Binding, Username) of
                                ok ->
                                    {ok, QNameBin};
                                {error, _} = Err ->
                                    Err
                            end
                    end;
                {error, _} = Err ->
                    Err
            end;
        _ ->
            {error, {address_not_utf8_string, Address}}
    end.

transfer_frames(Transfer, Sections, unlimited) ->
    [[Transfer, Sections]];
transfer_frames(Transfer, Sections, MaxFrameSize) ->
    %% TODO Ugh
    TLen = iolist_size(amqp10_framing:encode_bin(Transfer)),
    encode_frames(Transfer, Sections, MaxFrameSize - TLen, []).

encode_frames(_T, _Msg, MaxContentLen, _Transfers) when MaxContentLen =< 0 ->
    protocol_error(?V_1_0_AMQP_ERROR_FRAME_SIZE_TOO_SMALL,
                   "Frame size is too small by ~tp bytes",
                   [-MaxContentLen]);
encode_frames(T, Msg, MaxContentLen, Transfers) ->
    case iolist_size(Msg) > MaxContentLen of
        true ->
            <<Chunk:MaxContentLen/binary, Rest/binary>> = iolist_to_binary(Msg),
            T1 = T#'v1_0.transfer'{more = true},
            encode_frames(T, Rest, MaxContentLen, [[T1, Chunk] | Transfers]);
        false ->
            lists:reverse([[T, Msg] | Transfers])
    end.

source_filters_to_consumer_args(#'v1_0.source'{filter = {map, KVList}}) ->
    source_filters_to_consumer_args(
      [<<"rabbitmq:stream-offset-spec">>,
       <<"rabbitmq:stream-filter">>,
       <<"rabbitmq:stream-match-unfiltered">>],
      KVList,
      []);
source_filters_to_consumer_args(_Source) ->
    [].

source_filters_to_consumer_args([], _KVList, Acc) ->
    Acc;
source_filters_to_consumer_args([<<"rabbitmq:stream-offset-spec">> = H | T], KVList, Acc) ->
    Key = {symbol, H},
    Arg = case keyfind_unpack_described(Key, KVList) of
              {_, {timestamp, Ts}} ->
                  [{<<"x-stream-offset">>, timestamp, Ts div 1000}]; %% 0.9.1 uses second based timestamps
              {_, {utf8, Spec}} ->
                  [{<<"x-stream-offset">>, longstr, Spec}]; %% next, last, first and "10m" etc
              {_, {_, Offset}} when is_integer(Offset) ->
                  [{<<"x-stream-offset">>, long, Offset}]; %% integer offset
              _ ->
                  []
          end,
    source_filters_to_consumer_args(T, KVList, Arg ++ Acc);
source_filters_to_consumer_args([<<"rabbitmq:stream-filter">> = H | T], KVList, Acc) ->
    Key = {symbol, H},
    Arg = case keyfind_unpack_described(Key, KVList) of
              {_, {list, Filters0}} when is_list(Filters0) ->
                  Filters = lists:foldl(fun({utf8, Filter}, L) ->
                                                [{longstr, Filter} | L];
                                           (_, L) ->
                                                L
                                        end, [], Filters0),
                  [{<<"x-stream-filter">>, array, Filters}];
              {_, {utf8, Filter}} ->
                  [{<<"x-stream-filter">>, longstr, Filter}];
              _ ->
                  []
          end,
    source_filters_to_consumer_args(T, KVList, Arg ++ Acc);
source_filters_to_consumer_args([<<"rabbitmq:stream-match-unfiltered">> = H | T], KVList, Acc) ->
    Key = {symbol, H},
    Arg = case keyfind_unpack_described(Key, KVList) of
              {_, MU} when is_boolean(MU) ->
                  [{<<"x-stream-match-unfiltered">>, bool, MU}];
              _ ->
                  []
          end,
    source_filters_to_consumer_args(T, KVList, Arg ++ Acc);
source_filters_to_consumer_args([_ | T], KVList, Acc) ->
    source_filters_to_consumer_args(T, KVList, Acc).

keyfind_unpack_described(Key, KvList) ->
    %% filterset values _should_ be described values
    %% they aren't always however for historical reasons so we need this bit of
    %% code to return a plain value for the given filter key
    case lists:keyfind(Key, 1, KvList) of
        {Key, {described, Key, Value}} ->
            {Key, Value};
        {Key, _} = Kv ->
            Kv;
        false ->
            false
    end.

validate_attach(#'v1_0.attach'{target = #'v1_0.coordinator'{}}) ->
    protocol_error(?V_1_0_AMQP_ERROR_NOT_IMPLEMENTED,
                   "Transactions not supported", []);
validate_attach(#'v1_0.attach'{unsettled = Unsettled,
                               incomplete_unsettled = IncompleteSettled})
  when Unsettled =/= undefined andalso Unsettled =/= {map, []} orelse
       IncompleteSettled =:= true ->
    protocol_error(?V_1_0_AMQP_ERROR_NOT_IMPLEMENTED,
                   "Link recovery not supported", []);
validate_attach(
  #'v1_0.attach'{snd_settle_mode = SndSettleMode,
                 rcv_settle_mode = ?V_1_0_RECEIVER_SETTLE_MODE_SECOND})
  when SndSettleMode =/= ?V_1_0_SENDER_SETTLE_MODE_SETTLED ->
    protocol_error(?V_1_0_AMQP_ERROR_NOT_IMPLEMENTED,
                   "rcv-settle-mode second not supported", []);
validate_attach(#'v1_0.attach'{}) ->
    ok.

validate_multi_transfer_delivery_id(?UINT(Id), Id) ->
    ok;
validate_multi_transfer_delivery_id(undefined, _FirstDeliveryId) ->
    %% "On continuation transfers the delivery-id MAY be omitted."
    ok;
validate_multi_transfer_delivery_id(OtherId, FirstDeliveryId) ->
    %% "It is an error if the delivery-id on a continuation transfer
    %% differs from the delivery-id on the first transfer of a delivery."
    protocol_error(
      ?V_1_0_CONNECTION_ERROR_FRAMING_ERROR,
      "delivery-id of continuation transfer (~p) differs from delivery-id on first transfer (~p)",
      [OtherId, FirstDeliveryId]).

validate_multi_transfer_settled(Settled, Settled)
  when is_boolean(Settled) ->
    ok;
validate_multi_transfer_settled(undefined, Settled)
  when is_boolean(Settled) ->
    ok;
validate_multi_transfer_settled(Other, First)
  when is_boolean(First) ->
    protocol_error(
      ?V_1_0_CONNECTION_ERROR_FRAMING_ERROR,
      "field 'settled' of continuation transfer (~p) differs from "
      "(interpreted) field 'settled' on first transfer (~p)",
      [Other, First]).

%% "If the message is being sent settled by the sender,
%% the value of this field [rcv-settle-mode] is ignored." [2.7.5]
validate_transfer_rcv_settle_mode(?V_1_0_RECEIVER_SETTLE_MODE_SECOND, _Settled = false) ->
    protocol_error(?V_1_0_AMQP_ERROR_NOT_IMPLEMENTED,
                   "rcv-settle-mode second not supported", []);
validate_transfer_rcv_settle_mode(_, _) ->
    ok.

validate_incoming_message_size(Message) ->
    validate_message_size(Message, persistent_term:get(max_message_size)).

validate_message_size(_, unlimited) ->
    ok;
validate_message_size(Message, MaxMsgSize)
  when is_integer(MaxMsgSize) ->
    MsgSize = iolist_size(Message),
    case MsgSize =< MaxMsgSize of
        true ->
            ok;
        false ->
            %% "Any attempt to deliver a message larger than this results in a message-size-exceeded link-error." [2.7.3]
            %% We apply that sentence to both incoming messages that are too large for us and outgoing messages that are
            %% too large for the client.
            %% This is an interesting protocol difference to MQTT where we instead discard outgoing messages that are too
            %% large to send then behave as if we had completed sending that message [MQTT 5.0, MQTT-3.1.2-25].
            protocol_error(
              ?V_1_0_LINK_ERROR_MESSAGE_SIZE_EXCEEDED,
              "message size (~b bytes) > maximum message size (~b bytes)",
              [MsgSize, MaxMsgSize])
    end.

ensure_terminus(Type, {exchange, {XNameList, _RoutingKey}}, Vhost, User, Durability) ->
    ok = exit_if_absent(exchange, Vhost, XNameList),
    case Type of
        target -> undefined;
        source -> declare_queue(generate_queue_name(), Vhost, User, Durability)
    end;
ensure_terminus(target, {topic, _bindingkey}, _, _, _) ->
    %% exchange amq.topic exists
    undefined;
ensure_terminus(source, {topic, _BindingKey}, Vhost, User, Durability) ->
    %% exchange amq.topic exists
    declare_queue(generate_queue_name(), Vhost, User, Durability);
ensure_terminus(target, {queue, undefined}, _, _, _) ->
    %% Target "/queue" means publish to default exchange with message subject as routing key.
    %% Default exchange exists.
    undefined;
ensure_terminus(_, {queue, QNameList}, Vhost, User, Durability) ->
    declare_queue(unicode:characters_to_binary(QNameList), Vhost, User, Durability);
ensure_terminus(_, {amqqueue, QNameList}, Vhost, _, _) ->
    %% Target "/amq/queue/" is handled specially due to AMQP legacy:
    %% "Queue names starting with "amq." are reserved for pre-declared and
    %% standardised queues. The client MAY declare a queue starting with "amq."
    %% if the passive option is set, or the queue already exists."
    QNameBin = unicode:characters_to_binary(QNameList),
    ok = exit_if_absent(queue, Vhost, QNameBin),
    QNameBin.

exit_if_absent(Kind, Vhost, Name) ->
    ResourceName = rabbit_misc:r(Vhost, Kind, unicode:characters_to_binary(Name)),
    Mod = case Kind of
              exchange -> rabbit_exchange;
              queue -> rabbit_amqqueue
          end,
    case Mod:exists(ResourceName) of
        true ->
            ok;
        false ->
            protocol_error(?V_1_0_AMQP_ERROR_NOT_FOUND, "no ~ts", [rabbit_misc:rs(ResourceName)])
    end.

generate_queue_name() ->
    rabbit_guid:binary(rabbit_guid:gen_secure(), "amq.gen").

declare_queue(QNameBin, Vhost, User = #user{username = Username}, TerminusDurability) ->
    QName = rabbit_misc:r(Vhost, queue, QNameBin),
    check_configure_permitted(QName, User),
    check_vhost_queue_limit(Vhost, QName),
    rabbit_core_metrics:queue_declared(QName),
    Q0 = amqqueue:new(QName,
                      _Pid = none,
                      queue_is_durable(TerminusDurability),
                      _AutoDelete = false,
                      _QOwner = none,
                      _QArgs = [],
                      Vhost,
                      #{user => Username},
                      rabbit_classic_queue),
    case rabbit_queue_type:declare(Q0, node()) of
        {new, _Q}  ->
            rabbit_core_metrics:queue_created(QName),
            QNameBin;
        {existing, _Q} ->
            QNameBin;
        Other ->
            protocol_error(?V_1_0_AMQP_ERROR_INTERNAL_ERROR, "Failed to declare ~s: ~p", [rabbit_misc:rs(QName), Other])
    end.

outcomes(#'v1_0.source'{outcomes = undefined}) ->
    {array, symbol, ?OUTCOMES};
outcomes(#'v1_0.source'{outcomes = {array, symbol, Syms} = Outcomes}) ->
    case lists:filter(fun(O) -> not lists:member(O, ?OUTCOMES) end, Syms) of
        [] ->
            Outcomes;
        Unsupported ->
            rabbit_amqp_util:protocol_error(
              ?V_1_0_AMQP_ERROR_NOT_IMPLEMENTED,
              "Outcomes not supported: ~tp",
              [Unsupported])
    end;
outcomes(#'v1_0.source'{outcomes = Unsupported}) ->
    rabbit_amqp_util:protocol_error(
      ?V_1_0_AMQP_ERROR_NOT_IMPLEMENTED,
      "Outcomes not supported: ~tp",
      [Unsupported]);
outcomes(_) ->
    {array, symbol, ?OUTCOMES}.

-spec handle_to_ctag(link_handle()) -> rabbit_types:ctag().
handle_to_ctag(Handle) ->
    integer_to_binary(Handle).

-spec ctag_to_handle(rabbit_types:ctag()) -> link_handle().
ctag_to_handle(Ctag) ->
    binary_to_integer(Ctag).

queue_is_durable(?V_1_0_TERMINUS_DURABILITY_NONE) ->
    false;
queue_is_durable(?V_1_0_TERMINUS_DURABILITY_CONFIGURATION) ->
    true;
queue_is_durable(?V_1_0_TERMINUS_DURABILITY_UNSETTLED_STATE) ->
    true;
queue_is_durable(undefined) ->
    %% <field name="durable" type="terminus-durability" default="none"/>
    %% [3.5.3]
    queue_is_durable(?V_1_0_TERMINUS_DURABILITY_NONE).

-spec remove_link_from_outgoing_unsettled_map(link_handle() | rabbit_types:ctag(), Map) ->
    {Map, [rabbit_amqqueue:msg_id()]}
      when Map :: #{delivery_number() => #outgoing_unsettled{}}.
remove_link_from_outgoing_unsettled_map(Handle, Map)
  when is_integer(Handle) ->
    remove_link_from_outgoing_unsettled_map(handle_to_ctag(Handle), Map);
remove_link_from_outgoing_unsettled_map(Ctag, Map)
  when is_binary(Ctag) ->
    maps:fold(fun(DeliveryId,
                  #outgoing_unsettled{consumer_tag = Tag,
                                      msg_id = Id},
                  {M, Ids})
                    when Tag =:= Ctag ->
                      {maps:remove(DeliveryId, M), [Id | Ids]};
                 (_, _, Acc) ->
                      Acc
              end, {Map, []}, Map).

messages_received(Settled) ->
    rabbit_global_counters:messages_received(?PROTOCOL, 1),
    case Settled of
        true -> ok;
        false -> rabbit_global_counters:messages_received_confirm(?PROTOCOL, 1)
    end.

messages_delivered(Redelivered, QueueType) ->
    rabbit_global_counters:messages_delivered(?PROTOCOL, QueueType, 1),
    case Redelivered of
        true -> rabbit_global_counters:messages_redelivered(?PROTOCOL, QueueType, 1);
        false -> ok
    end.

messages_acknowledged(complete, QName, QS, MsgIds) ->
    case rabbit_queue_type:module(QName, QS) of
        {ok, QType} ->
            rabbit_global_counters:messages_acknowledged(?PROTOCOL, QType, length(MsgIds));
        _ ->
            ok
    end;
messages_acknowledged(_, _, _, _) ->
    ok.

publisher_or_consumer_deleted(#incoming_link{}) ->
    rabbit_global_counters:publisher_deleted(?PROTOCOL);
publisher_or_consumer_deleted(#outgoing_link{}) ->
    rabbit_global_counters:consumer_deleted(?PROTOCOL).

publisher_or_consumer_deleted(
  #state{incoming_links = NewIncomingLinks,
         outgoing_links = NewOutgoingLinks},
  #state{incoming_links = OldIncomingLinks,
         outgoing_links = OldOutgoingLinks}) ->
    if map_size(NewIncomingLinks) < map_size(OldIncomingLinks) ->
           rabbit_global_counters:publisher_deleted(?PROTOCOL);
       map_size(NewOutgoingLinks) < map_size(OldOutgoingLinks) ->
           rabbit_global_counters:consumer_deleted(?PROTOCOL);
       true ->
           ok
    end.

%% If we previously already sent a detach with an error condition, and the Detach we
%% receive here is therefore the client's reply, do not reply again with a 3rd detach.
maybe_detach_reply(
  Detach,
  #state{incoming_links = NewIncomingLinks,
         outgoing_links = NewOutgoingLinks,
         incoming_management_links = NewIncomingMgmtLinks,
         outgoing_management_links = NewOutgoingMgmtLinks,
         cfg = #cfg{writer_pid = WriterPid,
                    channel_num = Ch}},
  #state{incoming_links = OldIncomingLinks,
         outgoing_links = OldOutgoingLinks,
         incoming_management_links = OldIncomingMgmtLinks,
         outgoing_management_links = OldOutgoingMgmtLinks})
  when map_size(NewIncomingLinks) < map_size(OldIncomingLinks) orelse
       map_size(NewOutgoingLinks) < map_size(OldOutgoingLinks) orelse
       map_size(NewIncomingMgmtLinks) < map_size(OldIncomingMgmtLinks) orelse
       map_size(NewOutgoingMgmtLinks) < map_size(OldOutgoingMgmtLinks) ->
    Reply = Detach#'v1_0.detach'{error = undefined},
    rabbit_amqp_writer:send_command(WriterPid, Ch, Reply);
maybe_detach_reply(_, _, _) ->
    ok.

-spec maybe_detach_mgmt_link(link_handle(), state()) -> state().
maybe_detach_mgmt_link(
  HandleInt,
  State = #state{management_link_pairs = LinkPairs0,
                 incoming_management_links = IncomingLinks0,
                 outgoing_management_links = OutgoingLinks0}) ->
    case maps:take(HandleInt, IncomingLinks0) of
        {#management_link{name = Name}, IncomingLinks} ->
            Pair = #management_link_pair{outgoing_half = OutgoingHalf} = maps:get(Name, LinkPairs0),
            LinkPairs = case OutgoingHalf of
                            unattached ->
                                maps:remove(Name, LinkPairs0);
                            _ ->
                                maps:update(Name, Pair#management_link_pair{incoming_half = unattached}, LinkPairs0)
                        end,
            State#state{incoming_management_links = IncomingLinks,
                        management_link_pairs = LinkPairs};
        error ->
            case maps:take(HandleInt, OutgoingLinks0) of
                {#management_link{name = Name}, OutgoingLinks} ->
                    Pair = #management_link_pair{incoming_half = IncomingHalf} = maps:get(Name, LinkPairs0),
                    LinkPairs = case IncomingHalf of
                                    unattached ->
                                        maps:remove(Name, LinkPairs0);
                                    _ ->
                                        maps:update(Name, Pair#management_link_pair{outgoing_half = unattached}, LinkPairs0)
                                end,
                    State#state{outgoing_management_links = OutgoingLinks,
                                management_link_pairs = LinkPairs};
                error ->
                    State
            end
    end.

check_internal_exchange(#exchange{internal = true,
                                  name = XName}) ->
    protocol_error(?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS,
                   "attach to internal ~ts is forbidden",
                   [rabbit_misc:rs(XName)]);
check_internal_exchange(_) ->
    ok.

check_write_permitted(Resource, User) ->
    check_resource_access(Resource, User, write).

check_read_permitted(Resource, User) ->
    check_resource_access(Resource, User, read).

check_configure_permitted(Resource, User) ->
    check_resource_access(Resource, User, configure).

check_resource_access(Resource, User, Perm) ->
    Context = #{},
    ok = try rabbit_access_control:check_resource_access(User, Resource, Perm, Context)
         catch exit:#amqp_error{name = access_refused,
                                explanation = Msg} ->
                   protocol_error(?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS, Msg, [])
         end.

check_write_permitted_on_topic(Resource, User, RoutingKey) ->
    check_topic_authorisation(Resource, User, RoutingKey, write).

check_read_permitted_on_topic(Resource, User, RoutingKey) ->
    check_topic_authorisation(Resource, User, RoutingKey, read).

check_topic_authorisation(#exchange{type = topic,
                                    name = XName = #resource{virtual_host = VHost}},
                          User = #user{username = Username},
                          RoutingKey,
                          Permission) ->
    Resource = XName#resource{kind = topic},
    CacheElem = {Resource, RoutingKey, Permission},
    Cache = case get(?TOPIC_PERMISSION_CACHE) of
                undefined -> [];
                List -> List
            end,
    case lists:member(CacheElem, Cache) of
        true ->
            ok;
        false ->
            VariableMap = #{<<"vhost">> => VHost,
                            <<"username">> => Username},
            Context = #{routing_key => RoutingKey,
                        variable_map => VariableMap},
            try rabbit_access_control:check_topic_access(User, Resource, Permission, Context) of
                ok ->
                    CacheTail = lists:sublist(Cache, ?MAX_PERMISSION_CACHE_SIZE - 1),
                    put(?TOPIC_PERMISSION_CACHE, [CacheElem | CacheTail]),
                    ok
            catch
                exit:#amqp_error{name = access_refused,
                                 explanation = Msg} ->
                    protocol_error(?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS, Msg, [])
            end
    end;
check_topic_authorisation(_, _, _, _) ->
    ok.

check_vhost_queue_limit(Vhost, QName) ->
    case rabbit_vhost_limit:is_over_queue_limit(Vhost) of
        false ->
            ok;
        {true, Limit} ->
            protocol_error(
              ?V_1_0_AMQP_ERROR_RESOURCE_LIMIT_EXCEEDED,
              "cannot declare ~ts: vhost queue limit (~p) is reached",
              [rabbit_misc:rs(QName), Limit])
    end.

check_user_id(Mc, User) ->
    case rabbit_access_control:check_user_id(Mc, User) of
        ok ->
            ok;
        {refused, Reason, Args} ->
            protocol_error(?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS, Reason, Args)
    end.

maps_update_with(Key, Fun, Init, Map) ->
    case Map of
        #{Key := Value} ->
            Map#{Key := Fun(Value)};
        _ ->
            Map#{Key => Init}
    end.

max_message_size({ulong, Size})
  when Size > 0 ->
    Size;
max_message_size(_) ->
    %% "If this field is zero or unset, there is no
    %% maximum size imposed by the link endpoint."
    unlimited.

check_paired({map, Properties}) ->
    case lists:any(fun({{symbol, <<"paired">>}, true}) ->
                           true;
                      (_) ->
                           false
                   end, Properties) of
        true ->
            ok;
        false ->
            property_paired_not_set()
    end;
check_paired(_) ->
    property_paired_not_set().

-spec property_paired_not_set() -> no_return().
property_paired_not_set() ->
    protocol_error(?V_1_0_AMQP_ERROR_INVALID_FIELD,
                   "Link property 'paired' is not set to boolean value 'true'", []).

format_status(
  #{state := #state{cfg = Cfg,
                    outgoing_pending = OutgoingPending,
                    remote_incoming_window = RemoteIncomingWindow,
                    remote_outgoing_window = RemoteOutgoingWindow,
                    next_incoming_id = NextIncomingId,
                    incoming_window = IncomingWindow,
                    next_outgoing_id = NextOutgoingId,
                    outgoing_delivery_id = OutgoingDeliveryId,
                    incoming_links = IncomingLinks,
                    outgoing_links = OutgoingLinks,
                    outgoing_unsettled_map = OutgoingUnsettledMap,
                    stashed_rejected = StashedRejected,
                    stashed_settled = StashedSettled,
                    stashed_down = StashedDown,
                    stashed_eol = StashedEol,
                    queue_states = QueueStates}} = Status) ->
    State = #{cfg => Cfg,
              outgoing_pending => queue:len(OutgoingPending),
              remote_incoming_window => RemoteIncomingWindow,
              remote_outgoing_window => RemoteOutgoingWindow,
              next_incoming_id => NextIncomingId,
              incoming_window => IncomingWindow,
              next_outgoing_id => NextOutgoingId,
              outgoing_delivery_id => OutgoingDeliveryId,
              incoming_links => IncomingLinks,
              outgoing_links => OutgoingLinks,
              outgoing_unsettled_map => OutgoingUnsettledMap,
              stashed_rejected => StashedRejected,
              stashed_settled => StashedSettled,
              stashed_down => StashedDown,
              stashed_eol => StashedEol,
              queue_states => rabbit_queue_type:format_status(QueueStates)},
    maps:update(state, State, Status).
