/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */


package io.crate.protocols.postgres;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.RemoteConnectionParser;
import org.elasticsearch.transport.Transport.Connection;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.transport.netty4.Netty4MessageChannelHandler;
import org.elasticsearch.transport.netty4.Netty4Transport;

import io.crate.common.collections.BorrowedItem;
import io.crate.netty.NettyBootstrap;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

public class PgClient implements Closeable {

    private final Settings settings;
    private final NettyBootstrap nettyBootstrap;
    private final PageCacheRecycler pageCacheRecycler;
    private final Netty4Transport transport;
    private final DiscoveryNode host;

    private BorrowedItem<EventLoopGroup> eventLoopGroup;
    private Bootstrap bootstrap;
    private Channel channel;


    public PgClient(Settings nodeSettings,
                    NettyBootstrap nettyBootstrap,
                    Netty4Transport transport,
                    PageCacheRecycler pageCacheRecycler,
                    DiscoveryNode host) {
        this.settings = nodeSettings;
        this.nettyBootstrap = nettyBootstrap;
        this.pageCacheRecycler = pageCacheRecycler;
        this.transport = transport;
        this.host = host;
    }

    public CompletableFuture<Connection> connect() {
        bootstrap = new Bootstrap();
        eventLoopGroup = nettyBootstrap.getSharedEventLoopGroup(settings);
        bootstrap.group(eventLoopGroup.item());
        bootstrap.channel(NettyBootstrap.clientChannel());
        bootstrap.option(ChannelOption.TCP_NODELAY, TransportSettings.TCP_NO_DELAY.get(settings));
        bootstrap.option(ChannelOption.SO_KEEPALIVE, TransportSettings.TCP_KEEP_ALIVE.get(settings));

        CompletableFuture<Connection> result = new CompletableFuture<>();
        bootstrap.handler(new ClientChannelInitializer(
            settings,
            host,
            transport,
            pageCacheRecycler,
            result
        ));
        bootstrap.remoteAddress(host.getAddress().address());
        ChannelFuture connect = bootstrap.connect();
        channel = connect.channel();

        ByteBuf buffer = channel.alloc().buffer();
        /// TODO: user must come from connectionInfo
        ClientMessages.sendStartupMessage(buffer, "doc", Map.of("user", "crate"));
        channel.writeAndFlush(buffer);
        return result;
    }

    @Override
    public void close() throws IOException {
        if (eventLoopGroup != null) {
            eventLoopGroup.close();
            eventLoopGroup = null;
        }
        if (bootstrap != null) {
            bootstrap = null;
        }
        if (channel != null) {
            channel.close().awaitUninterruptibly();
            channel = null;
        }
    }

    static class ClientChannelInitializer extends ChannelInitializer<Channel> {

        private final Settings settings;
        private final DiscoveryNode node;
        private final CompletableFuture<Connection> result;
        private final Netty4Transport transport;
        private final PageCacheRecycler pageCacheRecycler;

        public ClientChannelInitializer(Settings settings,
                                        DiscoveryNode node,
                                        Netty4Transport transport,
                                        PageCacheRecycler pageCacheRecycler,
                                        CompletableFuture<Connection> result) {
            this.settings = settings;
            this.node = node;
            this.transport = transport;
            this.pageCacheRecycler = pageCacheRecycler;
            this.result = result;
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast("decoder", new Decoder());

            Handler handler = new Handler(settings, node, transport, pageCacheRecycler, result);
            ch.pipeline().addLast("dispatcher", handler);
        }
    }

    static class Handler extends SimpleChannelInboundHandler<ByteBuf> {

        private final CompletableFuture<Connection> result;
        private final Netty4Transport transport;
        private final PageCacheRecycler pageCacheRecycler;
        private final Settings settings;
        private final DiscoveryNode node;

        public Handler(Settings settings,
                       DiscoveryNode node,
                       Netty4Transport transport,
                       PageCacheRecycler pageCacheRecycler,
                       CompletableFuture<Connection> result) {
            this.settings = settings;
            this.node = node;
            this.transport = transport;
            this.pageCacheRecycler = pageCacheRecycler;
            this.result = result;
        }

        @Override
        public boolean acceptInboundMessage(Object msg) throws Exception {
            return true;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
            byte msgType = msg.readByte();
            int msgLength = msg.readInt() - 4; // exclude self

            switch (msgType) {
                case 'R' -> handleAuth(msg);
                case 'S' -> handleParameterStatus(msg);
                case 'Z' -> handleReadyForQuery(ctx.channel(), msg);
                default -> throw new IllegalStateException("Unexpected message type: " + msgType);
            }
        }

        private void handleReadyForQuery(Channel channel, ByteBuf msg) {
            byte transactionStatus = msg.readByte();
            System.out.println("transactionStatus=" + (char) transactionStatus);

            channel.pipeline().remove("decoder");
            channel.pipeline().remove("dispatcher");

            // TODO: Probably wrong to use existing Netty4Transport;
            // Need to create a transport/pass along components based on *this*
            var handler = new Netty4MessageChannelHandler(pageCacheRecycler, transport);
            channel.pipeline().addLast("dispatcher", handler);

            result.completeExceptionally(new UnsupportedOperationException("NYI"));
        }

        private void handleParameterStatus(ByteBuf msg) {
            String name = PostgresWireProtocol.readCString(msg);
            String value = PostgresWireProtocol.readCString(msg);
            System.out.println(name + "=" + value);
        }

        private void handleAuth(ByteBuf msg) {
            AuthType authType = AuthType.of(msg.readInt());
            System.out.println("AuthType=" + authType);
            switch (authType) {
                case Ok:
                    break;
                case CleartextPassword:
                    break;
                case KerberosV5:
                    break;
                case MD5Password:
                    break;
                default:
                    break;
            }
        }
    }

    enum AuthType {
        Ok,
        KerberosV5,
        CleartextPassword,
        MD5Password;

        public static AuthType of(int type) {
            return switch (type) {
                case 0 -> Ok;
                case 2 -> KerberosV5;
                case 3 -> CleartextPassword;
                case 5 -> MD5Password;
                default -> throw new IllegalArgumentException("Unknown auth type: " + type);
            };
        }
    }


    static class Decoder extends LengthFieldBasedFrameDecoder {

        // PostgreSQL wire protocol message format:
        // | Message Type (Byte1) | Length including self (Int32) | Body (depending on type) |
        private static final int LENGTH_FIELD_OFFSET = 1;
        private static final int LENGTH_FIELD_LENGTH = 4;
        private static final int LENGTH_ADJUSTMENT = -4;

        // keep the header
        private static final int INITIAL_BYTES_TO_STRIP = 0;

        public Decoder() {
            super(
                Integer.MAX_VALUE,
                LENGTH_FIELD_OFFSET,
                LENGTH_FIELD_LENGTH,
                LENGTH_ADJUSTMENT,
                INITIAL_BYTES_TO_STRIP
            );
        }
    }
}
