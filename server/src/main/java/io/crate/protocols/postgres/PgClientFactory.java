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

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;

import io.crate.netty.NettyBootstrap;


/**
 * Used to create {@link PgClient} instances.
 *
 * The main intention of this class is to hide dependencies of a PgClient from components
 * which may need to create a PgClient.
 **/
@Singleton
public class PgClientFactory {

    private final Settings settings;
    private final NettyBootstrap nettyBootstrap;

    @Inject
    public PgClientFactory(Settings settings,
                           NettyBootstrap nettyBootstrap) {
        this.settings = settings;
        this.nettyBootstrap = nettyBootstrap;
    }

    public PgClient createClient(String host) {
        return new PgClient(settings, nettyBootstrap, host);
    }
}
