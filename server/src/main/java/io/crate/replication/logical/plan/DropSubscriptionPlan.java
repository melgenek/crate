/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.replication.logical.plan;

import io.crate.action.FutureActionListener;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.engine.collect.sources.InformationSchemaIterables;
import io.crate.execution.support.ChainableAction;
import io.crate.execution.support.ChainableActions;
import io.crate.execution.support.OneRowActionListener;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.replication.logical.action.DropSubscriptionRequest;
import io.crate.replication.logical.analyze.AnalyzedDropSubscription;
import org.elasticsearch.action.support.master.AcknowledgedResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.crate.replication.logical.LogicalReplicationSettings.REPLICATION_SUBSCRIPTION_NAME;

public class DropSubscriptionPlan implements Plan {

    private final AnalyzedDropSubscription analyzedDropSubscription;

    public DropSubscriptionPlan(AnalyzedDropSubscription analyzedDropSubscription) {
        this.analyzedDropSubscription = analyzedDropSubscription;
    }

    @Override
    public StatementType type() {
        return StatementType.DDL;
    }

    @Override
    public void executeOrFail(DependencyCarrier dependencies,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row params,
                              SubQueryResults subQueryResults) throws Exception {

        // This is POC - takes only first table and utilizes existing requests for close/open.
        // Test passes because it has only one table so findFirst doesn't hurt.
        // If it works, close/open requests must be adapted to get list of DocTableInfo


        DocTableInfo tableInfo = (DocTableInfo) InformationSchemaIterables.tablesStream(dependencies.schemas())
            .filter(t -> {
                if (t instanceof DocTableInfo dt) {
                    return analyzedDropSubscription.name().equals(REPLICATION_SUBSCRIPTION_NAME.get(dt.parameters()));
                }
                return false;
            })
            .findFirst().get();

        final List<ChainableAction<Long>> actions = new ArrayList<>();

        // Step 1 - Close subscribed tables and consequently stop tracking and remove retention lease.
        actions.add(new ChainableAction<>(
            () -> dependencies.alterTableOperation()
                .executeAlterTableOpenClose(tableInfo, false, null),
            () -> CompletableFuture.completedFuture(-1L)
        ));

        // Step 2
        // Drop setting and subscription
        actions.add(new ChainableAction<>(
            () -> {
                FutureActionListener<AcknowledgedResponse, Long> listener = new FutureActionListener<>(r -> -1L);
                var request = new DropSubscriptionRequest(analyzedDropSubscription.name(), analyzedDropSubscription.ifExists());
                dependencies.dropSubscriptionAction().execute(request, listener);
                return listener;
            },
            () -> CompletableFuture.completedFuture(-1L)
        ));


        // Step 3
        // Reopen table to update table engine to normal (Operation will be reset back to ALL after step 2)
        actions.add(new ChainableAction<>(
            () -> dependencies.alterTableOperation()
                .executeAlterTableOpenClose(tableInfo, true, null),
            () -> CompletableFuture.completedFuture(-1L)
        ));

        ChainableActions.run(actions).whenComplete(new OneRowActionListener<>(consumer, rCount -> new Row1(rCount == null ? -1 : rCount)));

    }
}
