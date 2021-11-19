/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.expression.scalar;

import io.crate.data.Input;
import io.crate.expression.scalar.arithmetic.BinaryScalar;
import io.crate.metadata.FunctionName;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.types.DataTypes;

import static io.crate.metadata.functions.Signature.scalar;

public class AgeFunction extends Scalar<Long, Object> {

    private static final FunctionName NAME = new FunctionName(PgCatalogSchemaInfo.NAME, "age");

    public static void register(ScalarFunctionModule module) {
        module.register(
            scalar(
                NAME,
                DataTypes.TIMESTAMP.getTypeSignature(),
                DataTypes.LONG.getTypeSignature()
            ).withFeatures(NO_FEATURES),
            AgeFunction::new
        );

        module.register(
            scalar(
                NAME,
                DataTypes.TIMESTAMP.getTypeSignature(),
                DataTypes.TIMESTAMP.getTypeSignature(),
                DataTypes.LONG.getTypeSignature()
            ).withFeatures(NO_FEATURES),
            (signature, boundSignature) ->
                new BinaryScalar<>(
                    (t1, t2) -> t1 - t2,
                    signature,
                    boundSignature,
                    DataTypes.TIMESTAMP
                )
        );
    }

    private final Signature signature;
    private final Signature boundSignature;

    public AgeFunction(Signature signature, Signature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Signature boundSignature() {
        return boundSignature;
    }

    @Override
    public Long evaluate(TransactionContext txnCtx, NodeContext nodeContext, Input<Object>... args) {
        Long x = DataTypes.TIMESTAMP.sanitizeValue(args[0].value());
        if (x == null) {
            return null;
        }
        long millis = txnCtx.currentInstant().toEpochMilli();
        return millis - millis % 86400000 - x;
    }
}
