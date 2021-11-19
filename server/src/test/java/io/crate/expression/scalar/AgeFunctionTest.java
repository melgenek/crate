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
package io.crate.expression.scalar;


import io.crate.metadata.SystemClock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneOffset;

public class AgeFunctionTest extends ScalarTestCase {

    private static final long FIRST_JAN_2021_MIDNIGHT_UTC_AS_MILLIS =
        LocalDate.of(2021, Month.JANUARY, 1).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();

    @Before
    public void prepare() {
        SystemClock.setCurrentMillisFixedUTC(FIRST_JAN_2021_MIDNIGHT_UTC_AS_MILLIS);
    }

    @After
    public void cleanUp() {
        SystemClock.setCurrentMillisSystemUTC();
    }

    @Test
    public void test_calls_within_statement_are_idempotent() {
        assertEvaluate("age('2019-01-01' :: TIMESTAMP) = age('2019-01-01' :: TIMESTAMP)", true);
    }

    @Test
    public void test_1_argument_before_curdate_age_is_positive_() {
        // 1 second left to the current_date (manually set 2021.01.01 midnight)
        assertEvaluate("pg_catalog.age('2020-12-31 23:59:59'::timestamp without time zone) ", 1000L);
    }

    @Test
    public void test_1_argument_before_curdate_age_is_negative() {
        // 1 second after the current_date (manually set 2021.01.01 midnight)
        assertEvaluate("pg_catalog.age('2021-01-01 00:00:01'::timestamp without time zone) ", -1000L);
    }

    @Test
    public void test_floating_point_arguments() {
        // We treat float and double values as seconds with milliseconds as fractions.
        // BinaryScalar calls sanitizeValue() which does NOT multiply into 1000 but test case
        // passes since implicitCast called before sanitizeValue could 'spoil' floating point representation.
        assertEvaluate("pg_catalog.age(1.2, 1.1) ", 1200L - 1100L);
    }

    @Test
    public void test_2_arguments_positive() {
        assertEvaluate("pg_catalog.age('2019-01-02'::TIMESTAMP, '2019-01-01'::TIMESTAMP) ", 86400000L);
    }

    @Test
    public void test_2_arguments_negative() {
        assertEvaluate("pg_catalog.age('2019-01-01'::TIMESTAMP, '2019-01-02'::TIMESTAMP) ", -86400000L);
    }

    @Test
    public void test_at_least_one_arg_is_null_returns_null() {
        assertEvaluate("age(null)", null);
        assertEvaluate("age(null, '2019-01-02'::TIMESTAMP)", null);
        assertEvaluate("age('2019-01-02'::TIMESTAMP, null)", null);
        assertEvaluate("age(null, null)", null);
    }

    @Test
    public void test_2_arguments_with_timezone() throws Exception {
        assertEvaluate("pg_catalog.age(CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)", 0L);
    }
}
