/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.api.planner.converters;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.Aggregation;
import org.apache.flink.table.expressions.DistinctAgg;

/**
 * DistinctAggConverter
 */
public class DistinctAggConverter {

    public static RelBuilder.AggCall toAggCall (
            DistinctAgg agg, String name, RelBuilder relBuilder) {
        Aggregation child = (Aggregation) agg.child();
        try {
            return (RelBuilder.AggCall)AggregationConverterUtils.class.getDeclaredMethod
                    ("toAggCall", child.getClass(),
                     String.class, boolean.class,
                     RelBuilder.class).invoke(null, child, name, true, relBuilder);
        } catch (NoSuchMethodException e) {
            throw new TableException("xxx");
        }catch (Exception e){
            throw new TableException("xxx");
        }
    }

    public static SqlAggFunction getSqlAggFunction(DistinctAgg agg, RelBuilder relBuilder) {
        Aggregation child = (Aggregation) agg.child();
        try {
            return (SqlAggFunction)AggregationConverterUtils.class.getDeclaredMethod
                    ("getSqlAggFunction", child.getClass(),
                     RelBuilder.class).invoke(null, child, relBuilder);
        } catch (NoSuchMethodException e) {
            throw new TableException("xxx");
        }catch (Exception e){
            throw new TableException("xxx");
        }
    }
}
