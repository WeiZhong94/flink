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

package org.apache.flink.streaming.api.utils.input;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.types.Row;

import static org.apache.flink.streaming.api.utils.PythonOperatorUtils.KeyedProcessFunctionInputFlag.EVENT_TIME_TIMER;
import static org.apache.flink.streaming.api.utils.PythonOperatorUtils.KeyedProcessFunctionInputFlag.NORMAL_DATA;
import static org.apache.flink.streaming.api.utils.PythonOperatorUtils.KeyedProcessFunctionInputFlag.PROC_TIME_TIMER;

/**
 * This factory produces runner input row for two input operators which need to send the timer
 * trigger event to python side.
 */
public class TwoInputWithTimerRowFactory {

    /** Reusable row for normal data runner inputs. */
    private final Row reuseNormalInput;

    private final Row reuseUnionRow;

    /** Reusable row for timer data runner inputs. */
    private final Row reuseTimerData;

    public TwoInputWithTimerRowFactory() {
        this.reuseNormalInput = new Row(5);
        reuseNormalInput.setField(0, NORMAL_DATA.value);
        this.reuseUnionRow = new Row(3);
        this.reuseTimerData = new Row(5);
    }

    public Row fromNormalData(boolean isLeft, long timestamp, long watermark, Row userInput) {
        reuseUnionRow.setField(0, isLeft);
        if (isLeft) {
            // The input row is a tuple of key and value.
            reuseUnionRow.setField(1, userInput);
            // need to set null since it is a reuse row.
            reuseUnionRow.setField(2, null);
        } else {
            // need to set null since it is a reuse row.
            reuseUnionRow.setField(1, null);
            // The input row is a tuple of key and value.
            reuseUnionRow.setField(2, userInput);
        }

        reuseNormalInput.setField(1, timestamp);
        reuseNormalInput.setField(2, watermark);
        reuseNormalInput.setField(4, reuseUnionRow);
        return reuseNormalInput;
    }

    public Row fromTimer(TimeDomain timeDomain, long timestamp, long watermark, Row key) {
        if (timeDomain == TimeDomain.PROCESSING_TIME) {
            reuseTimerData.setField(0, PROC_TIME_TIMER.value);
        } else {
            reuseTimerData.setField(0, EVENT_TIME_TIMER.value);
        }
        reuseTimerData.setField(1, timestamp);
        reuseTimerData.setField(2, watermark);
        reuseTimerData.setField(3, key);
        return reuseTimerData;
    }

    public static TypeInformation<Row> getRunnerInputTypeInfo(
            TypeInformation<Row> userInputType1,
            TypeInformation<Row> userInputType2,
            TypeInformation<Row> keyType) {
        // structure: [isLeftUserInput, leftInput, rightInput]
        RowTypeInfo unifiedInputTypeInfo =
                new RowTypeInfo(Types.BOOLEAN, userInputType1, userInputType2);

        // structure: [inputFlag, timestamp, currentWatermark, key, userInput]
        // for more details about the input flag, see `KeyedProcessFunctionInputFlag`
        return Types.ROW(Types.BYTE, Types.LONG, Types.LONG, keyType, unifiedInputTypeInfo);
    }
}
