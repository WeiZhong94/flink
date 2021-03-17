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

package org.apache.flink.streaming.api.utils.output;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.utils.PythonOperatorUtils.KeyedProcessFunctionOutputFlag;
import org.apache.flink.types.Row;

import static org.apache.flink.streaming.api.utils.PythonOperatorUtils.KeyedProcessFunctionOutputFlag.NORMAL_DATA;

/** This handler can accepts the runner output which contains timer registration event. */
public class OutputWithTimerRowHandler {

    private final KeyedStateBackend<Row> keyedStateBackend;
    private final TimerService timerService;
    private final TimestampedCollector collector;

    public OutputWithTimerRowHandler(
            KeyedStateBackend<Row> keyedStateBackend,
            TimerService timerService,
            TimestampedCollector collector) {
        this.keyedStateBackend = keyedStateBackend;
        this.timerService = timerService;
        this.collector = collector;
    }

    public void accept(Row runnerOutput, long timestamp) {
        if (runnerOutput.getField(0) == null
                || (byte) runnerOutput.getField(0) == NORMAL_DATA.value) {
            onData(timestamp, runnerOutput.getField(3));
        } else {
            KeyedProcessFunctionOutputFlag operandType =
                    KeyedProcessFunctionOutputFlag.values()[(byte) runnerOutput.getField(0)];
            onTimerOperation(
                    operandType, (long) runnerOutput.getField(1), (Row) runnerOutput.getField(2));
        }
    }

    private void onTimerOperation(KeyedProcessFunctionOutputFlag operandType, long time, Row key) {
        synchronized (keyedStateBackend) {
            keyedStateBackend.setCurrentKey(key);
            switch (operandType) {
                case REGISTER_EVENT_TIMER:
                    timerService.registerEventTimeTimer(time);
                    break;
                case REGISTER_PROC_TIMER:
                    timerService.registerProcessingTimeTimer(time);
                    break;
                case DEL_EVENT_TIMER:
                    timerService.deleteEventTimeTimer(time);
                    break;
                case DEL_PROC_TIMER:
                    timerService.deleteProcessingTimeTimer(time);
            }
        }
    }

    private void onData(long timestamp, Object data) {
        if (timestamp != Long.MIN_VALUE) {
            collector.setAbsoluteTimestamp(timestamp);
        } else {
            collector.eraseTimestamp();
        }
        collector.collect(data);
    }

    public static TypeInformation<Row> getRunnerOutputTypeInfo(
            TypeInformation<?> outputType, TypeInformation<Row> keyType) {
        // structure: [outputFlag, timestamp, key, userOutput]
        // for more details about the output flag, see `KeyedProcessFunctionOutputFlag`
        return Types.ROW(Types.BYTE, Types.LONG, keyType, outputType);
    }
}
