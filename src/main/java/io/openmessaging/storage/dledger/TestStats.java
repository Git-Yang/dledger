/*
 * Copyright 2017-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openmessaging.storage.dledger;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestStats {
    private static Logger logger = LoggerFactory.getLogger(TestStats.class);
    private static ConcurrentHashMap<String, ConcurrentLinkedQueue<Long>> stats = new ConcurrentHashMap<>(32);
    private static Timer timer = new Timer();

    static {
        timer.scheduleAtFixedRate(new PrintTest(), 1000L, 1000L);
    }

    public static class PrintTest extends TimerTask {
        @Override
        public void run() {
            ConcurrentHashMap<String, ConcurrentLinkedQueue<Long>> printStats = stats;
            stats = new ConcurrentHashMap<>();
            StringBuilder sb = new StringBuilder("TEST:\n");
            for (Map.Entry<String, ConcurrentLinkedQueue<Long>> entry : printStats.entrySet()) {
                double avgTime = 0;
                for (Long time : entry.getValue()) {
                    avgTime += time;
                }
                avgTime /= entry.getValue().size();
                sb.append(entry.getKey()).append(":").append(avgTime / 1000 / 1000).append("\n");
            }
            logger.warn(sb.toString());
        }
    }

    public static void put(IndexEnum indexEnum, long startTime, long endTime) {
        if (stats.containsKey(indexEnum.getName())) {
            stats.get(indexEnum.getName()).add(endTime - startTime);
        }
        synchronized (indexEnum) {
            if (stats.containsKey(indexEnum.getName())) {
                stats.get(indexEnum.getName()).add(endTime - startTime);
            }
            stats.put(indexEnum.getName(), new ConcurrentLinkedQueue<>());
            stats.get(indexEnum.getName()).add(endTime - startTime);
        }
    }

    public enum IndexEnum {
        PROCESS_REQUEST("Broker #ProcessRequest"),
        PUT_MESSAGE("Broker #PutMessage"),
        ASYN_PUT_MESSAGE_1("Broker #AsyncPutMessage_1"),
        ASYN_PUT_MESSAGE_2("Broker #AsyncPutMessage_2"),
        ASYN_PUT_MESSAGE_2_1("Broker #AsyncPutMessage_2_1"),
        APPEND_ENTRY_RESPONSE("Broker #AppendEntryResponse"),
        APPEND_AS_LEADER("Leader #AppendAsLeader"),
        DO_APEND_INNER("Leader #DoAppendInner"),
        DO_BATCH_APEND_INNER("Leader #DoBatchAppendInner"),
        QUORUM_ACK_CHECKER("Leader #QuorumAckChecker"),
        HANDLE_PUSH("Leader #HandlePush"),
        HANDLE_DO_APPEND("Leader #HandleDoAppend"),

        ;

        private String name;

        IndexEnum(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }
}
