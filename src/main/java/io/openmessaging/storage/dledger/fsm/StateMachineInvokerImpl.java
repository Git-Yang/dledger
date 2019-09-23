/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openmessaging.storage.dledger.fsm;

import io.openmessaging.storage.dledger.ApplyTask;
import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.DLedgerLeaderElector;
import io.openmessaging.storage.dledger.MemberState;
import io.openmessaging.storage.dledger.ShutdownAbleThread;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.exception.DLedgerException;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.protocol.RequestOrResponse;
import io.openmessaging.storage.dledger.store.DLedgerStore;
import io.openmessaging.storage.dledger.store.snapshot.SnapshotReader;
import io.openmessaging.storage.dledger.store.snapshot.SnapshotWriter;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StateMachineInvokerImpl extends ShutdownAbleThread implements StateMachineInvoker {
    private static Logger logger = LoggerFactory.getLogger(StateMachineInvokerImpl.class);

    private volatile long lastAppliedIndex = 0;

    private volatile long lastAppliedTerm = 0;

    private StateMachine stateMachine;

    private DLedgerStore dLedgerStore;

    private DLedgerConfig dLedgerConfig;

    private SnapshotReader snapshotReader;

    private SnapshotWriter snapshotWriter;

    DLedgerLeaderElector.RoleChangeHandler stateMachineRoleChangeHandler = new StateMachineRoleChangeHandlerImpl();

    private Map<Long, ConcurrentHashMap<Long, ApplyTask>> pendingTaskMap = new ConcurrentHashMap<>();

    public StateMachineInvokerImpl(DLedgerStore dLedgerStore, DLedgerConfig dLedgerConfig) {
        super("StateMachineInvokerImpl", logger);
        this.dLedgerStore = dLedgerStore;
        this.dLedgerConfig = dLedgerConfig;
    }

    private void checkTermForPendingMap(long term, String env) {
        if (!pendingTaskMap.containsKey(term)) {
            logger.info("Initialize the pending task map in {} for term={}", env, term);
            pendingTaskMap.putIfAbsent(term, new ConcurrentHashMap<>());
        }
    }

    @Override public long getLastAppliedIndex() {
        return this.lastAppliedIndex;
    }

    @Override public boolean onRoleChanged(MemberState.Role role) {
        return stateMachine.onRoleChange(role);
    }

    @Override public void onCommit() {
        wakeup();
    }

    @Override public boolean onSnapshotSave() {
        CompletableFuture<RequestOrResponse> responseCompletableFuture = stateMachine.onSnapshotSave(snapshotWriter);
        try {
            RequestOrResponse response = responseCompletableFuture.get(3000, TimeUnit.MILLISECONDS);
            if (response.getCode() == DLedgerResponseCode.SUCCESS.getCode()) {
                return true;
            }
        } catch (Exception ex) {
            logger.warn("Snapshot save error", ex);
        }
        return false;
    }

    @Override public boolean onSnapshotLoad() {
        return onSnapshotLoad();
    }

    @Override public void onException(DLedgerException dLedgerException) {
        stateMachine.onException(dLedgerException);
    }

    @Override public void onStop() {
        this.stateMachine.onStop();
    }

    @Override public void onStart() {
        this.stateMachine.onStart();
    }

    @Override public DLedgerLeaderElector.RoleChangeHandler getRoleChangeHandler() {
        return this.stateMachineRoleChangeHandler;
    }

    private void applyEntry() {
        for (long index = lastAppliedIndex; index < dLedgerStore.getCommittedIndex(); index++) {
            DLedgerEntry dLedgerEntry = this.dLedgerStore.get(index);
            ConcurrentHashMap<Long, ApplyTask> taskMap = this.pendingTaskMap.get(dLedgerEntry.getTerm());
            if (taskMap == null) {
                continue;
            }
            long start = System.currentTimeMillis();
            ApplyTask applyTask = taskMap.get(dLedgerEntry.getIndex());
            this.stateMachine.onApply(dLedgerEntry, applyTask);
            if (DLedgerUtils.elapsed(start) > 200) {
                logger.warn("Apply task used more than 200 ms");
            }
            this.lastAppliedIndex = dLedgerEntry.getIndex();
            this.lastAppliedTerm = dLedgerEntry.getTerm();
            taskMap.remove(dLedgerEntry.getIndex());
        }
    }

    @Override public void apply(ApplyTask applyTask) {
        checkTermForPendingMap(applyTask.getTerm(), "waitApply");
        ConcurrentHashMap<Long, ApplyTask> taskMap = this.pendingTaskMap.get(applyTask.getTerm());
        ApplyTask oldTask = taskMap.putIfAbsent(applyTask.getTerm(), applyTask);
        if (oldTask != null) {
            logger.warn("[MONITOR] get old apply task at index={}", applyTask.getIndex());
        }
    }

    @Override public void setStateMachine(StateMachine stateMachine) {
        this.stateMachine = stateMachine;
    }

    @Override public boolean isPendingFull(long term) {
        checkTermForPendingMap(term, "waitApply");
        return this.pendingTaskMap.get(term).size() > this.dLedgerConfig.getMaxPendingTasksNum();
    }

    @Override public void checkAbnormalTask(AppendEntryResponse appendEntryResponse) {
        checkTermForPendingMap(appendEntryResponse.getTerm(), "waitApply");
        ConcurrentHashMap<Long, ApplyTask> taskMap = this.pendingTaskMap.get(appendEntryResponse.getTerm());
        if (appendEntryResponse.getCode() != DLedgerResponseCode.SUCCESS.getCode()) {
            ApplyTask applyTask = taskMap.remove(appendEntryResponse);
            if (applyTask != null && !applyTask.getResponseFuture().isDone()) {
                applyTask.getResponseFuture().complete(appendEntryResponse);
            }
        }
        return;
    }

    public void setLastAppliedIndex(long lastAppliedIndex) {
        this.lastAppliedIndex = lastAppliedIndex;
    }

    public long getLastAppliedTerm() {
        return lastAppliedTerm;
    }

    public void setLastAppliedTerm(long lastAppliedTerm) {
        this.lastAppliedTerm = lastAppliedTerm;
    }

    public StateMachine getStateMachine() {
        return stateMachine;
    }

    @Override public void doWork() {
        try {
            if (lastAppliedIndex >= dLedgerStore.getCommittedIndex()) {
                waitForRunning(1);
            }
            applyEntry();
        } catch (Throwable throwable) {
            logger.error("Error in {}", getName(), throwable);
            DLedgerUtils.sleep(100);
        }
    }

    public class StateMachineRoleChangeHandlerImpl implements DLedgerLeaderElector.RoleChangeHandler {
        @Override public void handle(long term, MemberState.Role role) {
            if (stateMachine != null) {
                stateMachine.onRoleChange(role);
            }
        }

        @Override public void startup() {

        }

        @Override public void shutdown() {

        }
    }
}
