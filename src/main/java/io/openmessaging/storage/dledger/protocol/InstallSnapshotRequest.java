package io.openmessaging.storage.dledger.protocol;/*
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

public class InstallSnapshotRequest extends RequestOrResponse {
    private long lastIncludeIndex;

    private long lastIncludeTerm;

    private long offset;

    private byte[] data;

    private boolean done;

    public long getLastIncludeIndex() {
        return lastIncludeIndex;
    }

    public void setLastIncludeIndex(long lastIncludeIndex) {
        this.lastIncludeIndex = lastIncludeIndex;
    }

    public long getLastIncludeTerm() {
        return lastIncludeTerm;
    }

    public void setLastIncludeTerm(long lastIncludeTerm) {
        this.lastIncludeTerm = lastIncludeTerm;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public boolean isDone() {
        return done;
    }

    public void setDone(boolean done) {
        this.done = done;
    }
}
