/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package me.jinsui.shennong.hpconnector;

import com.twitter.heron.api.spout.ISpoutOutputCollector;

import java.util.ArrayList;
import java.util.List;
import org.apache.pulsar.client.api.Message;

public class MockSpoutOutputCollector implements ISpoutOutputCollector {

    private boolean emitted = false;
    private Message lastMessage = null;
    private String data = null;

    @Override
    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
        emitted = true;
        data = (String) tuple.get(0);
        lastMessage = (Message) messageId;
        return new ArrayList<Integer>();
    }

    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
        emitted = true;
        data = (String) tuple.get(0);
        lastMessage = (Message) messageId;
    }

    @Override
    public void reportError(Throwable error) {
    }

    public boolean emitted() {
        return emitted;
    }

    public String getTupleData() {
        return data;
    }

    public Message getLastMessage() {
        return lastMessage;
    }

    public void reset() {
        emitted = false;
        data = null;
        lastMessage = null;
    }
}
