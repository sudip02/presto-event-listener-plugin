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

package org.presto.plugin.events.writer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MultiChannelWriter implements EventWriter {
    List<EventWriter> eventWriters;

    public MultiChannelWriter(Map<String, String> config, Event eventName) {
        eventWriters = new ArrayList<>();
        String writer[] = config.get("writer").split(",");
        for (String write: writer)
            eventWriters.add(EventWriterFactory.getWriterByName(write, config, eventName));
    }

    @Override
    public void write(String content) {
        for (EventWriter eventWriter: eventWriters)
            eventWriter.write(content);
    }
}
