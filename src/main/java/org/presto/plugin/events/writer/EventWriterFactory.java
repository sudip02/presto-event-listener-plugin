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

import java.util.Map;

public class EventWriterFactory {

    public static EventWriter getEventWriter(Map<String, String> config, EventWriter.Event eventName) {
        if (!config.containsKey("writer"))
            return new LocalFileWriter(config, eventName);
        String writer = config.get("writer");
        if (writer.split(",").length > 1)
            return new MultiChannelWriter(config, eventName);

        return getWriterByName(writer, config, eventName);
    }

    public static EventWriter getWriterByName(String name, Map<String, String> config, EventWriter.Event eventName) {
        switch (name.toLowerCase()) {
            case "kafka":
                return new KafkaWriter(config, eventName);
            case "local":
                return new LocalFileWriter(config, eventName);
            default:
                return new LocalFileWriter(config, eventName);
        }
    }


}
