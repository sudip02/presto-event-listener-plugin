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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;
import java.util.Properties;

public class KafkaWriter implements EventWriter {
    private Producer<String, String> producer;
    String topicName;

    public KafkaWriter(Map<String, String> config, Event eventName) {
        String prefix = String.format("kafka.%s.", eventName.name().toLowerCase());

        Properties properties = new Properties();
        for (Map.Entry<String,String> item: config.entrySet()) {
            if (!item.getKey().startsWith(prefix))
                continue;

            String key = item.getKey().substring(prefix.length());

            if (key.equalsIgnoreCase("topic"))
                topicName = item.getValue();
            else
                properties.put(key, item.getValue());
        }
        producer = new KafkaProducer<String, String>(properties);
    }

    @Override
    public void write(String content) {
        producer.send(new ProducerRecord<String,String>(topicName, content));
    }
}
