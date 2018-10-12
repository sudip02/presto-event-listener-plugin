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

package org.presto.plugin.events.plugin;

import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;

import java.util.Map;

/**
 * Created by sudip.hc on 01/02/18.
 */


/**
 * Config's for the plugin put in <presto-installation>/etc/event-listener.properties
 *
 * event-listener.name=<name-of-plugin>

 * date_prefix_format=yyyy-MM-dd-HH
 * time_zone=GMT
 * current_base_path= [ base path where log will be stored before putting to final folder eq, /data/presto/var/log ]
 * completed_base_path= [ base path where completed log files are kept, from where a watcher looks for files and puts to S3 eq, /data/presto/var/queries ]
 * created_query_file_suffix= [ Filename suffix for created query events eq, created-queries.log ]
 * completed_query_file_suffix= [ Filename suffix for completed query events eq, completed-queries.log ]
 *
 * fabric.kafka1 = [ eq, localhost:9092 ]
 * fabric.kafka2 = [ eq, localhost:8092 ]
 * fabric.batch_size = [ eq, 2 ]
 * fabric.circuit_breaker = [ eq, true ]
 * fabric.sleep_window = [ in millis eq, 60000 (60 sec)]
 * fabric.rolling_window = [ in millis eq, 60000 (60 sec) ]
 * fabric.request_volume = [ eq, 20 ]
 * fabric.error_threshold = [ eq, 1 ]
 * fabric.fallback = [ eq, true ]
 * fabric.fallback_timeout = [ in millis eq, 100 ]
 * fabric.opentsdb = [ eq, false ]
 * fabric.opentsdb_url = [  eq, http://localhost:4242 ]
 */

public class QueryEventListenerFactory implements EventListenerFactory {
    public String getName() {
        return "event-listener";
    }

    public EventListener create(Map<String, String> config) {
        return new QueryEventListener(config);
    }
}
