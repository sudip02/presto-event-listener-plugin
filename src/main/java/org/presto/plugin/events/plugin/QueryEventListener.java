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
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.SplitCompletedEvent;
import org.presto.plugin.events.mapper.ObjectMapperFactory;
import org.presto.plugin.events.mapper.PrestoObjectMapper;
import org.presto.plugin.events.writer.EventWriter;
import org.presto.plugin.events.writer.EventWriterFactory;
import io.airlift.log.Logger;

import java.util.Map;

/**
 * Created by sudip.hc on 01/02/18.
 */

public class QueryEventListener implements EventListener {
    private static final Logger log = Logger.get(QueryEventListener.class);

    EventWriter queryCreated, queryCompleted, querySplitted;
    PrestoObjectMapper mapper;

    public QueryEventListener(Map<String, String> config) {
        log.info("Query Logger Event Listener Config: %s", config);

        queryCreated = EventWriterFactory.getEventWriter(config, EventWriter.Event.Create);
        queryCompleted = EventWriterFactory.getEventWriter(config, EventWriter.Event.Complete);
        querySplitted =  EventWriterFactory.getEventWriter(config, EventWriter.Event.Split);

        ObjectMapperFactory factory = new ObjectMapperFactory();
        mapper = factory.getPrestoObjectMapper(config.get("mapper"));

    }

    public void queryCompleted(QueryCompletedEvent queryCompletedEvent) {
        queryCompleted.write(mapper.getQueryCompletedEvent(queryCompletedEvent));
    }

    public void queryCreated(QueryCreatedEvent queryCreatedEvent) {
        queryCreated.write(mapper.getQueryCreatedEvent(queryCreatedEvent));
    }

    public void splitCompleted(SplitCompletedEvent splitCompletedEvent) {
        querySplitted.write(mapper.getQuerySplittedEvent(splitCompletedEvent));
    }

}

