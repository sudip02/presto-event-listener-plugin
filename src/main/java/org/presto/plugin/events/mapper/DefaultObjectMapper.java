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

package org.presto.plugin.events.mapper;

import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.SplitCompletedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.airlift.log.Logger;

public class DefaultObjectMapper extends PrestoObjectMapper {
    private static final Logger log = Logger.get(DefaultObjectMapper.class);

    @Override
    public String getQueryCompletedEvent(QueryCompletedEvent queryCompletedEvent) {
        try {
            return objectMapper.writeValueAsString(queryCompletedEvent);
        } catch (JsonProcessingException e) {
            log.error("Issue converting Completed Event pojo to string", e);
        }
        return null;
    }

    @Override
    public String getQueryCreatedEvent(QueryCreatedEvent queryCreatedEvent) {
        try {
            return objectMapper.writeValueAsString(queryCreatedEvent);
        } catch (JsonProcessingException e) {
            log.error("Issue converting Created Event pojo to string", e);
        }
        return null;
    }

    @Override
    public String getQuerySplittedEvent(SplitCompletedEvent splitCompletedEvent) {
        try {
            return objectMapper.writeValueAsString(splitCompletedEvent);
        } catch (JsonProcessingException e) {
            log.error("Issue converting Created Event pojo to string", e);
        }
        return null;
    }
}
