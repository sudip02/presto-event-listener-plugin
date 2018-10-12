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

import com.facebook.presto.spi.eventlistener.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airlift.log.Logger;

import java.util.Optional;

/**
 * Created by sudip.hc on 01/02/18.
 */
public class CustomObjectMapper extends PrestoObjectMapper {
    private static final Logger log = Logger.get(CustomObjectMapper.class);

    public static class Key {
        public static final String USER = "user";
        public static final String QUERY_ID = "query_id";
        public static final String QUERY = "query";
        public static final String STATE = "state";
        public static final String ENV = "environment";
        public static final String CREATE_TIME = "create_time";
        public static final String SERVER_ADDRESS = "server_address";
        public static final String SERVER_VERSION = "server_version";
    }

    public String getQueryCompletedEvent(QueryCompletedEvent queryCompletedEvent) {

        ObjectNode root = objectMapper.createObjectNode();

        root.put(Key.USER, queryCompletedEvent.getContext().getUser());
        root.put(Key.QUERY_ID, queryCompletedEvent.getMetadata().getQueryId());
        root.put(Key.QUERY, queryCompletedEvent.getMetadata().getQuery());
        root.put(Key.STATE, queryCompletedEvent.getMetadata().getQueryState());
        root.put(Key.CREATE_TIME, queryCompletedEvent.getCreateTime().toEpochMilli());
        root.put("start_time", queryCompletedEvent.getExecutionStartTime().toEpochMilli());
        root.put("end_time", queryCompletedEvent.getEndTime().toEpochMilli());

        if (queryCompletedEvent.getFailureInfo().isPresent()) {
            QueryFailureInfo queryFailureInfo = queryCompletedEvent.getFailureInfo().get();

            root.put("error_code", queryFailureInfo.getErrorCode().getCode());
            root.put("error_code_name", queryFailureInfo.getErrorCode().getName());
            root.put("error_code_type", queryFailureInfo.getErrorCode().getType().name());
            root.put("failure_json", queryFailureInfo.getFailuresJson());

            setIfNotNull(root, "failure_type", queryFailureInfo.getFailureType());
            setIfNotNull(root, "failure_task", queryFailureInfo.getFailureTask());
            setIfNotNull(root, "failure_host", queryFailureInfo.getFailureHost());
            setIfNotNull(root, "failure_message", queryFailureInfo.getFailureMessage());

        }


        ArrayNode inputList = root.putArray("inputs");
        for (QueryInputMetadata queryInputMetadata: queryCompletedEvent.getIoMetadata().getInputs()) {
            ObjectNode node = objectMapper.createObjectNode();
            node.put("connector_id", queryInputMetadata.getConnectorId());
            node.put("schema", queryInputMetadata.getSchema());
            node.put("table", queryInputMetadata.getTable());

            ArrayNode columnList = node.putArray("columns");
            for(String column: queryInputMetadata.getColumns())
                columnList.add(column);
            inputList.add(node);
        }

        root.put("completed_splits", queryCompletedEvent.getStatistics().getCompletedSplits());
        root.put("cumulative_memory", queryCompletedEvent.getStatistics().getCumulativeMemory());
        root.put("total_bytes", queryCompletedEvent.getStatistics().getTotalBytes());
        root.put("total_rows", queryCompletedEvent.getStatistics().getTotalRows());
        root.put("cpu_time", queryCompletedEvent.getStatistics().getCpuTime().toMillis());
        root.put("queued_time", queryCompletedEvent.getStatistics().getQueuedTime().toMillis());
        root.put("wall_time", queryCompletedEvent.getStatistics().getWallTime().toMillis());

        root.put(Key.ENV, queryCompletedEvent.getContext().getEnvironment());
        root.put(Key.SERVER_ADDRESS, queryCompletedEvent.getContext().getServerAddress());
        root.put(Key.SERVER_VERSION, queryCompletedEvent.getContext().getServerVersion());
        setIfNotNull(root, "schema", queryCompletedEvent.getContext().getSchema());
        setIfNotNull(root, "source", queryCompletedEvent.getContext().getSource());
        setIfNotNull(root, "catalog", queryCompletedEvent.getContext().getCatalog());
        setIfNotNull(root, "principal", queryCompletedEvent.getContext().getPrincipal());
        setIfNotNull(root, "client_info", queryCompletedEvent.getContext().getClientInfo());
        setIfNotNull(root, "remote_client_address", queryCompletedEvent.getContext().getRemoteClientAddress());
        setIfNotNull(root, "user_agent", queryCompletedEvent.getContext().getUserAgent());

        return root.toString();
    }


    public String getQueryCreatedEvent(QueryCreatedEvent queryCreatedEvent) {

        ObjectNode root = objectMapper.createObjectNode();

        root.put(Key.USER, queryCreatedEvent.getContext().getUser());
        root.put(Key.QUERY_ID, queryCreatedEvent.getMetadata().getQueryId());
        root.put(Key.QUERY, queryCreatedEvent.getMetadata().getQuery());
        root.put(Key.STATE, queryCreatedEvent.getMetadata().getQueryState());
        root.put(Key.CREATE_TIME, queryCreatedEvent.getCreateTime().toEpochMilli());
        root.put(Key.ENV, queryCreatedEvent.getContext().getEnvironment());
        root.put(Key.SERVER_ADDRESS, queryCreatedEvent.getContext().getServerAddress());
        root.put(Key.SERVER_VERSION, queryCreatedEvent.getContext().getServerVersion());

        setIfNotNull(root, "transaction_id", queryCreatedEvent.getMetadata().getTransactionId());

        setIfNotNull(root, "schema", queryCreatedEvent.getContext().getSchema());
        setIfNotNull(root, "source", queryCreatedEvent.getContext().getSource());
        setIfNotNull(root, "catalog", queryCreatedEvent.getContext().getCatalog());
        setIfNotNull(root, "principal", queryCreatedEvent.getContext().getPrincipal());
        setIfNotNull(root, "client_info", queryCreatedEvent.getContext().getClientInfo());
        setIfNotNull(root, "remote_client_address", queryCreatedEvent.getContext().getRemoteClientAddress());
        setIfNotNull(root, "user_agent", queryCreatedEvent.getContext().getUserAgent());

        return root.toString();
    }

    public String getQuerySplittedEvent(SplitCompletedEvent splitCompletedEvent) {
        try {
            return objectMapper.writeValueAsString(splitCompletedEvent);
        } catch (JsonProcessingException e) {
            log.error("Issue converting Created Event pojo to string", e);
        }
        return null;
    }


    private void setIfNotNull(ObjectNode node, String key, Optional<? extends String> value) {
        if (value.isPresent())
            node.put(key, value.get());
    }

}
