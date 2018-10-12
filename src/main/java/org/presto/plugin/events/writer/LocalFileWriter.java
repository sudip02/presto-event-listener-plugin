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

import org.presto.plugin.events.utils.FileUtils;
import io.airlift.log.Logger;

import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

/**
 * Created by sudip.hc on 01/02/18.
 */
public class LocalFileWriter implements EventWriter {
    private static final Logger log = Logger.get(LocalFileWriter.class);

    private final String defaultTimeZone = "GMT";
    private final String defaultDateFormat = "yyyy-MM-dd";
    private static final String NEW_LINE = "\n";

    FileWriter writer;
    String basePath, completedPath;
    DateFormat gmtFormat;
    String suffix, existingPrefix;

    public LocalFileWriter(Map<String, String> config, Event eventName) {

        try {
            String datePrefixFormat = config.containsKey("date_prefix_format") ? config.get("date_prefix_format") : defaultDateFormat;
            gmtFormat = new SimpleDateFormat(datePrefixFormat);
            String timeZone = config.containsKey("time_zone") ? config.get("time_zone") : defaultTimeZone;
            gmtFormat.setTimeZone(TimeZone.getTimeZone(timeZone));

            basePath = FileUtils.cleanPath(config.get("current_base_path"));
            FileUtils.createDirectory(basePath);
            completedPath = FileUtils.cleanPath(config.get("completed_base_path"));
            FileUtils.createDirectory(completedPath);

            switch (eventName) {
                case Complete:
                    suffix = config.get("created_query_file_suffix");
                    break;
                case Create:
                    suffix = config.get("completed_query_file_suffix");
                    break;
                case Split:
                    suffix = config.get("splitted_query_file_suffix");
                    break;
                default:
                    throw new IOException("Non recognizable event name " + eventName);
            }

        } catch (IOException e) {
            log.error("Unable to initialize Query Logger Event Listener", e);
        }
    }

    private void updatedWriter() throws IOException {
        String datePrefix = gmtFormat.format(new Date());
        if (datePrefix.equals(existingPrefix) && (writer != null))
            return;
        if (writer != null) {
            writer.close();
            if (!FileUtils.moveFile(basePath, completedPath, FileUtils.getFilename(existingPrefix, suffix)))
                log.error("Could not move the query log files to the target directory");
        }
        writer = new FileWriter(FileUtils.getFilePath(basePath, FileUtils.getFilename(datePrefix, suffix)), true);
        existingPrefix = datePrefix;
    }

    public synchronized void write(String content) {
        try {
            updatedWriter();
            writer.append(content).append(NEW_LINE);
            writer.flush();
        } catch (IOException e) {
            log.error("Issue from Query Event Listener Writer", e);
        }
    }

}
