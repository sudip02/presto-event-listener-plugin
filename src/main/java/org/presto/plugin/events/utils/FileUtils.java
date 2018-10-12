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

package org.presto.plugin.events.utils;


import io.airlift.log.Logger;

import java.io.File;
import java.io.IOException;

public class FileUtils {
    private final static Logger log = Logger.get(FileUtils.class);

    public static String cleanPath(String path) {
        path = path.trim();
        if (path.endsWith("/"))
            path = path.substring(0, path.length() - 2);
        return path;
    }

    // Creates the directory if not exists
    public static void createDirectory(String path) throws IOException {
        File directory = new File(path);
        if (directory.exists())
            return;
        log.info("Required base directory %s does not exists. Creating it.", path);
        if (!directory.mkdirs())
            throw new IOException("Unable to created base directory " + path);
    }

    public static String getFilePath(String basePath, String filename) {
        if (basePath.trim().endsWith("/"))
            return (basePath.trim() + filename.trim());
        else
            return basePath.trim() + "/" + filename.trim();
    }

    public static String getFilename(String datePrefix, String nameSuffix) {
        return datePrefix + "_" + nameSuffix;
    }

    public static boolean moveFile(String sourceFolder, String targetFolder, String name) {
        File source = new File(getFilePath(sourceFolder, name));
        File target = new File(getFilePath(targetFolder, name));
        return source.renameTo(target);
    }
}
