/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.core.datastore.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.common.Maps;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.*;
import org.apache.carbondata.core.util.CarbonProperties;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**
 * FileType provider to create CarbonFile specific to the file system where the path belongs to.
 */
public class DefaultFileTypeProvider implements FileTypeInterface {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(DefaultFileTypeProvider.class.getName());

  /**
   * Custom file type provider for supporting non default file systems.
   */
  protected FileTypeInterface customFileTypeProvider = null;

  protected Boolean customFileTypeProviderInitialized = false;

  private final Object lock = new Object();

  protected Map<String, Pair<CarbonFile, Long>> carbonFileCache = new ConcurrentHashMap<>();

  private boolean carbonFileCacheEnable;
  private long carbonFileCacheInterval;

  public DefaultFileTypeProvider() {
  }

  /**
   * This method is required apart from Constructor to handle the below circular dependency.
   * CarbonProperties-->FileFactory-->DefaultTypeProvider-->CarbonProperties
   */
  private void initializeCustomFileProvider() {
    if (!customFileTypeProviderInitialized) {
      // This initialization can happen in concurrent threads.
      synchronized (lock) {
        if (!customFileTypeProviderInitialized) {
          String customFileProvider = CarbonProperties.getInstance()
              .getProperty(CarbonCommonConstants.CUSTOM_FILE_PROVIDER);
          if (customFileProvider != null && !customFileProvider.trim().isEmpty()) {
            try {
              customFileTypeProvider =
                  (FileTypeInterface) Class.forName(customFileProvider).newInstance();
            } catch (Exception e) {
              LOGGER.error("Unable load configured FileTypeInterface class. Ignored.", e);
            }
            customFileTypeProviderInitialized = true;
          }
        }
        this.carbonFileCacheEnable = Boolean.valueOf(CarbonProperties.getInstance().getProperty(
            CarbonCommonConstants.CARBON_FILE_CACHE_ENABLE, "false"));
        this.carbonFileCacheInterval = Long.valueOf(CarbonProperties.getInstance().getProperty(
            CarbonCommonConstants.CARBON_FILE_CACHE_INTERVAL, "120000"));
      }
    }
  }

  /**
   * Delegate to the custom file provider to check if the path is supported or not.
   * Note this function do not check the default supported file systems as  #getCarbonFile expects
   * this method output is from customFileTypeProvider.
   *
   * @param path path of the file
   * @return true if supported by the custom
   */
  @Override public boolean isPathSupported(String path) {
    initializeCustomFileProvider();
    if (customFileTypeProvider != null) {
      return customFileTypeProvider.isPathSupported(path);
    }
    return false;
  }

  @Override
  public CarbonFile getCarbonFile(String path, Configuration conf) {
    // Handle the custom file type first
    if (isPathSupported(path)) {
      return customFileTypeProvider.getCarbonFile(path, conf);
    }

    long currentTime = System.currentTimeMillis();
    if (this.carbonFileCacheEnable) {
      Pair<CarbonFile, Long> pair = Maps.getOrDefault(
          this.carbonFileCache, path, Pair.<CarbonFile, Long>of(null, -1L));
      CarbonFile lastFile = pair.getLeft();
      long lastTime = pair.getRight();
      if (lastFile != null && lastTime != -1
            && currentTime - this.carbonFileCacheInterval < lastTime) {
        return lastFile;
      }
    }
    LOGGER.debug("get carbon file path: " + path);

    FileFactory.FileType fileType = FileFactory.getFileType(path);
    CarbonFile carbonFile;
    switch (fileType) {
      case LOCAL:
        carbonFile = new LocalCarbonFile(FileFactory.getUpdatedFilePath(path, fileType));
        break;
      case HDFS:
        carbonFile = new HDFSCarbonFile(path, conf);
        break;
      case S3:
        carbonFile = new S3CarbonFile(path, conf);
        break;
      case ALLUXIO:
        carbonFile = new AlluxioCarbonFile(path);
        break;
      case VIEWFS:
        carbonFile = new ViewFSCarbonFile(path);
        break;
      default:
        carbonFile = new LocalCarbonFile(FileFactory.getUpdatedFilePath(path, fileType));
        break;
    }
    if (this.carbonFileCacheEnable) {
      this.carbonFileCache.put(path, Pair.of(carbonFile, currentTime));
    }
    return carbonFile;
  }
}
