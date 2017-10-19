/**
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
package me.jinsui.shennong.test;

import org.apache.bookkeeper.util.IOUtils;
import org.apache.distributedlog.DLMTestUtil;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.LocalDLMEmulator;
import org.apache.distributedlog.LogSegmentMetadata;

import java.io.File;



/**
 * Test bk zk connection, mostly from TestDistributedLogBase to find the right use of LocalDLMEmulator.
 */
public class TestConnection {

    static {
        // org.apache.zookeeper.test.ClientBase uses FourLetterWordMain, from 3.5.3 four letter words
        // are disabled by default due to security reasons
        System.setProperty("zookeeper.4lw.commands.whitelist", "*");
    }

    // Num worker threads should be one, since the exec service is used for the ordered
    // future pool in test cases, and setting to > 1 will therefore result in unordered
    // write ops.
    protected static DistributedLogConfiguration conf =
        new DistributedLogConfiguration()
                .setEnableReadAhead(true)
                .setReadAheadMaxRecords(1000)
                .setReadAheadBatchSize(10)
                .setLockTimeout(1)
                .setNumWorkerThreads(1)
                .setReadAheadNoSuchLedgerExceptionOnReadLACErrorThresholdMillis(20)
                .setSchedulerShutdownTimeoutMs(0)
                .setDLLedgerMetadataLayoutVersion(LogSegmentMetadata.LEDGER_METADATA_CURRENT_LAYOUT_VERSION);
    protected static LocalDLMEmulator bkutil;

    protected static int zkPort;
    protected static int numBookies = 3;

    public static void setupCluster() throws Exception {
        File zkTmpDir = IOUtils.createTempDir("zookeeper", "distrlog");
        zkPort = LocalDLMEmulator.runZookeeperOnAnyPort(zkTmpDir).getRight();
        bkutil = LocalDLMEmulator.newBuilder()
                .numBookies(numBookies)
                .zkHost("127.0.0.1")
                .zkPort(zkPort)
                .serverConf(DLMTestUtil.loadTestBkConf())
                .shouldStartZK(false)
                .build();
        bkutil.start();

    }

//    public static void main(String[] args) throws Exception {
//
//        setupCluster();
//
//        System.out.println("after setup cluster");
//    }
}
