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

package me.jinsui.shennong.bench.utils;

import com.beust.jcommander.Parameter;
import java.util.ArrayList;
import java.util.List;

/**
 * Default CLI Options.
 */
public class CliFlags {

    @Parameter(description = "args")
    public List<String> arguments = new ArrayList<>();

    @Parameter(
        names = {
            "-h", "--help"
        },
        description = "Display help information")
    public boolean help = false;

    @Parameter(
        names = {
            "-n", "--num-events"
        },
        description = "Number of events to write in total. If 0, it will keep writing")
    public long numEvents = 0;

    @Parameter(
        names = {
            "-b", "--num-bytes"
        },
        description = "Number of bytes to write in total. If 0, it will keep writing")
    public long numBytes = 0;

    @Parameter(
        names = {
            "-r", "--rate"
        },
        description = "Write rate bytes/s across all streams/topics/files")
    public double writeRate = 10000000;

    @Parameter(
        names = {
            "-bypass", "--bypass-server"
        },
        description = "Bypass server to monitor bench performance. Default 0")
    public long bypass = 0;

}
