/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink;

import org.apache.flink.sql.SqlExecutor;

/** Main class for executing SQL scripts. */
public class FlinkSqlRunner {

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new Exception("Exactly one argument is expected.");
        }
        SqlExecutor sqlExecutor = new SqlExecutor();
        sqlExecutor.executeSqlFile(args[0]);
    }
}