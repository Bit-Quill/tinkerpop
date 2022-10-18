/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.driver.util;

import org.apache.tinkerpop.gremlin.util.Gremlin;

import javax.naming.NamingException;

public class UserAgent {
    public static String getUserAgent() {
        String applicationName = null;
        try {
            applicationName = (String)(new javax.naming.InitialContext().lookup("java:app/AppName"));
        } catch (NamingException e) {
            applicationName = "NotAvailable";
        };
        String glvVersion = Gremlin.version().replace(' ', '_');
        String javaVersion = System.getProperty("java.version").replace(' ', '_');
        String osName = System.getProperty("os.name").replace(' ', '_');
        String osVersion = System.getProperty("os.version").replace(' ', '_');
        String cpuArch = System.getProperty("os.arch").replace(' ', '_');

        return String.format("%s Gremlin-Driver/%s %s %s/%s %s",
                                applicationName, glvVersion, javaVersion,
                                osName, osVersion, cpuArch);
    }
}
