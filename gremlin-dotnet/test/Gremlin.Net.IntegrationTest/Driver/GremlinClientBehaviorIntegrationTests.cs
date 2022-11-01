#region License

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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#endregion

using System;
using System.Linq;
using System.Threading.Tasks;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.Driver.Messages;
using Gremlin.Net.IntegrationTest.Util;
using Gremlin.Net.Structure;
using Gremlin.Net.Structure.IO.GraphBinary;
using Xunit;

namespace Gremlin.Net.IntegrationTest.Driver
{
    public class GremlinClientBehaviorIntegrationTests
    {
        private static readonly string TestHost = ConfigProvider.Configuration["GremlinSocketServerIpAddress"]!;

        private static readonly SocketServerSettings Settings =
            SocketServerSettings.FromYaml(ConfigProvider.Configuration["GremlinSocketServerConfig"]);

        
        [Fact]
        public async Task shouldTryCreateNewConnectionIfClosedByServer()
        {
            var sessionId = Guid.NewGuid().ToString();
            var poolSettings = new ConnectionPoolSettings {PoolSize = 1};
            
            GremlinServer gremlinServer = new GremlinServer(TestHost, Settings.PORT);
            GremlinClient gremlinClient = new GremlinClient(gremlinServer, messageSerializer: new GraphBinaryMessageSerializer(), connectionPoolSettings: poolSettings, sessionId: sessionId);

            Assert.Equal(1, gremlinClient.NrConnections);
            
            Task<Vertex?> response1 = gremlinClient.SubmitWithSingleResultAsync<Vertex>(RequestMessage.Build("1").OverrideRequestId(Settings.CLOSE_CONNECTION_REQUEST_ID).Create());

            try
            {
                await response1;
            }
            catch (ConnectionClosedException e)
            {
                Assert.Contains("CloseStatus: NormalClosure", e.Message);
            }

            Assert.True(response1.IsFaulted);

            Vertex? response2 = await gremlinClient.SubmitWithSingleResultAsync<Vertex>(RequestMessage.Build("1").OverrideRequestId(Settings.SINGLE_VERTEX_REQUEST_ID).Create());
            Assert.NotNull(response2);
            Assert.Equal(1, gremlinClient.NrConnections);
        }


    }
}
