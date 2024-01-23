using System;
using System.Collections.Generic;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Structure;
using Gremlin.Net.Structure.IO.GraphBinary;
using Gremlin.Net.Structure.IO.GraphSON;
using Xunit;
using Xunit.Abstractions;

namespace Gremlin.Net.IntegrationTest.Process.Traversal.DriverRemoteConnection
{
    public class SerializationTests
    {
        private readonly ITestOutputHelper _output;
        private readonly RemoteConnectionFactory _connectionFactory = new();

        public static List<object[]> testCases =>
            new()
            {
                //new object[] { DataType.Int, 1 },
                //new object[] { DataType.Long, 1L },
                //new object[] { DataType.String, "qq" },
                //new object[] { DataType.Date, DateTimeOffset.FromUnixTimeSeconds(1705105408) }, // not a DateTime, but who cares?
                //new object[] { DataType.Timestamp, DateTimeOffset.FromUnixTimeSeconds(1705105408) },
                //new object[] { DataType.Class, GremlinType.FromFqcn("test") }, // hangs
                //new object[] { DataType.Double, 1.0 },
                //new object[] { DataType.Float, 1.0f },
                //new object[] { DataType.List, new List<object>() { 1, 2, "test"} },
                //new object[] { DataType.Map, new Dictionary<string, string>() { ["hello"] = "world"} },
                //new object[] { DataType.Set, new HashSet<string>() { "test"} },
                //new object[] { DataType.Uuid, Guid.NewGuid() },
                //new object[] { DataType.Edge, new Edge(1, new Vertex(1), "person", new Vertex(1)) },
                //new object[] { DataType.Path, new Path(new List<ISet<string>>(), new List<object?>()) },
                //new object[] { DataType.Property, new Property("test", "prop") }

                new object[] { DataType.Vertex, 1 },
                new object[] { DataType.VertexProperty, 1 },
                new object[] { DataType.Barrier, 1 },
                new object[] { DataType.Binding, 1 },
                new object[] { DataType.Bytecode, 1 },
                new object[] { DataType.Cardinality, 1 },
                new object[] { DataType.Column, 1 },
                new object[] { DataType.Direction, 1 },
                new object[] { DataType.Operator, 1 },
                new object[] { DataType.Order, 1 },
                new object[] { DataType.Pick, 1 },
                new object[] { DataType.Pop, 1 },
                new object[] { DataType.Lambda, 1 },
                new object[] { DataType.P, 1 },
                new object[] { DataType.Scope, 1 },
            };


        private readonly GraphTraversalSource _gGraphSON;
        private readonly GraphTraversalSource _gGraphBinary;

        public SerializationTests(ITestOutputHelper output)
        {
            _output = output;

            _gGraphSON = AnonymousTraversalSource.Traversal()
                .WithRemote(_connectionFactory.CreateRemoteConnection("g", 2, new GraphSON3MessageSerializer()));

            _gGraphBinary = AnonymousTraversalSource.Traversal()
                .WithRemote(_connectionFactory.CreateRemoteConnection("g", 2, new GraphBinaryMessageSerializer()));
        }

        [Theory]
        [MemberData(nameof(testCases))]
        public void RoundTripTest(DataType dataType, object value)
        {
            try
            {
                var v = _gGraphSON.AddV().Property("test", value).Next()!;
                var serverValue = _gGraphSON.V(v.Id).Values<object>("test").Next();
                Assert.Equal(value, serverValue);

                _output.WriteLine($"GraphSON support {dataType.TypeCode}");
            }
            catch
            {
                _output.WriteLine($"GraphSON not support {dataType.TypeCode}");
            }

            try
            {
                var v = _gGraphBinary.AddV().Property("test", value).Next()!;
                var serverValue = _gGraphBinary.V(v.Id).Values<object>("test").Next();
                Assert.Equal(value, serverValue);

                _output.WriteLine($"GraphBinary support {dataType.TypeCode}");
            }
            catch
            {
                _output.WriteLine($"GraphBinary not support {dataType.TypeCode}");
            }
        }



    }
}
