using eventanalyser.Projections;

namespace eventanalyser.tests {
    using EventStore.Client;
    using Grpc.Core;
    using Microsoft.Extensions.Options;
    using Shouldly;
    using System;
    using System.Text;
    using System.Threading;
    using static eventanalyser.Projections.DeleteOptions;
    using static System.Runtime.InteropServices.JavaScript.JSType;
    using StreamState = Projections.StreamState;
    using String = System.String;

    public class ProjectionTests {
        [SetUp]
        public void Setup() {
            
        }

        //[Test]
        public async Task Test() {
            //AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

            //Int32 port = DockerHelper.EventStoreHttpPort;

            //var settings = EventStoreClientSettings.Create(
            //                                               $"esdb://admin:changeit@127.0.0.1:{port}?tls=false&tlsVerifyCert=false"
            //                                              );
            //var client = new EventStoreClient(settings);
            //var client = new EventStoreClient(settings);
            var client = DockerHelper.EventStoreClient;

            // Quick test: write and read event
            var evt = new EventData(
                                    Uuid.NewUuid(),
                                    "test-event",
                                    Encoding.UTF8.GetBytes("{\"msg\":\"hello\"}")
                                   );

            await client.AppendToStreamAsync("test-stream", EventStore.Client.StreamState.Any, new[] { evt });
            Console.WriteLine("Event appended.");

            var result = client.ReadStreamAsync(Direction.Backwards, "test-stream", StreamPosition.End, 1);
            await foreach (var re in result)
                Console.WriteLine(Encoding.UTF8.GetString(re.Event.Data.Span));
        }

        [Test]
        public async Task safe_mode_is_enabled_by_default() {
            StreamState streamState = new();
            Guid organisationId = Guid.NewGuid();
            DeleteOrganisation deleteOptions = new(Guid.NewGuid());

            string @event = $@"{{
  ""organisationId"": ""{organisationId}""
}}";

            Byte[] byteArray = Encoding.UTF8.GetBytes(@event);

            ReadOnlyMemory<Byte> data = new(byteArray);

            IDictionary<String, String> metadata = new Dictionary<String, String>();

            metadata.Add("type", "event1");
            metadata.Add("created", "1");
            metadata.Add("content-type", "json");

            StreamRemovalProjection projection = new(streamState, deleteOptions, DockerHelper.EventStoreClient);
            EventRecord eventRecord = new("TestStream1", Uuid.NewUuid(), StreamPosition.FromInt64(1), new Position(0, 0), metadata, data, null);

            EventData eventData =  new(Uuid.NewUuid(), "TestEvent", eventRecord.Data);

            //TODO: How do we verify the event was not deleted
            ResolvedEvent resolvedEvent = new(eventRecord, null, null);

            //await DockerHelper.EventStoreClient.DeleteAsync("TestStream", EventStore.Client.StreamState.Any);

            await DockerHelper.EventStoreClient.AppendToStreamAsync("TestStream", StreamRevision.None, [eventData]);

            //var result = DockerHelper.EventStoreClient.ReadAllAsync(Direction.Backwards, Position.Start);

            //await foreach (var re in result)
            //{
            //    var evt = re.Event;
            //    Console.WriteLine($"Event Type: {evt.EventType}");
            //    Console.WriteLine($"Event Data: {System.Text.Encoding.UTF8.GetString(evt.Data.Span)}");
            //}

            //TODO: How do we determine no delete?
            //The event still being there is one thing, but what if we have not configured this correctly?
            //Meaning the event would still be there anyway
            var newState = await projection.Handle(resolvedEvent);
        }

        [Test]
        public async Task stream_meta_data_set_to_max_eventCount() {
            StreamState streamState = new();
            Guid organisationId = Guid.NewGuid();
            SetStreamMaxEventCount deleteOptions = new(2, new List<string>() {
                "TestEvent"
            });
            deleteOptions = deleteOptions with {
                SafeMode = false
            };

            string @event = $@"{{
  ""organisationId"": ""{organisationId}""
}}";

            Byte[] byteArray = Encoding.UTF8.GetBytes(@event);

            ReadOnlyMemory<Byte> data = new(byteArray);

            IDictionary<String, String> metadata = new Dictionary<String, String>();

            metadata.Add("type", "TestEvent");
            metadata.Add("created", "1");
            metadata.Add("content-type", "application/json");

            StreamRemovalProjection projection = new(streamState, deleteOptions, DockerHelper.EventStoreClient);

            // Wrtite some events to a stream
            for (int i = 0; i < 10; i++) {
                EventRecord eventRecord = new("TestStream1", Uuid.NewUuid(), StreamPosition.FromInt64(1),
                    new Position(0, 0), metadata, data, null);

                EventData eventData = new(Uuid.NewUuid(), "TestEvent", eventRecord.Data);
                await DockerHelper.EventStoreClient.AppendToStreamAsync("TestStream1",
                    EventStore.Client.StreamState.Any, [eventData]);
            }

            var x = DockerHelper.EventStoreClient.ReadStreamAsync(Direction.Backwards, "TestStream1",
                StreamPosition.End, 100);
            var g = await x.ToListAsync();
            g.Count.ShouldBe(10);

            EventRecord eventRecordX = new("TestStream1", Uuid.NewUuid(), StreamPosition.FromInt64(1),
                new Position(0, 0), metadata, data, null);
            //TODO: How do we verify the event was not deleted
            ResolvedEvent resolvedEvent = new(eventRecordX, null, null);

            State newState = await projection.Handle(resolvedEvent);

            x = DockerHelper.EventStoreClient.ReadStreamAsync(Direction.Backwards, "TestStream1", StreamPosition.End,
                10);
            g = await x.ToListAsync();
            g.Count.ShouldBe(2);

        }

        [Test]
        public async Task event_size_is_recorded() {
            EventTypeSizeState state = new();
            eventanalyser.Options options = new("", "");
            Projection<EventTypeSizeState> projection = new EventTypeSizeProjection(state, options);

            String @event = @"{
  ""id"": 1
}";

            var byteArray = Encoding.UTF8.GetBytes(@event);

            ReadOnlyMemory<Byte> data = new(byteArray);

            IDictionary<String, String> metadata = new Dictionary<String, String>();

            metadata.Add("type", "event1");
            metadata.Add("created", "1");
            metadata.Add("content-type", "json");

            EventRecord eventRecord = new("TestStream1", Uuid.NewUuid(), StreamPosition.FromInt64(1), new Position(0, 0), metadata, data, null);

            ResolvedEvent resolvedEvent = new(eventRecord, null, null);

            State result = await projection.Handle(resolvedEvent);

            //EventTypeSizeState
            result.ShouldBeOfType<EventTypeSizeState>();
            EventTypeSizeState finalState = result as EventTypeSizeState;

            finalState.EventInfo.Count.ShouldBe(1);

            Int32 eventCount = finalState.EventInfo.Count;

            eventCount.ShouldBe(1);

            EventInfo eventInfo = finalState.EventInfo.First().Value;

            var sizeInBytes = eventInfo.SizeInBytes;
            sizeInBytes.ShouldBe(96);
        }

        [Test]
        public async Task stream_delete_only_deletes_streams_older_than_date() {
            StreamState streamState = new();
            Guid organisationId = Guid.NewGuid();
            // TODO: add second test event
            DeleteStreamBefore deleteOptions = new(new DateTime(2025,10,30), new List<string>() {
                "OldSaleStarted", "SaleStarted"
            });
            deleteOptions = deleteOptions with {
                SafeMode = false
            };
            StreamRemovalProjection projection = new(streamState, deleteOptions, DockerHelper.EventStoreClient);

            var startDate = new DateTime(2025, 9, 1);
            var endDate = new DateTime(2025, 9, 30);

            List<ResolvedEvent> resolvedEvents = new();

            for (var date = startDate; date <= endDate; date = date.AddDays(1))
            {
                var r = await WriteSaleEvent(date, organisationId, isOldEvent:true);
                resolvedEvents.Add(r);
                Console.WriteLine($"Processing date: {date:yyyy-MM-dd}");
            }

            // We need to add some sales to the database of different dates
            startDate = new DateTime(2025, 10, 1);
            endDate = new DateTime(2025, 10, 31);
            
            for (var date = startDate; date <= endDate; date = date.AddDays(1)) {
                var r = await WriteSaleEvent(date, organisationId);
                resolvedEvents.Add(r);
                Console.WriteLine($"Processing date: {date:yyyy-MM-dd}");
            }

            var x = DockerHelper.EventStoreClient.ReadStreamAsync(Direction.Backwards, "$ce-SalesTransactionAggregate",
                StreamPosition.End, 100);
            var g = await x.ToListAsync();
            g.Count.ShouldBe(61);

            foreach (ResolvedEvent resolvedEvent in resolvedEvents) {
                await projection.Handle(resolvedEvent);
            }

            Int32 deletedStreamCount = 0;
            Int32 notDeletedStreamCount = 0;
            // Now check the sales have been deleted correctly
            foreach (ResolvedEvent resolvedEvent in resolvedEvents) {
                var stream = resolvedEvent.OriginalStreamId;
                var eventString = Support.ConvertResolvedEventToString(resolvedEvent);
                var date = Support.GetDateFromEvent(eventString);

                var readStremResult = DockerHelper.EventStoreClient.ReadStreamAsync(Direction.Backwards, stream, StreamPosition.End);
                var readstate = await readStremResult.ReadState;

                if (DateTime.Parse(date) < deleteOptions.DateTime.Date) {
                    readstate.ShouldBe(ReadState.StreamNotFound, date);
                    deletedStreamCount++;
                }
                else {
                    readstate.ShouldBe(ReadState.Ok, date);
                    notDeletedStreamCount++;
                }
            }

            deletedStreamCount.ShouldBe(59);
            notDeletedStreamCount.ShouldBe(2);
        }

        internal ResolvedEvent CreateResolvedEvent(String streamName,String eventType, Guid organisationId) {
            string @event = $@"{{
  ""organisationId"": ""{organisationId}""
}}";

            Byte[] byteArray = Encoding.UTF8.GetBytes(@event);

            ReadOnlyMemory<Byte> data = new(byteArray);

            IDictionary<String, String> metadata = new Dictionary<String, String>();

            metadata.Add("type", eventType);
            metadata.Add("created", "1");
            metadata.Add("content-type", "application/json");

            EventRecord eventRecord = new(streamName, Uuid.NewUuid(), StreamPosition.FromInt64(1), new Position(0, 0), metadata, data, null);

            ResolvedEvent resolvedEvent = new(eventRecord, null, null);

            return resolvedEvent;
        }

        internal async Task<ResolvedEvent> WriteSaleEvent(DateTime dateTime, Guid organisationId, System.Boolean isOldEvent=false) {
            Guid salesTransactionId  = Guid.NewGuid();
            String streamName = $"SalesTransactionAggregate-{salesTransactionId:N}";
            string @event = $@"{{
    ""organisationId"": ""{organisationId}"",
    ""stid"": ""{salesTransactionId}"",
    ""dt"": ""{dateTime:O}""
}}";

            String eventType = isOldEvent switch {
                true => "OldSaleStarted",
                _ => "SaleStarted"
            };

            Byte[] byteArray = Encoding.UTF8.GetBytes(@event);

            ReadOnlyMemory<Byte> data = new(byteArray);

            IDictionary<String, String> metadata = new Dictionary<String, String>();

            metadata.Add("type", eventType);
            metadata.Add("created", "1");
            metadata.Add("content-type", "application/json");

            EventRecord eventRecord = new(streamName, Uuid.NewUuid(), StreamPosition.FromInt64(1),
                new Position(0, 0), metadata, data, null);

            EventData eventData = new(Uuid.NewUuid(), eventType, eventRecord.Data);
            await DockerHelper.EventStoreClient.AppendToStreamAsync(streamName,
                EventStore.Client.StreamState.Any, [eventData]);

            ResolvedEvent resolvedEvent = new(eventRecord, null, null);
            return resolvedEvent;
        }

        //TODO: Startpoint
        //TODO: Delete Organisation
    }
}