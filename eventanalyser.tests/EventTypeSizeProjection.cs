using eventanalyser.Projections;

namespace eventanalyser.tests {
    using EventStore.Client;
    using System;
    using System.Text;
    using Shouldly;
    using StreamState = Projections.StreamState;
    using static eventanalyser.Projections.DeleteOptions;
    using String = System.String;

    public class DeleteOrganisationTests {
        [SetUp]
        public void Setup() {

            //DockerHelper.StartContainerForEventStore().Wait();
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

            StreamState newState = await projection.Handle(resolvedEvent);
        }

        [Test]
        public async Task stream_meta_data_set_to_max_eventCount()
        {
            StreamState streamState = new();
            Guid organisationId = Guid.NewGuid();
            SetStreamMaxEventCount deleteOptions = new(2, new List<string>() {
                "TestEvent"
            });

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
                EventRecord eventRecord = new("TestStream1", Uuid.NewUuid(), StreamPosition.FromInt64(1), new Position(0, 0), metadata, data, null);

                EventData eventData = new(Uuid.NewUuid(), "TestEvent", eventRecord.Data);
                await DockerHelper.EventStoreClient.AppendToStreamAsync("TestStream", EventStore.Client.StreamState.Any, [eventData]);
            }
            EventRecord eventRecordX = new("TestStream1", Uuid.NewUuid(), StreamPosition.FromInt64(1), new Position(0, 0), metadata, data, null);
            //TODO: How do we verify the event was not deleted
            ResolvedEvent resolvedEvent = new(eventRecordX, null, null);
            
            StreamState newState = await projection.Handle(resolvedEvent);
        }
    }

    public class Tests {
        [SetUp]
        public void Setup() {
        }

        //TODO: Date ranges etc

        [Test]
        public async Task event_size_is_recorded() {
            EventTypeSizeState state = new();
            Options options = new("", "");
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

            EventTypeSizeState result = await projection.Handle(resolvedEvent);

            result.EventInfo.Count.ShouldBe(1);

            Int32 eventCount = result.EventInfo.Count;

            eventCount.ShouldBe(1);

            EventInfo eventInfo = result.EventInfo.First().Value;

            var sizeInBytes = eventInfo.SizeInBytes;
            sizeInBytes.ShouldBe(96);
        }
    }
}