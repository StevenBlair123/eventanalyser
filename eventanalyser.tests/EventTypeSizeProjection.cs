using eventanalyser.Projections;

namespace eventanalyser.tests {
    using EventStore.Client;
    using System;
    using System.Text;
    using Shouldly;

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