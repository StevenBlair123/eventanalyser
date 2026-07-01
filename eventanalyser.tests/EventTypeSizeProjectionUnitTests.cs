using System.Text;
using eventanalyser.Projections;
using KurrentDB.Client;
using Shouldly;

namespace eventanalyser.tests;

public class EventTypeSizeProjectionUnitTests {
    [Test]
    public async Task resolved_events_are_grouped_by_event_type_and_original_stream() {
        var options = new Options("", "") {
            ReloadState = false
        };

        EventTypeSizeProjection projection = new(options);

        await projection.Handle(CreateResolvedEvent("stream-1", "SaleStarted", "{ \"id\": 1 }"));
        State result = await projection.Handle(CreateResolvedEvent("stream-2", "SaleStarted", "{ \"id\": 2 }"));

        result.ShouldBeOfType<EventTypeSizeState>();

        EventTypeSizeState state = (EventTypeSizeState)result;
        state.EventInfo.Count.ShouldBe(2);
        state.EventInfo.Keys.ShouldContain("$>SaleStarted-stream-1");
        state.EventInfo.Keys.ShouldContain("$>SaleStarted-stream-2");
    }

    private static ResolvedEvent CreateResolvedEvent(String streamId, String eventType, String body) {
        Dictionary<String, String> metadata = new() {
            ["type"] = eventType,
            ["created"] = "1",
            ["content-type"] = "json"
        };

        Byte[] data = Encoding.UTF8.GetBytes(body);
        Byte[] eventMetadata = Array.Empty<Byte>();
        Position position = new(2, 1);
        EventRecord eventRecord = new(streamId,
                                      Uuid.FromGuid(Guid.NewGuid()),
                                      StreamPosition.Start,
                                      position,
                                      metadata,
                                      data,
                                      eventMetadata);

        return new ResolvedEvent(eventRecord, eventRecord, 2);
    }
}
