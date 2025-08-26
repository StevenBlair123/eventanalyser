using Newtonsoft.Json;

namespace eventanalyser.Projections;

using EventStore.Client;

public class StreamInfo {
    public String Name { get; set; }

    public UInt64 Count { get; set; }

    public Int64 SizeInBytes { get; set; }

    public StreamInfo(String name) {
        this.Name = name;
    }
}

public record OrganisationState : State {
    public Guid OrganisationId { get; }

    public OrganisationState(Guid organisationId) : base() {
        this.OrganisationId = organisationId;
    }

    public Dictionary<String, StreamInfo> StreamInfo { get; set; } = new();

    public override String GetStateAsString() {
        Dictionary<String, StreamInfo> sortedEventInfo = this.StreamInfo.OrderByDescending(e => e.Value.SizeInBytes).ToDictionary(e => e.Key, e => e.Value);

        this.StreamInfo = sortedEventInfo;

        return JsonConvert.SerializeObject(this);
    }
}

public class OrganisationRemovalProjection : Projection<OrganisationState> {
    private readonly EventStoreClient EventStoreClient;

    public OrganisationRemovalProjection(OrganisationState state, EventStoreClient eventStoreClient) : base(state) {
        this.EventStoreClient = eventStoreClient;
    }

    protected override async Task<OrganisationState> HandleEvent(OrganisationState state,
                                                                 ResolvedEvent @event) {


        if (@event.Event.ContentType != "application/json") {
            return await Task.FromResult(state);
        }

        String eventAsString = Support.ConvertResolvedEventToString(@event);
        var dyn = new {
                          organisationId = Guid.Empty
                      };
        var result = JsonConvert.DeserializeAnonymousType(eventAsString, dyn);

        if (result == null || result.organisationId == Guid.Empty) {
            return await Task.FromResult(state);
        }

        OrganisationState newState = state;

        String stream = @event.OriginalEvent.EventStreamId;

        if (!newState.StreamInfo.ContainsKey(stream)) {
            Console.WriteLine($"Deleting stream: {stream}");
            
            DeleteResult deleteResult = await this.EventStoreClient.DeleteAsync(stream,StreamState.Any);

            Console.WriteLine($"Deleted stream: {stream} {deleteResult.LogPosition.CommitPosition}");

            newState.StreamInfo.Add(stream, new StreamInfo(stream));
        }

        StreamInfo e = state.StreamInfo[stream];
        Int64 newSize = EventTypeSizeProjection.SizeOnDisk(@event.Event.EventType,
                                                           @event.OriginalEvent.Data.ToArray(),
                                                           @event.OriginalEvent.Metadata.ToArray(),
                                                           @event.OriginalStreamId);

        e.SizeInBytes += newSize;
        e.Count += 1;

        return await Task.FromResult(newState);
    }
}