namespace eventanalyser.Projections;

using EventStore.Client;
using Newtonsoft.Json;
using String = System.String;

public class EventInfo {
    public String Type { get; set; }
    public UInt64 Count { get; set; }
    public Double SizeInMegabytes => this.SizeInBytes / (1024.0 * 1024.0);
    public Int64 SizeInBytes { get; set; }
    public Int32 AverageSizePerEventInBytes => this.Count > 0 ? (Int32)Math.Ceiling((Double)this.SizeInBytes / this.Count) : 0;

    public EventInfo(String type) {
        this.Type = type;
    }
}

public record EventTypeSizeState : State {
    public EventTypeSizeState() : base() {
        
    }

    public Double TotalSizeInMegabytes {
        get => this.EventInfo.Sum(e => e.Value.SizeInMegabytes);
    }

    public UInt64 TotalEvents {
        get => (UInt64)this.EventInfo.Sum(e => (Int64)e.Value.Count);
    }

    public Dictionary<String, EventInfo> EventInfo { get; set; } = new();

    public override String GetStateAsString() {
        Dictionary<String, EventInfo> sortedEventInfo = this.EventInfo
                                                            .OrderByDescending(e => e.Value.SizeInBytes)
                                                            .ToDictionary(e => e.Key, e => e.Value);

        this.EventInfo = sortedEventInfo;

        return JsonConvert.SerializeObject(this);
    }
}

public class EventTypeSizeProjection : Projection<EventTypeSizeState> {
    public EventTypeSizeProjection(EventTypeSizeState state) : base(state) {
        //TODO: Not sure how we init state yet
    }

    protected override async Task<EventTypeSizeState> HandleEvent(EventTypeSizeState state,
                                                                  ResolvedEvent @event) {

        if (!state.EventInfo.ContainsKey(@event.OriginalEvent.EventType)) {
            state.EventInfo.Add(@event.OriginalEvent.EventType, new EventInfo(@event.OriginalEvent.EventType));
        }

        //TODO: We are only counting Data.Length but should we look at including EventId / Event Type (or perhaps the whole ResolvedEvent)
        //Safely perform the update
        EventInfo e = state.EventInfo[@event.OriginalEvent.EventType];

        //Previous size calculation was ultimately flawed.
        //The routine below was picked out the ES code base
        //UInt64 newSize = e.SizeInBytes += (UInt64)@event.OriginalEvent.Data.Length;
        Int64 newSize =SizeOnDisk(@event.Event.EventType, @event.OriginalEvent.Data.ToArray(), @event.OriginalEvent.Metadata.ToArray());

        e.SizeInBytes += newSize;
        e.Count += 1;

        return await Task.FromResult(state);
    }

    public static Int32 SizeOnDisk(String eventType, Byte[] data, Byte[] metadata) =>
        data?.Length ?? 0 + metadata?.Length ?? 0 + eventType.Length * 2;
}