namespace eventanalyser.Projections;

using EventStore.Client;
using Newtonsoft.Json;

public class EventInfo {
    public String Type { get; set; }
    public UInt64 Count { get; set; }
    public Double SizeInMegabytes => this.SizeInBytes / (1024.0 * 1024.0);
    public UInt64 SizeInBytes { get; set; }
    public Int32 AverageSizePerEventInBytes => this.Count > 0 ? (Int32)Math.Ceiling((Double)this.SizeInBytes / this.Count) : 0;

    public EventInfo(String type,
                     UInt64 count,
                     UInt64 sizeInBytes) {
        this.Count = count;
        this.Type = type;
        this.SizeInBytes += sizeInBytes;
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
            state.EventInfo.Add(@event.OriginalEvent.EventType, new EventInfo(@event.OriginalEvent.EventType, 0, 0));
        }

        //TODO: We are only counting Data.Length but should we look at including EventId / Event Type (or perhaps the whole ResolvedEvent)
        //Safely perform the update
        EventInfo e = state.EventInfo[@event.OriginalEvent.EventType];
        UInt64 newSize = e.SizeInBytes += (UInt64)@event.OriginalEvent.Data.Length;
        state.EventInfo[@event.OriginalEvent.EventType] = new EventInfo(@event.OriginalEvent.EventType, 
                                                                        ++e.Count,
                                                                        newSize);

        return await Task.FromResult(state);
    }
}