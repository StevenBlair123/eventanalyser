namespace eventanalyser.Projections;

using KurrentDB.Client;
using Newtonsoft.Json;

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
    public UInt64  SkippedEvents { get; init; }
    public DateTime LastEventDate { get; init; }
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
    public EventTypeSizeProjection(Options options) : base(options) {

    }
    protected override async Task<EventTypeSizeState> HandleEvent(EventTypeSizeState state,
                                                                  ResolvedEvent @event) {

        if (Options.EventDateFilter.HasValue) {
            if (@event.Event.Created.Date != Options.EventDateFilter.GetValueOrDefault().Date) {
                ulong skipped = state.SkippedEvents;
                var newstate = state with {
                    SkippedEvents = skipped + 1,
                    LastEventDate = @event.Event.Created
                };
                return newstate;
            }
        }

        var newState = state with {
            LastEventDate = @event.Event.Created
        };

        String eventTypeKey = @event.IsResolved switch {
            true => $"$>{@event.Event.EventType}",
            _ => @event.OriginalEvent.EventType
        };

        if (!newState.EventInfo.ContainsKey(eventTypeKey)) {
            newState.EventInfo.Add(eventTypeKey, new EventInfo(eventTypeKey));
        }

        //TODO: We are only counting Data.Length but should we look at including EventId / Event Type (or perhaps the whole ResolvedEvent)
        //Safely perform the update
        EventInfo e = state.EventInfo[eventTypeKey];

        //Previous size calculation was ultimately flawed.
        //The routine below was picked out the ES code base
        //UInt64 newSize = e.SizeInBytes += (UInt64)@event.OriginalEvent.Data.Length;
        Int64 newSize =SizeOnDisk(eventTypeKey, 
                                  @event.OriginalEvent.Data.ToArray(), 
                                  @event.OriginalEvent.Metadata.ToArray(), 
                                  @event.OriginalStreamId);

        e.SizeInBytes += newSize;
        e.Count += 1;

        return await Task.FromResult(newState);
    }

    public static Int32 SizeOnDisk(String eventType, Byte[] data, Byte[] metadata, String streamName) {
        Int32 length = 0;

        length += sizeof(UInt16); // Flags
        length += sizeof(Int64);  // TransactionPosition
        length += sizeof(Int32); // TransactionOffset
        length += 1;// header for stream name
        length += streamName.Length;
        length += 16; //EventId
        length += 16; //CorrelationId
        length += sizeof(Int64); //TimeStamp
        length += 1;// header for event type
        length += eventType.Length;
        length += sizeof(Int32); // data length header
        length += data.Length;
        length += sizeof(Int32); // metadata length header
        length += metadata.Length;

        return length;
    }


    //public static Int32 SizeOnDisk(String eventType, Byte[] data, Byte[] metadata) =>
    //    data?.Length ?? 0 + metadata?.Length ?? 0 + eventType.Length * 2;
}