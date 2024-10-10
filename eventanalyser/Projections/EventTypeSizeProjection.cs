using System.Runtime.Serialization.Formatters.Binary;

namespace eventanalyser.Projections;

using EventStore.Client;
using Newtonsoft.Json;

public class EventInfo {
    public ulong SizeInBytesJson { get; set; }
    public String Type { get; set; }
    public UInt64 Count { get; set; }
    public Double SizeInMegabytes => this.SizeInBytes / (1024.0 * 1024.0);
    public UInt64 SizeInBytes { get; set; }
    public Int32 AverageSizePerEventInBytes => this.Count > 0 ? (Int32)Math.Ceiling((Double)this.SizeInBytes / this.Count) : 0;

    public EventInfo(String type,
                     UInt64 count,
                     UInt64 sizeInBytes,
                     UInt64 sizeInBytesJson) {
        SizeInBytesJson = sizeInBytesJson;
        this.Count = count;
        this.Type = type;
        this.SizeInBytes += sizeInBytes;
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
    private readonly Options Options;

    public EventTypeSizeProjection(EventTypeSizeState state, Options options) : base(state) {
        Options = options;
        //TODO: Not sure how we init state yet
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
        if (!newState.EventInfo.ContainsKey(@event.OriginalEvent.EventType)) {
            newState.EventInfo.Add(@event.OriginalEvent.EventType, new EventInfo(@event.OriginalEvent.EventType, 0, 0, 0));
        }

        //TODO: We are only counting Data.Length but should we look at including EventId / Event Type (or perhaps the whole ResolvedEvent)
        //Safely perform the update
        EventInfo e = state.EventInfo[@event.OriginalEvent.EventType];
        //UInt64 newSize = e.SizeInBytes += (UInt64)@event.OriginalEvent.Data.Length;
        //ulong newSizeJson = e.SizeInBytes += (ulong)JsonConvert.SerializeObject(@event).Length;
        //var jsonString = JsonConvert.SerializeObject(@event);
        //ulong newSizeJson  = (ulong)System.Text.Encoding.UTF8.GetByteCount(jsonString);


        newState.EventInfo[@event.OriginalEvent.EventType] = new EventInfo(@event.OriginalEvent.EventType, 
                                                                        ++e.Count,
                                                                        newSize,
                                                                        newSizeJson);

        return await Task.FromResult(newState);
    }

    public static int SizeOnDisk(string eventType, byte[] data, byte[] metadata) =>
        data?.Length ?? 0 + metadata?.Length ?? 0 + eventType.Length * 2;

}