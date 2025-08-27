namespace eventanalyser.Projections;

using EventStore.Client;
using Newtonsoft.Json;
using System.Threading;
using static eventanalyser.Projections.DeleteOptions;

public record StreamState : State
{
    public Dictionary<String, StreamInfo> StreamInfo { get; set; } = new();

    public override String GetStateAsString() {
        Dictionary<String, StreamInfo> sortedEventInfo = this.StreamInfo.OrderByDescending(e => e.Value.SizeInBytes).ToDictionary(e => e.Key, e => e.Value);

        this.StreamInfo = sortedEventInfo;

        return JsonConvert.SerializeObject(this);
    }
}

public class StreamInfo {
    public String Name { get; set; }

    public UInt64 Count { get; set; }

    public Int64 SizeInBytes { get; set; }

    public StreamInfo(String name) {
        this.Name = name;
    }
}

public abstract record DeleteOptions {
    public Boolean SafeMode { get; init; }

    public DeleteOptions(Boolean safeMode=true) {
        this.SafeMode = safeMode;
    }

    public record DeleteSalesBefore : DeleteOptions {
        public DateTime DateTime { get; }

        public DeleteSalesBefore(DateTime dateTime) {
            this.DateTime = dateTime;
        }
    }

    public record DeleteOrganisation : DeleteOptions {
        public Guid OrganisationId { get; }

        public DeleteOrganisation(Guid organisationId) {
            this.OrganisationId = organisationId;
        }
    }

    public record DeleteStream : DeleteOptions {
        public String StreamName { get; }

        public DeleteStream(String streamName) {
            this.StreamName = streamName;
        }
    }
}

public class StreamRemovalProjection : Projection<StreamState> {
    private readonly DeleteOptions DeleteOptions;

    private readonly EventStoreClient EventStoreClient;

    public StreamRemovalProjection(StreamState state,
                                   DeleteOptions deleteOptions,
                                   EventStoreClient eventStoreClient) : base(state) {
        this.DeleteOptions = deleteOptions;
        this.EventStoreClient = eventStoreClient;
    }

    protected override async Task<StreamState> HandleEvent(StreamState state,
                                                           ResolvedEvent @event) {
        if (Support.EventCanBeProcessed(@event) == false) {
            return await Task.FromResult(state);
        }

        String stream = @event.OriginalEvent.EventStreamId;
        StreamState newState = state;
        Boolean deleteStream=false;
        String eventAsString = Support.ConvertResolvedEventToString(@event);

        //Check the Delete options for our conditions
        switch (this.DeleteOptions) {
            case DeleteOrganisation delOrg:
                var dyn = new {
                                  organisationId = Guid.Empty
                              };

                var result = JsonConvert.DeserializeAnonymousType(eventAsString, dyn);

                if (result == null || result.organisationId == Guid.Empty) {
                    return await Task.FromResult(state);
                }

                if (delOrg.OrganisationId != result.organisationId) {
                    return await Task.FromResult(state);
                }

                //if (!newState.StreamInfo.ContainsKey(stream)) {
                //    deleteStream = true;
                //}


                //if stream already exists, it means we have already truncated.

                //if (deleteStream) {
                //    void Log(String msg) => Console.WriteLine($"{(this.DeleteOptions.SafeMode ? "***SAFE MODE*** " : String.Empty)}{msg}");

                //    if (this.DeleteOptions.SafeMode == false) {
                //        await this.EventStoreClient.DeleteAsync(stream, EventStore.Client.StreamState.Any);
                //    }

                //    Log($"Deleted stream: {stream}");

                //    newState.StreamInfo.Add(stream, new StreamInfo(stream));
                //}

                break;

            case DeleteSalesBefore db:
                String eventDateTime = Support.GetDateFromEvent(eventAsString);

                if (String.IsNullOrEmpty(eventDateTime)) {
                    return await Task.FromResult(state);
                }

                DateTime date = DateTime.Parse(eventDateTime);

                if (date == DateTime.MinValue || date >= db.DateTime) {
                    return await Task.FromResult(state);
                }

                //TODO: Where are we checking the tb
                //newState.StreamInfo.Add(stream, new StreamInfo(stream));
                //We cannot blindly delete a stream with an older event.
                //This has to be linked to specific types (Sales for example)
                //The risk would be nuking product / org streams because of an old created event

                //StreamMetadata streamMetaData = new(null, null,@event.Event.Position.CommitPosition);

                //if (this.DeleteOptions.SafeMode == false) {
                //    await EventStoreClient.DeleteAsync(stream,EventStore.Client.StreamState.Any);

                //    //TODO: TRUNCATE BEFORE
                //    //await this.EventStoreClient.SetStreamMetadataAsync(stream,
                //    //                                                   EventStore.Client.StreamState.Any,
                //    //                                                   streamMetaData,
                //    //                                                   null,
                //    //                                                   null,
                //    //                                                   null,
                //    //                                                   CancellationToken.None);
                //}

                break;
        }

        //TODO: Use the DeleteOptions to decide on the delete?

        if (!newState.StreamInfo.ContainsKey(stream)) {
            deleteStream = true;
        }

        if (deleteStream) {
            void Log(String msg) => Console.WriteLine($"{(this.DeleteOptions.SafeMode ? "***SAFE MODE*** " : String.Empty)}{msg}");

            if (this.DeleteOptions.SafeMode == false) {
                await this.EventStoreClient.DeleteAsync(stream, EventStore.Client.StreamState.Any);
            }

            Log($"Deleted stream: {stream}");

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