namespace eventanalyser.Projections;

using Newtonsoft.Json;
using static DeleteOptions;
using KurrentDB.Client;

public record StreamState : State {
    public Dictionary<String, StreamInfo> StreamInfo { get; set; } = new();

    public List<(String, String)> Errors { get; set; } = new();

    public override String GetStateAsString() {
        Dictionary<String, StreamInfo> sortedEventInfo = this.StreamInfo.OrderByDescending(e => e.Value.SizeInBytes).ToDictionary(e => e.Key, e => e.Value);

        this.StreamInfo = sortedEventInfo;

        return JsonConvert.SerializeObject(this);
    }
}

public record DeleteState : State {
    public Int64 DeletedCount { get; set; }
    public List<(String, String)> Errors { get; set; } = new();
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

    public record DeleteStreamBefore : DeleteOptions
    {
        public DateTime DateTime { get; }
        public List<String> EventTypes { get; }

        public DeleteStreamBefore(DateTime dateTime, List<String> eventTypes) {
            this.DateTime = dateTime;
            EventTypes = eventTypes;
        }
    }

    public record SetStreamMaxEventCount : DeleteOptions
    {
        public int EventCountToKeep { get; }
        public List<String> EventTypes { get; }

        public SetStreamMaxEventCount(Int32 eventCountToKeep, List<String> eventTypes)
        {
            EventCountToKeep = eventCountToKeep;
            EventTypes = eventTypes;
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

public class StreamRemovalProjection : Projection<DeleteState> {
    private readonly DeleteOptions DeleteOptions;

    private readonly KurrentDBClient EventStoreClient;

    public StreamRemovalProjection(Options options,
                                   KurrentDBClient eventStoreClient) : base(options.ReloadState) {
        this.DeleteOptions = options.DeleteOptions;
        this.EventStoreClient = eventStoreClient;
    }

    public override String GetFormattedName() {
        return $"{this.GetType().Name}-{this.DeleteOptions.GetType().Name}";
    }

    protected override async Task<DeleteState> HandleEvent(DeleteState state,
                                                           ResolvedEvent @event) {
        if (Support.EventCanBeProcessed(@event) == false) {
            return await Task.FromResult(state);
        }

        String stream = @event.OriginalEvent.EventStreamId;
        DeleteState newState = state;
        Boolean deleteStream = false;
        String eventDateTime = null;
        DateTime date = DateTime.MinValue;
        String eventAsString = Support.ConvertResolvedEventToString(@event);

        try {
            //Check the Delete options for our conditions
            switch(this.DeleteOptions) {
                case DeleteOrganisation delOrg:

                    //TODO: Just search  for the guid?
                    if (!eventAsString.Contains(delOrg.OrganisationId.ToString())) {
                        return await Task.FromResult(state);
                    }
                    break;

                case DeleteSalesBefore db:

                    if (@event.Event.EventType != "SalesTransactionStartedEvent") {
                        return await Task.FromResult(state);
                    }

                    eventDateTime = Support.GetDateFromEvent(eventAsString);

                    if (String.IsNullOrEmpty(eventDateTime)) {
                        return await Task.FromResult(state);
                    }

                    date = DateTime.Parse(eventDateTime);

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
                case DeleteStreamBefore db:

                    if (db.EventTypes.Contains(@event.Event.EventType) == false) {
                        // We are not interested in this event type
                        return await Task.FromResult(state);
                    }

                    eventDateTime = Support.GetDateFromEvent(eventAsString);

                    if (String.IsNullOrEmpty(eventDateTime)) {
                        return await Task.FromResult(state);
                    }

                    date = DateTime.Parse(eventDateTime);

                    if (date == DateTime.MinValue || date >= db.DateTime) {
                        return await Task.FromResult(state);
                    }

                    break;
                case SetStreamMaxEventCount db:

                    if (db.EventTypes.Contains(@event.Event.EventType) == false)
                    {
                        // We are not interested in this event type
                        return await Task.FromResult(state);
                    }

                    break;
            }

            //TODO: Use the DeleteOptions to decide on the delete?

            //if (!newState.StreamInfo.ContainsKey(stream)) {
            //    deleteStream = true;
            //}

            //if (deleteStream) {
                void Log(String msg) => WriteLineHelper.WriteWarning($"{(this.DeleteOptions.SafeMode ? "***SAFE MODE*** " : String.Empty)}{msg}");

                if (this.DeleteOptions.SafeMode == false) {
                    Task t = this.DeleteOptions switch {
                        DeleteStreamBefore => this.EventStoreClient.DeleteAsync(stream,
                            KurrentDB.Client.StreamState.Any),
                        SetStreamMaxEventCount s => this.EventStoreClient.SetStreamMetadataAsync(stream,
                                                                                                 KurrentDB.Client.StreamState.Any,
                            new StreamMetadata(maxCount: s.EventCountToKeep),
                            null,
                            null,
                            null,
                            CancellationToken.None),
                        _ => throw new Exception("Unknown option atm")
                    };

                    await t;
                }

                switch (this.DeleteOptions) {
                    case DeleteStreamBefore:
                        Log($"Deleted stream: {stream}");
                        break;
                    case SetStreamMaxEventCount s:
                        Log($"stream: {stream} max events set to {s.EventCountToKeep}");
                        break;
                }
                newState = newState with {
                    DeletedCount = newState.DeletedCount + 1
                };

            return await Task.FromResult(newState);
        }
        catch(Exception ex) {
            state.Errors.Add(new(eventAsString, ex.Message));
        }

        return await Task.FromResult(state);
    }
}