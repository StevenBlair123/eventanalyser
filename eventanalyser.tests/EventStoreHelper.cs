namespace eventanalyser.tests;

using System.Text;
using KurrentDB.Client;

public class EventStoreHelper {
    private readonly KurrentDBClient EventStoreClient;

    public EventStoreHelper(KurrentDBClient eventStoreClient) {
        this.EventStoreClient = eventStoreClient;
    }

    public async Task<(DateTime SaleDateTime,String Stream)> WriteSaleEvent(DateTime dateTime,
                                               Guid organisationId,
                                               System.Boolean isOldEvent = false) {
        Guid salesTransactionId = Guid.NewGuid();
        String streamName = $"SalesTransactionAggregate-{salesTransactionId:N}";
        String @event = $@"{{
    ""organisationId"": ""{organisationId}"",
    ""stid"": ""{salesTransactionId}"",
    ""dt"": ""{dateTime:O}""
}}";

        String eventType = isOldEvent switch {
            true => "SalesTransactionStartedEvent",
            _ => "SaleStarted"
        };

        await this.WriteEvent(streamName, @event, eventType);

        return (dateTime,streamName);
    }

    public async Task WriteEvent(String stream, String @event, String eventType) {
        Guid eventId = Guid.NewGuid();

        IDictionary<String, String> metadata = new Dictionary<String, String>();

        metadata.Add("type", eventType);
        metadata.Add("created", "1");
        metadata.Add("content-type", "json");

        Byte[] bytes = Encoding.Default.GetBytes(@event);
        EventData eventData = new(Uuid.FromGuid(eventId), eventType, (ReadOnlyMemory<Byte>)bytes);

        await this.EventStoreClient.AppendToStreamAsync(stream,
                                                        KurrentDB.Client.StreamState.Any,
                                                        new List<EventData>() { eventData });
    }

    public async Task<Int32> GetEventCountFromStream(String stream,CancellationToken cancellationToken) {
        var x = DockerHelper.EventStoreClient.ReadStreamAsync(Direction.Backwards, stream, StreamPosition.End, 100);

        var t = await x.ReadState;

        if (t == ReadState.StreamNotFound) {
            return 0;
        }

        var g = await x.ToListAsync(cancellationToken);
            
        return g.Count;
    }

    public async Task<Int32> GetEventCountFromAll(CancellationToken cancellationToken) {
        SubscriptionFilterOptions filterOptions = new(EventTypeFilter.ExcludeSystemEvents());
        var subscription = this.EventStoreClient.SubscribeToAll(FromAll.Start,
                                                                resolveLinkTos: true,
                                                                filterOptions: filterOptions,
                                                                cancellationToken: cancellationToken);

        var messages = subscription.Messages;

        Int32 eventCount = 0;
        Boolean exit = false;

        await foreach (var message in messages.WithCancellation(cancellationToken))
        {
            switch (message)
            {
                case StreamMessage.Event:
                    eventCount++;
                    break;
                case StreamMessage.CaughtUp:
                case StreamMessage.AllStreamCheckpointReached:
                    exit = true;
                    break;
            }

            if (exit)
            {
                break;
            }
        }

        return eventCount;
    }
}