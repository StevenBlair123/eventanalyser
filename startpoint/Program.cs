using EventStore.Client;

namespace startpoint;

using CommandLine;
using Microsoft.Extensions.Configuration;
using System.Threading;

class Program {
    static async Task Main(String[] args) {
        CancellationToken cancellationToken = new();
        IConfigurationBuilder builder = new ConfigurationBuilder()
                                        .SetBasePath(Directory.GetCurrentDirectory())
                                        .AddJsonFile("appsettings.json", optional: false);

        IConfiguration config = builder.Build();

        String? eventStoreConnectionString = config.GetSection("ConnectionStrings") switch {
            { } section when section.GetValue<String>("EventStoreConnection") != null
                => section.GetValue<String>("EventStoreConnection"),
            _ => "esdb://admin:changeit@127.0.0.1:2113?tls=false&tlsVerifyCert=false"
        };

        String? eventFilter = config.GetSection("AppSettings") switch {
            { } section when section.GetValue<String>("EventDateFilter") != null
                => section.GetValue<String>("EventDateFilter"),
            _ => DateTime.Now.ToString("yyyy-MM-dd")
        };


        //EventStoreClientSettings settings1 = EventStoreClientSettings.Create(eventStoreConnectionString);
        //EventStorePersistentSubscriptionsClient espsc = new(settings1);
        //PersistentSubscriptionSettings pss = new(true, startFrom: new Position(1884884647, 1884884647), maxRetryCount: 0);

        //await espsc.DeleteToAllAsync("Group1", cancellationToken:cancellationToken);

        ////Ensure created (try / catch)
        //try {
        //    await espsc.CreateToAllAsync("Group1", pss, null, null, cancellationToken);
        //}
        //catch(Exception e) {

        //}

        //await espsc.SubscribeToAllAsync("Group1",
        //                                EventAppeared, 
        //                                (subscription,
        //                                 reason,
        //                                 arg3) => Console.WriteLine($"Subscription dropped {reason}"),
        //                                cancellationToken:cancellationToken);

        //Console.ReadKey();

        DateTime eventFilerDate = eventFilter switch {
            null => DateTime.Now,
            "today" => DateTime.Now.Date,
            "yesterday" => DateTime.Now.Date.AddDays(-1),
            _ => DateTime.ParseExact(eventFilter, "yyyy-MM-dd", null)
        };

        Console.WriteLine($"Looking for first event on {eventFilerDate:yyyy-MM-dd}");

        EventStoreClientSettings settings = EventStoreClientSettings.Create(eventStoreConnectionString);
        EventStoreClient eventStoreClient = new(settings); //Use this for deleting streams

        // Get the start position for the date (if it has been set)
        Position? startPosition = await Program.GetDateStartPosition(eventStoreClient,
                                                                     eventFilerDate, 
                                                                     cancellationToken);

        String result = startPosition switch {
            null => $"No start position found for event date filter {eventFilerDate:yyyy-MM-dd}",
            _ => $"startPosition for EventDate {eventFilerDate} is {startPosition.Value.ToString()}"
        };

        Console.WriteLine(result);

        Console.ReadKey();
    }

    private static async Task EventAppeared(PersistentSubscription arg1,
                                      ResolvedEvent arg2,
                                      Int32? arg3,
                                      CancellationToken arg4) {

        try {
            if (arg2.IsResolved) {
                Console.WriteLine($"Event Read: {arg2.Event.EventType}: {arg2.Event.Created:yyyy-MM-dd}");
            }
            else {
                Console.WriteLine($"Event Read: {arg2.OriginalEvent.EventType}: {arg2.OriginalEvent.Created:yyyy-MM-dd}");
            }

            //Write to Kafka
            //Need to know if we want to write this event and if so, where
            //Make makes the event "eposity"

            //Event Has org Id || eventType for typemap - do we know about this event?
            //Write to $ce-Aggregate / $et

            //if category "projection" enabled
            String category = arg2.Event.EventStreamId.Split("-").First();

            //if eventype "projection" enabled
            String eventType = arg2.Event.EventType;

            //TODO: if we have a org id, org bucket (not store Id)
            //TODO: if we have a store id, store bucket
        } catch (Exception e) {
            Console.WriteLine(e);
        }

        // await Task.Delay(TimeSpan.FromMilliseconds(500));

        await arg1.Ack(arg2);
    }

    // TODO: this can return the number of events to process (only count the events from the target date)
    private static async Task<Position?> GetDateStartPosition(EventStoreClient client, DateTime targetDate,CancellationToken cancellationToken) {
        // Start reading from the end of the $all stream
        Position currentPosition = Position.End; // Starting from the end of the stream
        ResolvedEvent? firstEventOfDay = null;   // To track the first event found for the day
        Boolean keepReading = true;
        Int64 eventCountRead = 0;
        EventTypeFilter eventFilter = EventTypeFilter.ExcludeSystemEvents();

        while (keepReading) {
            // Read events with a filter applied
            var events = client.ReadAllAsync(
                                             Direction.Backwards,
                                             currentPosition,
                                             eventFilter,
                                             4096, // Adjust page size as needed
                                             resolveLinkTos: true, // Whether to resolve links to events
                                              null, // Optional: credentials if required
                                             null,
                                             cancellationToken
                                            );

            await foreach (var e in events) {
               // Console.WriteLine($"EventAppeared: {e.Event.EventType} - {e.Event.Created}");

                eventCountRead++;
                var eventDate = e.Event.Created;

                //Scenario 1 - we find target date
                //Scenario 2 - never find target date but events before it
                //Scenario 3 - never find target date but no events before it

                // Check if the event is on the target date
                if (eventDate.Date == targetDate.Date) {
                    firstEventOfDay = e; // Track the first event found for that day
                }

                // If we pass the target date (i.e., the event is earlier than the target date), stop reading
                if (eventDate.Date < targetDate.Date) {
                    // Update the current position to continue from the last event's position
                    currentPosition = e.OriginalPosition.GetValueOrDefault();

                    if(firstEventOfDay.HasValue == false) {
                        firstEventOfDay = e; // Track the first event found for that day
                    }

                    keepReading = false;  // Stop the outer loop
                    break;
                }
            }

            Console.WriteLine($"{DateTime.Now:HH:mm:ss.fff} - Read {eventCountRead} events");
        }

        // If a matching event was found, print the details
        if (firstEventOfDay.HasValue) {
            var matchedEvent = firstEventOfDay.Value;
            Position? result = firstEventOfDay.Value.OriginalPosition;
            Console.WriteLine($"First event on {firstEventOfDay.Value.Event.Created.ToShortDateString()} is {matchedEvent.Event.EventType} at {matchedEvent.Event.Created}");
            return result;
        }

        Console.WriteLine($"No event found on {targetDate.ToShortDateString()}");
        return null;
    }
}