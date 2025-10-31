namespace eventanalyser {
    using System;
    using System.Globalization;
    using System.IO;
    using System.Threading.Tasks;
    using CommandLine;
    using EventStore.Client;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.Options;
    using Projections;
    using StreamState = Projections.StreamState;

    /*
     * Support multiple projections
     * Start point should indicate what date it is and the Position?
     * Should start point be a projection? (Date and last date changed Position)
     * Different appsettings for projections?
     * Checkpoints
     * dev settings
     * Write state after <n> time has elapsed
     *
     */

    public enum Mode {
        Catchup = 0,
        Persistent = 1
    }

    class Program {

        static IProjection InitialProjection(Options options,EventStoreClient eventStoreClient) {
        
            //State? state = null;

            //TODO Remaining projections
            if (false) {
                EventTypeSizeState state = new();
                return new EventTypeSizeProjection(state, options);
            }

            if (true) {
                StartPositionFromDateState state = new();
                return new StartPositionFromDateProjection(state, options);
            }

            if (options.DeleteOptions != null) {
                StreamState state = new StreamState();
                return new StreamRemovalProjection(state, options.DeleteOptions, eventStoreClient);
            }


            //State state = new EventTypeSizeState();
            //EventTypeSizeProjection projection = new((EventTypeSizeState)state, options);

            //IProjection pp = "test2" switch {
            //    "test1" => new StartPositionFromDateProjection((StartPositionFromDateState)state, options),
            //    "test2" => new EventTypeSizeProjection((EventTypeSizeState)state, options),
            //    "test3" => new StreamRemovalProjection((StreamState)state, null, eventStoreClient),
            //};
        }

        static async Task Main(String[] args) {
            
            IConfigurationBuilder builder = new ConfigurationBuilder()
                                            .SetBasePath(Directory.GetCurrentDirectory())
                                            .AddJsonFile("appsettings.json", optional: false);

            IConfiguration config = builder.Build();
            Options options = Program.GetOptions(config);
            
            EventStoreClientSettings settings = EventStoreClientSettings.Create(options.EventStoreConnectionString);
            EventStoreClient eventStoreClient = new(settings); //Use this for deleting streams

            //TODO: DU for different projection config?
            //Options is too clunky and error prone. Hide this away from user.

            //State state = new EventTypeSizeState();
            //EventTypeSizeProjection projection = new((EventTypeSizeState)state, options);

            //IProjection pp = "test2" switch {
            //    "test1" => new StartPositionFromDateProjection((StartPositionFromDateState)state, options),
            //    "test2" => new EventTypeSizeProjection((EventTypeSizeState)state, options),
            //    "test3" => new StreamRemovalProjection((StreamState)state,null,eventStoreClient),
            //};

            IProjection projection = InitialProjection(options,eventStoreClient);

            //if (true) {
            //    var state1 = await projection.Handle(default);
            //}


            // Get the start position for the date (if it has been set)
            //Position? startPosition = await GetDateStartPosition(eventStoreClient, options.EventDateFilter);

            //TODO: Load method which will pickup checkpoint / state

            //OrganisationState state = new(options.OrganisationId);
            //OrganisationRemovalProjection projection = new(state,eventStoreClient);

            //eventanalyser.Projections.StreamState state = new ();

            //StreamRemovalProjection projection = new(state, options.DeleteOptions, eventStoreClient);

            WriteLineHelper.WriteInfo($"Starting projection {projection.GetType().Name}");

            //TODO: This will need hooked up in SubscribeToAllAsync
            //FromStream fs = FromStream.After(new((UInt64)projection.Position));

            //FromAll.After()

            //I think we need to include system events to determine if these are causing us size problems.
            // SubscriptionFilterOptions filterOptions = new(EventTypeFilter.ExcludeSystemEvents());
            SubscriptionFilterOptions filterOptions = new(EventTypeFilter.None);

            CancellationToken cancellationToken = CancellationToken.None;
            State state = null;

            //TODO: Improve this (signal?)
            while (true) {
                try {
                    FromAll fromAll = options.StartFromPosition switch {
                        _ when options.StartFromPosition.HasValue => FromAll.After( new Position(options.StartFromPosition.Value, options.StartFromPosition.Value)),
                        _ => FromAll.Start
                        //_ => FromAll.After(startPosition.GetValueOrDefault())
                    };

                    if (fromAll != FromAll.Start) {
                        var g = fromAll.ToUInt64();
                        Console.WriteLine($"Commit: {g.commitPosition}");
                        Console.WriteLine($"Prepare: {g.preparePosition}");
                    }

                    Console.WriteLine($"Press return to start...");
                    Console.ReadKey();

                    IAsyncEnumerable<StreamMessage>? messages = null;

                    if (projection is StartPositionFromDateProjection) {
                        EventStoreClient.ReadAllStreamResult events = eventStoreClient.ReadAllAsync(
                                                                                                    Direction.Backwards,
                                                                                                    Position.End,
                                                                                                    EventTypeFilter.ExcludeSystemEvents(),
                                                                                                    4096, // Adjust page size as needed
                                                                                                    resolveLinkTos: true, // Whether to resolve links to events
                                                                                                    null, // Optional: credentials if required
                                                                                                    null,
                                                                                                    cancellationToken
                                                                                                   );

                        messages = events.Messages;


                    }
                    else {
                        await using EventStoreClient.StreamSubscriptionResult subscription = eventStoreClient.SubscribeToAll(fromAll, filterOptions: filterOptions,
                            cancellationToken: cancellationToken);

                        messages = subscription.Messages;
                    }


                        //await using EventStoreClient.StreamSubscriptionResult subscription = eventStoreClient.SubscribeToAll(fromAll, filterOptions: filterOptions,
                        //                                                                                    cancellationToken: cancellationToken);

                    //var ss = options.DeleteOptions switch {
                    //    DeleteOrganisation => eventStoreClient.SubscribeToAll(fromAll, filterOptions:filterOptions, cancellationToken:cancellationToken),

                    //    DeleteSalesBefore => eventStoreClient.SubscribeToAll(fromAll, filterOptions:filterOptions, cancellationToken:cancellationToken),

                    //    //DeleteSalesBefore => eventStoreClient.SubscribeToStream("$ce-SalesTransactionAggregate", FromStream.Start,true),

                    //    _ => throw new InvalidOperationException("Unsupported delete option")
                    //};

                    //await using var subscription = ss;

                    await foreach (var message in messages.WithCancellation(cancellationToken)) {
                        switch (message) {
                            case StreamMessage.Event(var @event):
                                Console.WriteLine($"In handle {@event.Event.EventType}");
                                state = await projection.Handle(@event);
                                break;

                            case StreamMessage.CaughtUp:
                                Console.WriteLine("Caught Up");
                                break;

                            case StreamMessage.LastAllStreamPosition:
                                Console.WriteLine("FirstStream Position");
                                break;
                        }

                        if (message is StreamMessage.Event || message is StreamMessage.CaughtUp) {
                            if (state.Count % options.CheckPointCount == 0 || message is StreamMessage.CaughtUp || state.ForceStateSave) {

                                if (message is StreamMessage.Event sm) {

                                    state = state with {
                                                           LastPosition = sm.ResolvedEvent.OriginalPosition.Value.CommitPosition,
                                                           ForceStateSave = false
                                                       };
                                }

                                //Console.WriteLine($"{DateTime.Now:HH:mm:ss.fff} Events process {state.Count} Last Date {state.LastEventDate}");
                                await Program.SaveState(state);
                            }
                        }



                        //TODO: Dump final state to console as well?
                    }

                    if (state.FinishProjection) {
                        WriteLineHelper.WriteWarning($"Signalled to finish Projection");
                        break;
                    }
                }
                catch(Exception e) {
                    Console.WriteLine(e);
                }
            }

            WriteLineHelper.WriteInfo($"Projection finished");
            Console.ReadKey();
        }

        // TODO: this can return the number of events to process (only count the events from the target date)
        private static async Task<Position?> GetDateStartPosition(EventStoreClient client, DateTime? targetDate) {

            if (targetDate.HasValue == false)
                return null;

            // Start reading from the end of the $all stream
            Position currentPosition = Position.End; // Starting from the end of the stream
            ResolvedEvent? firstEventOfDay = null;   // To track the first event found for the day
            Position? result = null;
            bool keepReading = true;
            Int64 eventCountRead = 0;
            while (keepReading)
            {
                Console.WriteLine($"{DateTime.Now:HH:mm:ss.fff} - Read {eventCountRead} events");
                eventCountRead += 4096;
                // Read the events in reverse from the current position
                var events = client.ReadAllAsync(Direction.Backwards, currentPosition, 4096); // Adjust page size as needed

                await foreach (var e in events)
                {
                    var eventDate = e.Event.Created;

                    // Check if the event is on the target date
                    if (eventDate.Date == targetDate.Value.Date)
                    {
                        firstEventOfDay = e; // Track the first event found for that day
                    }

                    // If we pass the target date (i.e., the event is earlier than the target date), stop reading
                    if (eventDate.Date < targetDate.Value.Date)
                    {
                        keepReading = false;  // Stop the outer loop
                        break;
                    }

                    // Update the current position to continue from the last event's position
                    currentPosition = e.OriginalPosition.GetValueOrDefault();
                }
            }

            // If a matching event was found, print the details
            if (firstEventOfDay.HasValue)
            {
                var matchedEvent = firstEventOfDay.Value;
                result = firstEventOfDay.Value.OriginalPosition;
                Console.WriteLine($"First event on {targetDate.Value.ToShortDateString()} is {matchedEvent.Event.EventType} at {matchedEvent.Event.Created}");
                return result;
            }
            Console.WriteLine($"No event found on {targetDate.Value.ToShortDateString()}");
            return null;
        }

        static Options GetOptions(IConfiguration config) {
            String evenStoreConnectionString = config.GetSection("ConnectionStrings")["EventStoreConnection"];

            Int32.TryParse(config.GetSection("AppSettings")["CheckpointCount"], out Int32 checkpointCount);

            if (checkpointCount == 0) {
                Console.WriteLine("Default checkpoint");
                checkpointCount = 1000;
            }

            String startPositionAsString = config.GetSection("AppSettings")["StartPosition"];

            //EventDateFilter
            var eventDateFilterAsString = config.GetSection("AppSettings")["EventDateFilter"];
            DateTime eventDateFilter = DateTime.Parse(eventDateFilterAsString);
            

            UInt64 startPosition;
            Boolean success = UInt64.TryParse(startPositionAsString, NumberStyles.Any,
                                              CultureInfo.InvariantCulture, out startPosition);

            UInt64? startPositionNullable = success ? startPosition : (ulong?)null;

            Console.WriteLine($"Setting checkpoint size to {checkpointCount}");
            DeleteOptions deleteOptions=null;

            if (config.GetSection("AppSettings:DeleteOptions") != null) {
                Boolean safeMode = true;

                if (!Boolean.TryParse(config.GetSection("AppSettings:DeleteOptions")["SafeMode"], out safeMode)) {
                    safeMode = true;
                }

                if (config.GetSection("AppSettings:DeleteOptions")["Type"] == "DeleteOrganisation") {
                    Guid organisationId = Guid.Parse(config.GetSection("AppSettings:DeleteOptions")["OrganisationId"]);


                    Console.WriteLine($"OrganisationId: {organisationId}");
                    deleteOptions = new DeleteOptions.DeleteOrganisation(organisationId) {
                                                                                             SafeMode = safeMode
                                                                                         };
                }

                if (config.GetSection("AppSettings:DeleteOptions")["Type"] == "DeleteSalesBefore") {
                    String dateTimeAsString = config.GetSection("AppSettings:DeleteOptions")["BeforeDateTime"];

                    DateTime beforeDateTime = DateTime.Parse(dateTimeAsString);


                    Console.WriteLine($"BeforeDateTime: {beforeDateTime}");
                    deleteOptions = new DeleteOptions.DeleteSalesBefore(beforeDateTime) {
                                                                                       SafeMode = safeMode
                                                                                   };
                }
            }

            Mode mode = Mode.Catchup;

            Options options = new(evenStoreConnectionString, "") {
                                                                     Mode = mode,
                                                                     CheckPointCount = checkpointCount,
                                                                     DeleteOptions = deleteOptions,
                                                                     StartFromPosition = startPositionNullable
                                                                 };

            options = options with {
                                       EventDateFilter = eventDateFilter
                                   };

            return options;
        }

        private static async Task SaveState<TState>(TState state) where TState : State {
            //TODO: Log state to screen / file?
            String json = state.GetStateAsString();

            String exeDirectory = AppContext.BaseDirectory;
            String filePath = Path.Combine(exeDirectory, $"{state.GetType().Name}.txt");

            await File.WriteAllTextAsync(filePath, json);
        }

        private static void SubscriptionDropped(StreamSubscription arg1,
                                                SubscriptionDroppedReason arg2,
                                                Exception? arg3) {
            WriteLineHelper.WriteWarning($"{arg2.ToString()}");
        }
    }
}
