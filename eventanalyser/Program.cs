namespace eventanalyser {
    using System;
    using System.Globalization;
    using System.IO;
    using System.Reflection.Metadata.Ecma335;
    using System.Threading.Tasks;
    using EventStore.Client;
    using Microsoft.Extensions.Configuration;
    using Newtonsoft.Json;
    using Projections;
    using StreamState = Projections.StreamState;

    /*
     * Start point should indicate what date it is and the Position?
     * Checkpoints
     * Write state after <n> time has elapsed
     * Main should have an entry point for unit testing so we cna test the evenstote conenction code!
     * Start Position code - is that working?
     *
     */

    public enum Mode {
        Catchup = 0,
        Persistent = 1
    }

    class Program {

        static IProjection InitialProjection(Options options,EventStoreClient eventStoreClient) {

            if (options.DeleteOptions != null) {
                StreamState state = new();
                return new StreamRemovalProjection(state, options.DeleteOptions, eventStoreClient);
            }

            if (options.EventDateFilter.HasValue) {
                StartPositionFromDateState state = new();
                return new StartPositionFromDateProjection(state, options);
            }

            //TODO Remaining projections
            if (options.EventTypeSize != null) {
                EventTypeSizeState state = new();
                return new EventTypeSizeProjection(state, options);
            }

            return null;
        }

        static async Task Main(String[] args) {
            
            IConfigurationBuilder builder = new ConfigurationBuilder()
                                            .SetBasePath(Directory.GetCurrentDirectory())
                                            .AddJsonFile("appsettings.json", optional: false)
                                            .AddJsonFile("appsettings.development.json", optional: true);

            IConfiguration config = builder.Build();
            Options options = Program.GetOptions(config);
            
            EventStoreClientSettings settings = EventStoreClientSettings.Create(options.EventStoreConnectionString);
            EventStoreClient eventStoreClient = new(settings); //Use this for deleting streams

            //TODO: DU for different projection config?
            //Options is too clunky and error prone. Hide this away from user.
            IProjection projection = InitialProjection(options,eventStoreClient);

            WriteLineHelper.WriteInfo($"Starting projection {projection.GetType().Name}");
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
                        EventStoreClient.StreamSubscriptionResult subscription = eventStoreClient.SubscribeToAll(fromAll, resolveLinkTos: true,filterOptions: filterOptions,
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
                                if (@event.Event == null) 
                                    continue;

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
                                await Program.SaveState(state,projection);
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

        static Options GetOptions(IConfiguration config) {
            String evenStoreConnectionString = config.GetSection("ConnectionStrings")["EventStoreConnection"];

            Int32.TryParse(config.GetSection("AppSettings")["CheckpointCount"], out Int32 checkpointCount);

            if (checkpointCount == 0) {
                Console.WriteLine("Default checkpoint");
                checkpointCount = 1000;
            }

            String startPositionAsString = config.GetSection("AppSettings")["StartPosition"];



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

                if (config.GetSection("AppSettings:DeleteOptions")["Type"] == "DeleteStreamBefore")
                {
                    String dateTimeAsString = config.GetSection("AppSettings:DeleteOptions")["BeforeDateTime"];

                    DateTime beforeDateTime = DateTime.Parse(dateTimeAsString);
                    
                    var eventTypes = config.GetSection("AppSettings:DeleteOptions")["EventTypes"];
                    List<String> eventTypeList = eventTypes.Split(',').ToList();

                    Console.WriteLine($"BeforeDateTime: {beforeDateTime}");
                    Console.WriteLine($"EventTypes: {eventTypes}");

                    deleteOptions = new DeleteOptions.DeleteStreamBefore(beforeDateTime, eventTypeList)
                    {
                        SafeMode = safeMode
                    };
                }

                if (config.GetSection("AppSettings:DeleteOptions")["Type"] == "SetStreamMaxEventCount")
                {
                    var eventTypes = config.GetSection("AppSettings:DeleteOptions")["EventTypes"];
                    List<String> eventTypeList = eventTypes.Split(',').ToList();

                    var maxEventCount = config.GetSection("AppSettings:DeleteOptions")["MaxEventCount"];
                    if (String.IsNullOrEmpty(maxEventCount))
                        throw new Exception("Invalid max event count, no value has been specified");
                    Int32 eventCountToKeep = Int32.Parse(maxEventCount);
                    if (eventCountToKeep<= 0)
                        throw new Exception("Invalid max event count, must be greater than zero");

                    Console.WriteLine($"EventTypes: {eventTypes}");

                    deleteOptions = new DeleteOptions.SetStreamMaxEventCount(eventCountToKeep, eventTypeList)
                    {
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

            //EventDateFilter
            if (config.GetSection("AppSettings")["EventDateFilter"] != null) {


                var eventDateFilterAsString = config.GetSection("AppSettings")["EventDateFilter"];
                DateTime.TryParse(eventDateFilterAsString, out DateTime eventDateFilter);

                options = options with {
                                           EventDateFilter = eventDateFilter
                                       };
            }





            //EventTypeSize
            if ( config.GetSection("AppSettings:EventTypeSize").Value != null);
            {
                //TODO: Check if enabled
                options = options with {
                                           EventTypeSize = new EventTypeSize(true)
                                       };

            }



            return options;
        }

        private static async Task SaveState<TState>(TState state,IProjection projection) where TState : State {
            //TODO: Log state to screen / file?
            String json = state.GetStateAsString();

            String exeDirectory = AppContext.BaseDirectory;
            String filePath = Path.Combine(exeDirectory, $"{projection.GetFormattedName()}.txt");

            await File.WriteAllTextAsync(filePath, json);
        }

        private static void SubscriptionDropped(StreamSubscription arg1,
                                                SubscriptionDroppedReason arg2,
                                                Exception? arg3) {
            WriteLineHelper.WriteWarning($"{arg2.ToString()}");
        }
    }
}
