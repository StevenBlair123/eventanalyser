namespace eventanalyser {
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using CommandLine;
    using EventStore.Client;
    using Microsoft.Extensions.Configuration;
    using Projections;

    // TODO: number event remaining
    // TODO: break out at end of the target date
    
    public enum Mode {
        Catchup = 0,
        Persistent = 1
    }

    public static class WriteLineHelper {
        public static void WriteWarning(String msg) {
            ConsoleColor foreground = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(msg);
            Console.ForegroundColor = foreground;
        }
    }

    public class CommandLineArgs
    {
        [Option('d', "date", Required = false, HelpText = "Set the date of run (today,yesterday, explicit date (yyyy-MM-dd)")]
        public String DateOption { get; set; }

        [Option('s', "startposition", Required = false, HelpText = "Set the date of run (today,yesterday, explicit date (yyyy-MM-dd)")]
        public Int32? StartPosition { get; set; }
    }

    class Program {
        static async Task Main(String[] args) {
            
            IConfigurationBuilder builder = new ConfigurationBuilder()
                                            .SetBasePath(Directory.GetCurrentDirectory())
                                            .AddJsonFile("appsettings.json", optional: false);

            IConfiguration config = builder.Build();
            Options options = Program.GetOptions(config);

            Parser.Default.ParseArguments<CommandLineArgs>(args)
                .WithParsed<CommandLineArgs>(o =>
                {
                    Console.WriteLine(o.DateOption);
                    Console.WriteLine(o.StartPosition);

                    options = options with {
                        EventDateFilter = o.DateOption switch {
                            //null => new DateTime(2024,10,9),//DateTime.MinValue,
                            null => null,
                            "" => null,
                            "today" => DateTime.Now.Date,
                            "yesterday" => DateTime.Now.Date.AddDays(-1),
                            _ => DateTime.ParseExact(o.DateOption, "yyyy-MM-dd", null)
                        },
                    };
                });
            
            EventStoreClientSettings settings = EventStoreClientSettings.Create(options.EventStoreConnectionString);
            EventStoreClient eventStoreClient = new(settings); //Use this for deleting streams
            
            // Get the start position for the date (if it has been set)
            Position? startPosition = await GetDateStartPosition(eventStoreClient, options.EventDateFilter);

            //TODO: Load method which will pickup checkpoint / state
            EventTypeSizeState state = new();
            EventTypeSizeProjection projection = new(state, options);

            Console.WriteLine("Starting projection EventTypeSizeProjection");

            //TODO: This will need hooked up in SubscribeToAllAsync
            //FromStream fs = FromStream.After(new((UInt64)projection.Position));

            //FromAll.After()

            //I think we need to include system events to determine if these are causing us size problems.
            // SubscriptionFilterOptions filterOptions = new(EventTypeFilter.ExcludeSystemEvents());
            SubscriptionFilterOptions filterOptions = new(EventTypeFilter.None);

            CancellationToken cancellationToken = new();

            //TODO: Improve this (signal?)
            while (true) {
                try {
                    FromAll fromAll = startPosition switch {
                        null => FromAll.Start,
                        _ => FromAll.After(startPosition.Value)
                    };

                    if (fromAll != FromAll.Start) {
                        var g = fromAll.ToUInt64();
                        Console.WriteLine(g.commitPosition);
                        Console.WriteLine(g.preparePosition);
                    }
                    
                    await using var subscription = eventStoreClient.SubscribeToAll(fromAll, filterOptions: filterOptions,
                                                                                   cancellationToken: cancellationToken);

                    await foreach (var message in subscription.Messages.WithCancellation(cancellationToken)) {
                        switch (message) {
                            case StreamMessage.Event(var @event):
                                Console.WriteLine($"In handle {@event.Event.EventType}");
                                state = await projection.Handle(@event);
                                break;

                            case StreamMessage.CaughtUp:
                                Console.WriteLine("Caught Up");
                                break;
                        }

                        if (message is StreamMessage.Event || message is StreamMessage.CaughtUp) {
                            if (state.Count % options.CheckPointCount == 0 || message is StreamMessage.CaughtUp) {
                                await Program.SaveState(state);
                            }
                        }

                        //TODO: Dump final state to console as well?
                    }
                }
                catch(Exception e) {
                    Console.WriteLine(e);
                }
            }

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

            Console.WriteLine($"Setting checkpoint size to {checkpointCount}");

            Mode mode = Mode.Catchup;

            Options options = new(evenStoreConnectionString, "") {
                                                                     Mode = mode,
                                                                     CheckPointCount = checkpointCount
                                                                 };

            return options;
        }

        private static async Task SaveState<TState>(TState state) where TState : State {
            Console.WriteLine($"{DateTime.Now:HH:mm:ss.fff} Events process {state.Count}");

            //TODO: Log state to screen / file?
            String json = state.GetStateAsString();

            String exeDirectory = AppContext.BaseDirectory;
            String filePath = Path.Combine(exeDirectory, "state.txt");

            await File.WriteAllTextAsync(filePath, json);
        }

        private static void SubscriptionDropped(StreamSubscription arg1,
                                                SubscriptionDroppedReason arg2,
                                                Exception? arg3) {
            WriteLineHelper.WriteWarning($"{arg2.ToString()}");
        }
    }
}
