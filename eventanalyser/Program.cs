namespace eventanalyser {
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using EventStore.Client;
    using Microsoft.Extensions.Configuration;
    using Projections;

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

    class Program {
        static async Task Main(String[] args) {
            IConfigurationBuilder builder = new ConfigurationBuilder()
                                            .SetBasePath(Directory.GetCurrentDirectory())
                                            .AddJsonFile("appsettings.json", optional: false);

            IConfiguration config = builder.Build();
            Options options = Program.GetOptions(config);

            EventStoreClientSettings settings = EventStoreClientSettings.Create(options.EventStoreConnectionString);
            EventStoreClient eventStoreClient = new(settings); //Use this for deleting streams

            //TODO: Load method which will pickup checkpoint / state
            EventTypeSizeState state = new();
            EventTypeSizeProjection projection = new(state);

            Console.WriteLine("Starting projection EventTypeSizeProjection");

            //TODO: This will need hooked up in SubscribeToAllAsync
            FromStream fs = FromStream.After(new((UInt64)projection.Position));

            //FromAll.After()

            //I think we need to include system events to determine if these are causing us size problems.
            // SubscriptionFilterOptions filterOptions = new(EventTypeFilter.ExcludeSystemEvents());
            SubscriptionFilterOptions filterOptions = new(EventTypeFilter.None);

            CancellationToken cancellationToken = new();

            await using var subscription = eventStoreClient.SubscribeToAll(FromAll.Start, filterOptions: filterOptions,
                                                                           cancellationToken: cancellationToken);

            await foreach (var message in subscription.Messages.WithCancellation(cancellationToken)) {
                switch (message) {
                    case StreamMessage.Event(var @event):
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

            Console.ReadKey();
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
