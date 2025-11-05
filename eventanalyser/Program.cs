namespace eventanalyser {
    using System;
    using System.Globalization;
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Configuration;
    using Projections;
    using KurrentDB.Client;

    /*
     * Checkpoints
     * Write state after <n> time has elapsed
     * System Events should be configurable

    Only do this once in ctor:
     Task t = this.DeleteOptions switch {

     * lookm at event type filtering on FromAll 
       e.g. var filter = new SubscriptionFilterOptions(
           EventTypeFilter.Prefix("Sale", "SalesTransaction")
            or 
           EventTypeFilter.Regex("^Sales?")
       );
     */

    public enum Mode {
        Catchup = 0,
        Persistent = 1
    }

    public class Program {
        static IProjection InitialiseProjection(Options options, 
                                             KurrentDB.Client.KurrentDBClient eventStoreClient,
                                             CancellationToken cancellationToken) {

            if (options.DeleteOptions != null) {
                DeleteState state = new();
                return new StreamRemovalProjection(options, 
                                                   eventStoreClient);
            }

            if (options.EventDateFilter.HasValue) {
                return new StartPositionFromDateProjection(options);
            }

            //TODO Remaining projections
            if (options.EventTypeSize != null) {
                return new EventTypeSizeProjection(options);
            }

            //TODO: Should we always load the state from file if present, or allow both (configuration)?
            //State state = await this.Projection.GetInitialState(true);

            return null;
        }

        static async Task Main(String[] args) {
            
            IConfigurationBuilder builder = new ConfigurationBuilder()
                                            .SetBasePath(Directory.GetCurrentDirectory())
                                            .AddJsonFile("appsettings.json", optional: false)
                                            .AddJsonFile("appsettings.development.json", optional: true);

            IConfiguration config = builder.Build();
            Options options = Program.GetOptions(config);

            KurrentDBClientSettings settings = KurrentDBClientSettings.Create(options.EventStoreConnectionString);
            KurrentDBClient eventStoreClient = new(settings); //Use this for deleting streams

            //TODO: DU for different projection config?
            //Options is too clunky and error prone. Hide this away from user.
            IProjection projection = InitialiseProjection(options,eventStoreClient,CancellationToken.None);

            ProjectionService projectionService = new(projection, 
                                                      eventStoreClient, 
                                                      options);

            //TODO: Result?
            await projectionService.Start(CancellationToken.None);

            WriteLineHelper.WriteInfo($"Projection finished");
            Console.ReadKey();
        }

        public static Options GetOptions(IConfiguration config) {
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

            if (config.GetSection("AppSettings:ReloadState").Value != null)
                ;
            {
                var reloadStateAsString = config.GetSection("AppSettings")["ReloadState"];
                Boolean.TryParse(reloadStateAsString, out Boolean reloadState);

                //TODO: Check if enabled
                options = options with {
                                           ReloadState = reloadState
                                       };
            }

            return options;
        }
    }
}
