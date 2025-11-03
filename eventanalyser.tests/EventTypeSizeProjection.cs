using eventanalyser.Projections;

namespace eventanalyser.tests {
    using Shouldly;
    using System;
    using System.Text;
    using System.Threading;
    using EventStore.Client;
    using Microsoft.Extensions.Configuration;
    using static eventanalyser.Projections.DeleteOptions;
    using StreamState = Projections.StreamState;
    using String = System.String;

    public class ProjectionTests {
        private EventStoreHelper EventStoreHelper;

        [SetUp]
        public void Setup() {
            this.EventStoreHelper = new EventStoreHelper(DockerHelper.EventStoreClient);


        }

        [Test]
        public async Task safe_mode_is_enabled_by_default() {
            StreamState streamState = new();
            Guid organisationId = Guid.NewGuid();
            String stream = $"TestStream_{Guid.NewGuid():N}";

            eventanalyser.Options options = new(DockerHelper.EventStoreClient.ConnectionName, "") {
                                                                                                      EventTypeSize = new EventTypeSize(true),

                                                                                                      ByPassReadKeyToStart = true
                                                                                                  };

            options = options with {
                                       DeleteOptions = new DeleteOrganisation(Guid.NewGuid()),
                                       
                                   };

            StreamRemovalProjection projection = new(streamState, options.DeleteOptions, DockerHelper.EventStoreClient);
            ProjectionService projectionService = new(projection, DockerHelper.EventStoreClient, options);

            String @event = $@"{{
  ""organisationId"": ""{organisationId}""
}}";

            await EventStoreHelper.WriteEvent(stream, @event, "event1");

            await projectionService.Start(CancellationToken.None);

            Int32 eventCount = await EventStoreHelper.GetEventCountFromStream(stream,CancellationToken.None);

            eventCount.ShouldBe(1);
        }

        //[Test]
        public async Task stream_meta_data_set_to_max_eventCount() {
            StreamState streamState = new();
            Guid organisationId = Guid.NewGuid();
            SetStreamMaxEventCount deleteOptions = new(2, ["testEvent"]);
            String stream = $"TestStream_{Guid.NewGuid():N}";

            eventanalyser.Options options = new(DockerHelper.EventStoreClient.ConnectionName, "") {
                                                                                                      ByPassReadKeyToStart = true
                                                                                                  };

            options = options with {
                                       DeleteOptions = deleteOptions,

                                   };
            deleteOptions = deleteOptions with {
                SafeMode = false
            };

            String @event = $@"{{
  ""organisationId"": ""{organisationId}""
}}";

            // Write some events to a stream
            for (Int32 i = 0; i < 10; i++) {
                await this.EventStoreHelper.WriteEvent(stream, @event, "testEvent");
            }

            IProjection projection = new StreamRemovalProjection(streamState,
                                                                             deleteOptions,
                                                                             DockerHelper.EventStoreClient);
            ProjectionService projectionService = new(projection, 
                                                      DockerHelper.EventStoreClient, 
                                                      options);

            var count = await EventStoreHelper.GetEventCountFromStream(stream,CancellationToken.None);
            count.ShouldBe(10);

            var finalState = await projectionService.Start(CancellationToken.None);

            count = await EventStoreHelper.GetEventCountFromStream(stream,CancellationToken.None);
            count.ShouldBe(2);
        }

        [Test]
        public async Task event_size_is_recorded() {

            IConfigurationBuilder builder = new ConfigurationBuilder()
                                            .SetBasePath(Directory.GetCurrentDirectory())
                                            .AddJsonFile("Config\\appsettings.event.analyser.json", optional: false);

            IConfiguration config = builder.Build();
            Options options = eventanalyser.Program.GetOptions(config);

            options = options with {
                                       EventStoreConnectionString = DockerHelper.EventStoreClient.ConnectionName,
                                       ByPassReadKeyToStart = true,
                                   };

            EventTypeSizeState state = new();
            Projection<EventTypeSizeState> projection = new EventTypeSizeProjection(state, options);

            ProjectionService projectionService = new(projection, 
                                                      DockerHelper.EventStoreClient, 
                                                      options);

            String @event = @"{
  ""id"": 1
}";

            await EventStoreHelper.WriteEvent("TestStream1", @event, "event1");

            //TODO: Should be a Result
            var result = await projectionService.Start(CancellationToken.None);

            result.ShouldBeOfType<EventTypeSizeState>();
            EventTypeSizeState finalState = result as EventTypeSizeState;

            finalState.EventInfo.Count.ShouldBe(1);

            Int32 eventCount = finalState.EventInfo.Count;

            eventCount.ShouldBe(1);

            EventInfo eventInfo = finalState.EventInfo.First().Value;

            var sizeInBytes = eventInfo.SizeInBytes;
            sizeInBytes.ShouldBe(96);
        }

        [Test]
        public async Task stream_delete_only_deletes_streams_older_than_date() {
            StreamState streamState = new();
            Guid organisationId = Guid.NewGuid();
            // TODO: add second test event
            DeleteStreamBefore deleteOptions = new(new DateTime(2025,10,30), ["OldSaleStarted", "SaleStarted"]) {
                                                                                                                    SafeMode = false
                                                                                                                };

            eventanalyser.Options options = new(DockerHelper.EventStoreClient.ConnectionName, 
                                                "") {
                                                                                                      ByPassReadKeyToStart = true,
                                                                                                      DeleteOptions = deleteOptions
            };

            StreamRemovalProjection projection = new(streamState, 
                                                     deleteOptions, 
                                                     DockerHelper.EventStoreClient);

            ProjectionService projectionService = new(projection,
                                                      DockerHelper.EventStoreClient,
                                                      options);




            var startDate = new DateTime(2025, 9, 1);
            var endDate = new DateTime(2025, 9, 30);

            List< (DateTime,String)> streams = new();

            for (var date = startDate; date <= endDate; date = date.AddDays(1)) {
                var stream = await this.EventStoreHelper.WriteSaleEvent(date, organisationId, true);

                streams.Add(stream);
            }

            // We need to add some sales to the database of different dates
            startDate = new DateTime(2025, 10, 1);
            endDate = new DateTime(2025, 10, 31);
            
            for (var date = startDate; date <= endDate; date = date.AddDays(1)) {
                var stream = await this.EventStoreHelper.WriteSaleEvent(date, organisationId, false);

                streams.Add(stream);
            }

            await Task.Delay(TimeSpan.FromMilliseconds(500));

            var count = await EventStoreHelper.GetEventCountFromStream("$ce-SalesTransactionAggregate", CancellationToken.None);
            count.ShouldBe(61);

            var finalState = await projectionService.Start(CancellationToken.None);

            Int32 deletedStreamCount = 0;
            Int32 notDeletedStreamCount = 0;
            // Now check the sales have been deleted correctly
            foreach ((DateTime saleDateTime, String) stream in streams) {
                count = await this.EventStoreHelper.GetEventCountFromStream(stream.Item2, 
                                                                            CancellationToken.None);

                try {
                    if (stream.saleDateTime < deleteOptions.DateTime.Date)
                    {
                        count.ShouldBe(0, stream.saleDateTime.ToString());
                        deletedStreamCount++;
                    } else
                    {
                        count.ShouldNotBe(0, stream.saleDateTime.ToString());
                        notDeletedStreamCount++;
                    }
                }
                catch(Exception e) {
                    Console.WriteLine(e);
                    throw;
                }
            }

            deletedStreamCount.ShouldBe(59);
            notDeletedStreamCount.ShouldBe(2);
        }

        //TODO: Startpoint
        //TODO: Delete Organisation
        //TODO: StreamRemovalProjection Metadata - various filters
    }
}