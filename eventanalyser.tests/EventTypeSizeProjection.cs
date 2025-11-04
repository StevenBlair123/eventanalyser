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

        private Options Options;

        [SetUp]
        public void Setup() {
            this.EventStoreHelper = new EventStoreHelper(DockerHelper.EventStoreClient);

            var testName = TestContext.CurrentContext.Test.Name;

            IConfigurationBuilder builder = new ConfigurationBuilder()
                                            .SetBasePath(Directory.GetCurrentDirectory())
                                            .AddJsonFile($"Config\\{testName}.json", optional: false);

            IConfiguration config = builder.Build();
            this.Options = eventanalyser.Program.GetOptions(config);

            Options = Options with {
                                       EventStoreConnectionString = DockerHelper.EventStoreClient.ConnectionName,
                                       ByPassReadKeyToStart = true,
                                   };
        }

        [Test]
        public async Task safe_mode_is_enabled_by_default() {
            DeleteState streamState = new();
            Guid organisationId = Guid.NewGuid();
            String stream = $"TestStream_{Guid.NewGuid():N}";

            StreamRemovalProjection projection = new(streamState, Options.DeleteOptions, DockerHelper.EventStoreClient);
            ProjectionService projectionService = new(projection, DockerHelper.EventStoreClient, Options);

            String @event = $@"{{
  ""organisationId"": ""{organisationId}""
}}";

            await EventStoreHelper.WriteEvent(stream, @event, "event1");

            await projectionService.Start(CancellationToken.None);

            Int32 eventCount = await EventStoreHelper.GetEventCountFromStream(stream,CancellationToken.None);

            eventCount.ShouldBe(1);
        }

        [Test]
        public async Task stream_meta_data_set_to_max_eventCount() {
            DeleteState streamState = new();
            Guid organisationId = Guid.NewGuid();
            SetStreamMaxEventCount deleteOptions = new(2, ["StoreProductStockUpdatedEvent", "StockUpdated"]);
            String stream = $"StoreProductStockTransferAggregate-{Guid.NewGuid():N}";

            this.Options = this.Options with {
                                                 DeleteOptions = this.Options.DeleteOptions with {
                                                                                                     SafeMode = false
                                                                                                 }
                                             };

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
                await this.EventStoreHelper.WriteEvent(stream, @event, "StoreProductStockUpdatedEvent");
            }
            for (Int32 i = 0; i < 10; i++)
            {
                await this.EventStoreHelper.WriteEvent(stream, @event, "StockUpdated");
            }

            IProjection projection = new StreamRemovalProjection(streamState,
                                                                             deleteOptions,
                                                                             DockerHelper.EventStoreClient);
            ProjectionService projectionService = new(projection, 
                                                      DockerHelper.EventStoreClient, 
                                                      options);

            var count = await EventStoreHelper.GetEventCountFromStream(stream,CancellationToken.None);
            count.ShouldBe(20);

            var finalState = await projectionService.Start(CancellationToken.None);

            count = await EventStoreHelper.GetEventCountFromStream(stream,CancellationToken.None);
            count.ShouldBe(2);
        }

        [Test]
        public async Task event_size_is_recorded() {

            EventTypeSizeState state = new();
            Projection<EventTypeSizeState> projection = new EventTypeSizeProjection(state, Options);

            ProjectionService projectionService = new(projection, 
                                                      DockerHelper.EventStoreClient, 
                                                      Options);

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
            DeleteState streamState = new();
            Guid organisationId = Guid.NewGuid();
            this.Options = this.Options with {
                                                 DeleteOptions = this.Options.DeleteOptions with {
                                                                                                     SafeMode = false
                                                                                                 }
                                             };

            StreamRemovalProjection projection = new(streamState, 
                                                     this.Options.DeleteOptions, 
                                                     DockerHelper.EventStoreClient);

            ProjectionService projectionService = new(projection,
                                                      DockerHelper.EventStoreClient,
                                                      Options);


            DeleteStreamBefore dsb = (DeleteStreamBefore)this.Options.DeleteOptions;

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
                    if (stream.saleDateTime < dsb.DateTime)
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