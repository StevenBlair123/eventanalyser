using eventanalyser.Projections;

namespace eventanalyser.tests {
    using Shouldly;
    using System;
    using System.Threading;
    using static eventanalyser.Projections.DeleteOptions;
    using static System.Runtime.InteropServices.JavaScript.JSType;
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

        [Test]
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
            eventanalyser.Options options = new(DockerHelper.EventStoreClient.ConnectionName, "") {
                                                                                                      EventTypeSize = new EventTypeSize(true),

                                                                                                      ByPassReadKeyToStart = true
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
            DeleteStreamBefore deleteOptions = new(new DateTime(2025,10,30), new List<string>() {
                "OldSaleStarted", "SaleStarted"
            });
            deleteOptions = deleteOptions with {
                SafeMode = false
            };
            StreamRemovalProjection projection = new(streamState, deleteOptions, DockerHelper.EventStoreClient);

            var startDate = new DateTime(2025, 9, 1);
            var endDate = new DateTime(2025, 9, 30);

            List<ResolvedEvent> resolvedEvents = new();

            for (var date = startDate; date <= endDate; date = date.AddDays(1))
            {
                var r = await WriteSaleEvent(date, organisationId, isOldEvent:true);
                resolvedEvents.Add(r);
                Console.WriteLine($"Processing date: {date:yyyy-MM-dd}");
            }

            // We need to add some sales to the database of different dates
            startDate = new DateTime(2025, 10, 1);
            endDate = new DateTime(2025, 10, 31);
            
            for (var date = startDate; date <= endDate; date = date.AddDays(1)) {
                var r = await WriteSaleEvent(date, organisationId);
                resolvedEvents.Add(r);
                Console.WriteLine($"Processing date: {date:yyyy-MM-dd}");
            }

            var x = DockerHelper.EventStoreClient.ReadStreamAsync(Direction.Backwards, "$ce-SalesTransactionAggregate",
                StreamPosition.End, 100);
            var g = await x.ToListAsync();
            g.Count.ShouldBe(61);

            foreach (ResolvedEvent resolvedEvent in resolvedEvents) {
                await projection.Handle(resolvedEvent);
            }

            Int32 deletedStreamCount = 0;
            Int32 notDeletedStreamCount = 0;
            // Now check the sales have been deleted correctly
            foreach (ResolvedEvent resolvedEvent in resolvedEvents) {
                var stream = resolvedEvent.OriginalStreamId;
                var eventString = Support.ConvertResolvedEventToString(resolvedEvent);
                var date = Support.GetDateFromEvent(eventString);

                var readStremResult = DockerHelper.EventStoreClient.ReadStreamAsync(Direction.Backwards, stream, StreamPosition.End);
                var readstate = await readStremResult.ReadState;

                if (DateTime.Parse(date) < deleteOptions.DateTime.Date) {
                    readstate.ShouldBe(ReadState.StreamNotFound, date);
                    deletedStreamCount++;
                }
                else {
                    readstate.ShouldBe(ReadState.Ok, date);
                    notDeletedStreamCount++;
                }
            }

            deletedStreamCount.ShouldBe(59);
            notDeletedStreamCount.ShouldBe(2);
        }

        internal ResolvedEvent CreateResolvedEvent(String streamName,String eventType, Guid organisationId) {
            string @event = $@"{{
  ""organisationId"": ""{organisationId}""
}}";

            Byte[] byteArray = Encoding.UTF8.GetBytes(@event);

            ReadOnlyMemory<Byte> data = new(byteArray);

            IDictionary<String, String> metadata = new Dictionary<String, String>();

            metadata.Add("type", eventType);
            metadata.Add("created", "1");
            metadata.Add("content-type", "application/json");

            EventRecord eventRecord = new(streamName, Uuid.NewUuid(), StreamPosition.FromInt64(1), new Position(0, 0), metadata, data, null);

            ResolvedEvent resolvedEvent = new(eventRecord, null, null);

            return resolvedEvent;
        }

        internal async Task<ResolvedEvent> WriteSaleEvent(DateTime dateTime, Guid organisationId, System.Boolean isOldEvent=false) {
            Guid salesTransactionId  = Guid.NewGuid();
            String streamName = $"SalesTransactionAggregate-{salesTransactionId:N}";
            string @event = $@"{{
    ""organisationId"": ""{organisationId}"",
    ""stid"": ""{salesTransactionId}"",
    ""dt"": ""{dateTime:O}""
}}";

            String eventType = isOldEvent switch {
                true => "OldSaleStarted",
                _ => "SaleStarted"
            };

            Byte[] byteArray = Encoding.UTF8.GetBytes(@event);

            ReadOnlyMemory<Byte> data = new(byteArray);

            IDictionary<String, String> metadata = new Dictionary<String, String>();

            metadata.Add("type", eventType);
            metadata.Add("created", "1");
            metadata.Add("content-type", "application/json");

            EventRecord eventRecord = new(streamName, Uuid.NewUuid(), StreamPosition.FromInt64(1),
                new Position(0, 0), metadata, data, null);

            EventData eventData = new(Uuid.NewUuid(), eventType, eventRecord.Data);
            await DockerHelper.EventStoreClient.AppendToStreamAsync(streamName,
                EventStore.Client.StreamState.Any, [eventData]);

            ResolvedEvent resolvedEvent = new(eventRecord, null, null);
            return resolvedEvent;
        }

        //TODO: Startpoint
        //TODO: Delete Organisation
        //TODO: StreamRemovalProjection Metadata - various filters
    }
}