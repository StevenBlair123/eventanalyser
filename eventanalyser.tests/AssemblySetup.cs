namespace eventanalyser.tests{
    using System;
    using Ductus.FluentDocker.Builders;
    using Ductus.FluentDocker.Services.Extensions;
    using EventStore.Client;
    using NUnit.Framework;

    [SetUpFixture]
    public static class TurboGoon
    {
        public static Boolean TestFlag;

        static TurboGoon() {
            TurboGoon.OneTimeTearDown();
        }

        [OneTimeSetUp]
        public static void Jstro() {

            if (DockerHelper.EventstoreContainer != null)
            {
                DockerHelper.EventstoreContainer.ClearUpContainer();
            }

            DockerHelper.StartContainerForEventStore().Wait();

            var eventStoreHttpPort = DockerHelper.EventstoreContainer.ToHostExposedEndpoint("2113/tcp").Port;
            var eventStoreLocalConnectionString = $"esdb://admin:changeit@127.0.0.1:{eventStoreHttpPort}?tls=false&tlsVerifyCert=false";

            var x = Setup.DefaultAppSettings["AppSettings:EventStoreConnectionString"];

            EventStoreClientSettings eventStoreClientSettings = EventStoreClientSettings.Create(eventStoreLocalConnectionString);

            DockerHelper.EventStoreClient = new EventStoreClient(eventStoreClientSettings);
        }

        [OneTimeTearDown]
        public static void Jstro2() {

            if (DockerHelper.EventstoreContainer != null)
            {
                DockerHelper.EventstoreContainer.ClearUpContainer();
            }
        }

        public static void OneTimeTearDown() {
            Console.WriteLine("OneTimeTearDown Base");

            var eventstoreContainer = new Builder().UseContainer()
                                                   .UseImage("eventstore/eventstore:22.10.0-bionic")
                                                   .ExposePort(2113)
                                                   .ExposePort(1113)
                                                   .WithName(DockerHelper.EvenstoreContainerName)
                                                   .WithEnvironment("EVENTSTORE_RUN_PROJECTIONS=all", "EVENTSTORE_START_STANDARD_PROJECTIONS=true", "EVENTSTORE_Insecure=true", "EVENTSTORE_ENABLE_ATOM_PUB_OVER_HTTP=true")
                                                   .WaitForPort("2113/tcp", 30000 /*30s*/)
                                                   .ReuseIfExists()
                                                   .Build();

            eventstoreContainer.ClearUpContainer();

        }
    }



}