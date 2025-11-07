namespace eventanalyser.tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Ductus.FluentDocker.Builders;
    using Ductus.FluentDocker.Extensions;
    using Ductus.FluentDocker.Model.Containers;
    using Ductus.FluentDocker.Services;
    using Ductus.FluentDocker.Services.Extensions;
    using KurrentDB.Client;
    using Shouldly;
    using SimpleResults;

    /// <summary>
    /// 
    /// </summary>
    public static class DockerHelper
    {
        private static readonly String SharedNetworkName = "shared-network-sqlserver";

        /// <summary>
        /// The SQL server host port
        /// </summary>
        public static IContainerService EventstoreContainer;
        public static Int32 EventStoreHttpPort;
        private static String EventStoreLocalConnectionString;

        public static String EvenstoreContainerName = "test-eventstore-for-projections";

        public static KurrentDBClient EventStoreClient { get; set; }

        public static async Task<SimpleResults.Result> StartContainerForEventStore()
        {
            // Startup Eventstore
            DockerHelper.EventstoreContainer = new Builder().UseContainer()
                                                            .UseImage("eventstore/eventstore:22.10.0-bionic")
                                                            .WithName(DockerHelper.EvenstoreContainerName)
                                                            .ExposePort(2113)
                                                            .ExposePort(1113)
                                                            .WithEnvironment(
                                                                             "EVENTSTORE_RUN_PROJECTIONS=all", 
                                                                             "EVENTSTORE_START_STANDARD_PROJECTIONS=true", 
                                                                             "EVENTSTORE_Insecure=true", 
                                                                             "EVENTSTORE_ENABLE_ATOM_PUB_OVER_HTTP=true")
                                                            .WaitForPort("2113/tcp", 30000 /*30s*/)
                                                            .Build();

            DockerHelper.EventstoreContainer.Start();

            DockerHelper.EventstoreContainer.WaitForHealthy(30000);

            Container config = DockerHelper.EventstoreContainer.GetConfiguration(true);

            config.State.ToServiceState().ShouldBe(ServiceRunningState.Running);

            // Event Store
            DockerHelper.EventStoreHttpPort = DockerHelper.EventstoreContainer.ToHostExposedEndpoint("2113/tcp").Port;



            Int32 port = DockerHelper.EventStoreHttpPort;

            var settings = KurrentDBClientSettings.Create(
                                                           $"esdb://admin:changeit@127.0.0.1:{port}?tls=false&tlsVerifyCert=false"
                                                          );
            DockerHelper.EventStoreClient = new KurrentDBClient(settings);


            //var settings = EventStoreClientSettings.Create(
            //                                               $"esdb://admin:changeit@127.0.0.1:{DockerHelper.EventStoreHttpPort}?tls=false&tlsVerifyCert=false"
            //                                              );
            //var client = new EventStoreClient(settings);


            //await client.DeleteAsync("Test", StreamState.Any);

            //DockerHelper.EventStoreLocalConnectionString = $"esdb://admin:changeit@127.0.0.1:{DockerHelper.EventStoreHttpPort}?tls=false&tlsVerifyCert=false";
            //Setup.DefaultAppSettings["AppSettings:EventStoreConnectionString"] = DockerHelper.EventStoreLocalConnectionString;

            return Result.Success();
        }
    }

    public static class DockerExtensions
    {
        /// <summary>
        /// ClearUpContainer the container.
        /// </summary>
        /// <param name="containerService">The container service.</param>
        public static void ClearUpContainer(this IContainerService containerService)
        {
            try
            {
                IList<IVolumeService> volumes = new List<IVolumeService>();
                IList<INetworkService> networks = containerService.GetNetworks();

                foreach (INetworkService networkService in networks)
                {
                    networkService.Detach(containerService, true);
                }

                // Doing a direct call to .GetVolumes throws an exception if there aren't any so we need to check first :|
                Container container = containerService.GetConfiguration(true);
                ContainerConfig containerConfig = container.Config;

                if (containerConfig != null)
                {
                    IDictionary<String, VolumeMount> configurationVolumes = containerConfig.Volumes;
                    if (configurationVolumes != null && configurationVolumes.Any())
                    {
                        volumes = containerService.GetVolumes();
                    }
                }

                containerService.StopOnDispose = true;
                containerService.RemoveOnDispose = true;
                containerService.Dispose();

                foreach (IVolumeService volumeService in volumes)
                {
                    volumeService.Stop();
                    volumeService.Remove(true);
                    volumeService.Dispose();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"Failed to stop container {containerService.InstanceId}. [{e}]");
            }
        }
    }
}