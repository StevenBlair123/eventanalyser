namespace eventanalyser.tests;



public class Setup {
    public static Dictionary<String, String> DefaultAppSettings
    {
        get;
    } = new() {
                  ["AppSettings:OrganisationStoresNotificationQueue"] = "TestValue",
                  ["AppSettings:EventStoreConnectionString"] =
                      "esdb://admin:changeit@127.0.0.1:2113?tls=false&tlsVerifyCert=false",
                  ["AppSettings:EventStorePath"] =
                      "ConnectTo=tcp://admin:changeit@127.0.0.1:1113;VerboseLogging=true;",

                  ["ConnectionStrings:OrganisationReadModel"] =
                      "server=eposity-datawarehouse.cpevcayzsht6.eu-west-1.rds.amazonaws.com;user id=admin;password=treb8houp5boct_GHAN;database=OrganisationReadModel",
              };

    [OneTimeSetUp]
    public void OneTimeSetUp() {
        Console.WriteLine("OneTimeSetUp Base");

    }
}