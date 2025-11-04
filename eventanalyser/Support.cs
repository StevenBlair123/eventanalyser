namespace eventanalyser;

using System.Text;
using KurrentDB.Client;
using Newtonsoft.Json.Linq;

public class Support {
    public static Func<ResolvedEvent, String> ConvertResolvedEventToString = (@event) => Support.ByteArrayToString(@event.Event.Data.ToArray());
    public static Func<Byte[], String> ByteArrayToString = (data) => Encoding.UTF8.GetString(data.ToArray());

    public static String GetDateFromEvent(String json) {
        JObject jObject = JObject.Parse(json);

        return jObject switch {
            _ when false => null,
            _ when jObject["createdDateTime"] != null => jObject["createdDateTime"].ToString(),
            _ when jObject["dateTime"] != null => jObject["dateTime"].ToString(),
            _ when jObject["entryDateTime"] != null => jObject["entryDateTime"].ToString(),
            _ when jObject["completionDateTime"] != null => jObject["completionDateTime"].ToString(),
            _ when jObject["lastStockTransferDate"] != null => jObject["lastStockTransferDate"].ToString(),
            _ when jObject["deliveryDateTime"] != null => jObject["deliveryDateTime"].ToString(),
            _ when jObject["datetime"] != null => jObject["datetime"].ToString(),
            _ when jObject["orderCreatedDateTime"] != null => jObject["orderCreatedDateTime"].ToString(),
            _ when jObject["dt"] != null => jObject["dt"].ToString(),
            _ => null
        };
    }

    public static Boolean EventCanBeProcessed(ResolvedEvent @event) =>
        @event switch {
            _ when @event.Event.ContentType != "application/json" => false,
            _ when @event.Event.EventType.StartsWith("$") => false,
            _ => true
        };

}