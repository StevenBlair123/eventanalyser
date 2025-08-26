namespace eventanalyser;

using System.Text;
using EventStore.Client;

public class Support {
    public static Func<ResolvedEvent, String> ConvertResolvedEventToString = (@event) => Support.ByteArrayToString(@event.Event.Data.ToArray());
    public static Func<Byte[], String> ByteArrayToString = (data) => Encoding.UTF8.GetString(data.ToArray());
}