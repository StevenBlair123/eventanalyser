using eventanalyser.Projections;

namespace eventanalyser;

public record Options {
    public Options(String eventStoreConnectionString, String streamName) {
        this.EventStoreConnectionString = eventStoreConnectionString;
        this.StreamName = streamName;
    }

    public String StreamName {
        get; init;
    }
    public String GroupName {
        get; init;
    }
    public String EventStoreConnectionString {
        get; init;
    }
    public Mode Mode {
        get; init;
    }
    public int? Position {
        get; init;
    }
    public Int32 BufferSize {
        get; init;
    }

    public Int32 CheckPointCount {
        get; set;
    }

    public DateTime? EventDateFilter
    {
        get; init;
    }

    public DeleteOptions DeleteOptions { get; init; }
}