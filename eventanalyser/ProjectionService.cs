namespace eventanalyser;

using KurrentDB.Client;
using Newtonsoft.Json;
using Projections;
using SimpleResults;

public class ProjectionService {
    private readonly IProjection Projection;
    private readonly KurrentDBClient EventStoreClient;

    private readonly Options Options;

    public ProjectionService(IProjection projection,
                             KurrentDBClient eventStoreClient,
                             Options options) {
        this.Projection = projection;
        this.EventStoreClient = eventStoreClient;
        this.Options = options;
    }

    public async Task<Result<State>> Start(CancellationToken cancellationToken) {
        //TODO: Will we need config?
        WriteLineHelper.WriteInfo($"Starting projection {this.Projection.GetType().Name}");

        State state = this.Projection.GetState();
        SubscriptionFilterOptions filterOptions = new(EventTypeFilter.ExcludeSystemEvents());

        //TODO: Improve this (signal?)
        while (true) {
            try {
                FromAll fromAll = this.Options.StartFromPosition switch {
                    _ when state.LastPosition != null => FromAll.After(state.LastPosition),
                    _ => FromAll.Start
                };

                if (fromAll != FromAll.Start) {
                    var g = fromAll.ToUInt64();
                    WriteLineHelper.WriteWarning($"Commit: {g.commitPosition}");
                    WriteLineHelper.WriteWarning($"Prepare: {g.preparePosition}");
                }

                IAsyncEnumerable<StreamMessage>? messages = null;

                if (this.Projection is StartPositionFromDateProjection) {
                    KurrentDBClient.ReadAllStreamResult events = this.EventStoreClient.ReadAllAsync(Direction.Backwards,
                                                                                                     Position.End,
                                                                                                     EventTypeFilter.ExcludeSystemEvents(),
                                                                                                     4096, // Adjust page size as needed
                                                                                                     resolveLinkTos:true, // Whether to resolve links to events
                                                                                                     null, // Optional: credentials if required
                                                                                                     null,
                                                                                                     cancellationToken);

                    messages = events.Messages;


                }
                else {
                    KurrentDBClient.StreamSubscriptionResult subscription =
                        this.EventStoreClient.SubscribeToAll(fromAll, 
                                                             resolveLinkTos:true, 
                                                             filterOptions:filterOptions,
                                                             cancellationToken:cancellationToken);

                    messages = subscription.Messages;
                }

                await foreach (var message in messages.WithCancellation(cancellationToken)) {
                    switch(message) {
                        case StreamMessage.Event(var @event):
                            if (@event.Event == null)
                                continue;

                            WriteLineHelper.WriteInfo($"In handle {@event.Event.EventType}");
                            state = await this.Projection.Handle(@event);
                            break;

                        case StreamMessage.CaughtUp:
                            WriteLineHelper.WriteWarning("Caught Up");
                            break;

                        case StreamMessage.LastAllStreamPosition:
                            WriteLineHelper.WriteWarning("FirstStream Position");
                            break;

                        case StreamMessage.AllStreamCheckpointReached goon:
                            if (this.Options.IsTestMode) {
                                if (goon.Position.PreparePosition == 0) {
                                    state = state with {
                                                           FinishProjection = true
                                                       };
                                }
                            }

                            WriteLineHelper.WriteWarning("AllStreamCheckpointReached");
                            break;

                    }

                    if (message is StreamMessage.Event || message is StreamMessage.CaughtUp) {

                        Boolean shouldCheckpoint = this.Options.CheckPointCount switch {
                            > 0 and var cp when state.Count % cp == 0 => true,
                            _ => false
                        };

                        if (shouldCheckpoint || 
                            message is StreamMessage.CaughtUp || 
                            state.ForceStateSave) {

                            if (message is StreamMessage.Event sm) {

                                state = state with {
                                                       ForceStateSave = false
                                                   };
                            }

                            await ProjectionService.SaveState(state, this.Projection);
                        }
                    }

                    if (state != null) {
                        if (state.FinishProjection) {
                            WriteLineHelper.WriteWarning($"Signalled to finish Projection");
                            break;
                        }
                    }
                }

                if (state != null) {
                    if (state.FinishProjection) {
                        WriteLineHelper.WriteWarning($"Signalled to finish Projection");
                        break;
                    }
                }
            }
            catch(Exception e) {
                return Result.Failure(e.Message);
            }
        }

        WriteLineHelper.WriteInfo($"Projection finished");
        return state;
    }

    private static async Task SaveState<TState>(TState state, IProjection projection) where TState : State {
        String json = state.GetStateAsString();

        String exeDirectory = AppContext.BaseDirectory;
        String filename = GetStateFilename(projection);
        String filePath = Path.Combine(exeDirectory, $"{filename}");

        await File.WriteAllTextAsync(filePath, json);
    }

    public static async Task<SimpleResults.Result<TState>> LoadState<TState>(IProjection projection,
                                                                     CancellationToken cancellationToken) where TState : State {

        try {
            String exeDirectory = AppContext.BaseDirectory;
            String filename = GetStateFilename(projection);
            String filePath = Path.Combine(exeDirectory, $"{filename}");

            String json = await File.ReadAllTextAsync(filePath, cancellationToken);

            return JsonConvert.DeserializeObject<TState>(json);
        }
        catch(Exception e) {
            return Result.Failure(e.Message);
        }
    }

    static String GetStateFilename(IProjection projection) {
        return $"{projection.GetFormattedName()}.txt";
    }
}