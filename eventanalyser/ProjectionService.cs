namespace eventanalyser;

using EventStore.Client;
using KurrentDB.Client;
using Projections;

public class ProjectionService {
    private readonly IProjection Projection;
    private readonly EventStoreClient EventStoreClient;

    private readonly Options Options;

    public ProjectionService(IProjection projection, 
                             EventStoreClient eventStoreClient,
                             Options options) {
        this.Projection = projection;
        this.EventStoreClient = eventStoreClient;
        this.Options = options;
    }

    public async Task<State> Start(CancellationToken cancellationToken) {
        //TODO: Will we need config?
        WriteLineHelper.WriteInfo($"Starting projection {this.Projection.GetType().Name}");
        State state = this.Projection.GetState();
        SubscriptionFilterOptions filterOptions = new(EventTypeFilter.ExcludeSystemEvents());

        //TODO: Improve this (signal?)
        while (true) {
            try {
                FromAll fromAll = this.Options.StartFromPosition switch {
                    _ when this.Options.StartFromPosition.HasValue => FromAll.After(new Position(this.Options.StartFromPosition.Value, 
                                                                                                 Options.StartFromPosition.Value)),
                    _ => FromAll.Start
                    //_ => FromAll.After(startPosition.GetValueOrDefault())
                };

                if (fromAll != FromAll.Start) {
                    var g = fromAll.ToUInt64();
                    WriteLineHelper.WriteWarning($"Commit: {g.commitPosition}");
                    WriteLineHelper.WriteWarning($"Prepare: {g.preparePosition}");
                }

                if (this.Options.ByPassReadKeyToStart == false) {
                    WriteLineHelper.WritePrompt($"Press return to start...");
                    Console.ReadKey();
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

                            Console.WriteLine($"In handle {@event.Event.EventType}");
                            state = await this.Projection.Handle(@event);
                            break;

                        case StreamMessage.CaughtUp:
                            Console.WriteLine("Caught Up");
                            break;

                        case StreamMessage.LastAllStreamPosition:
                            Console.WriteLine("FirstStream Position");
                            break;

                        case StreamMessage.AllStreamCheckpointReached goon:
                            //if (goon.Position.PreparePosition == 0) {
                            //    state = state with {
                            //        FinishProjection = true
                            //    };
                            //}

                            Console.WriteLine("AllStreamCheckpointReached");
                            break;

                    }

                    if (message is StreamMessage.Event || message is StreamMessage.CaughtUp) {

                        Boolean shouldCheckpoint = this.Options.CheckPointCount switch {
                            > 0 and var cp when state.Count % cp == 0 => true,
                            _ => false
                        };

                        if (shouldCheckpoint || message is StreamMessage.CaughtUp || state.ForceStateSave) {

                            if (message is StreamMessage.Event sm) {

                                state = state with {
                                                       LastPosition = sm.ResolvedEvent.OriginalPosition.Value.CommitPosition,
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

                    //TODO: Dump final state to console as well?
                }

                if (state != null) {
                    if (state.FinishProjection) {
                        WriteLineHelper.WriteWarning($"Signalled to finish Projection");
                        break;
                    }
                }
            }
            catch(Exception e) {
                Console.WriteLine(e);
            }
        }

        WriteLineHelper.WriteInfo($"Projection finished");
        return state;
    }

    private static async Task SaveState<TState>(TState state, IProjection projection) where TState : State {
        //TODO: Log state to screen / file?
        String json = state.GetStateAsString();

        String exeDirectory = AppContext.BaseDirectory;
        String filePath = Path.Combine(exeDirectory, $"{projection.GetFormattedName()}.txt");

        await File.WriteAllTextAsync(filePath, json);
    }
}