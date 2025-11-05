namespace eventanalyser.Projections;

using KurrentDB.Client;

public record StartPositionFromDateState : State {
    //TODO: Current date and Position
    public DateTime CurrentDateTime { get; init; }

    public DateTime TargetDateTime { get; init; }

    //TODO: Between name for this.
    public Position CommitPositionForCurrentDateTime { get; init; }

    public Boolean TargetReached { get; init; }
}

public class StartPositionFromDateProjection : Projection<StartPositionFromDateState> {
    ResolvedEvent? FirstEventOfDay = null;   // To track the first event found for the day

    private Position currentPosition; //TODO: Should this be in the state?

    public StartPositionFromDateProjection(Options options) : base(options) {
        this.State = this.State with {
                                         TargetDateTime = this.Options.EventDateFilter.GetValueOrDefault().Date
                                     };
    }
    public override String GetFormattedName() {
        return $"{this.GetType().Name}";
    }

    protected override async Task<StartPositionFromDateState> HandleEvent(StartPositionFromDateState state,
                                                                          ResolvedEvent @event) {
        DateTime eventDate = @event.Event.Created;

        WriteLineHelper.WriteInfo($"Date: {eventDate.Date.ToString()}");

        if (this.State.CurrentDateTime == default) {
            //First day
            this.State = this.State with {
                                             CurrentDateTime = eventDate.Date,
                                             CommitPositionForCurrentDateTime = @event.OriginalPosition.GetValueOrDefault()
                                         };
        } else if (eventDate.Date == this.State.TargetDateTime.Date) {
            //Target hit

            this.State = this.State with {
                                             CurrentDateTime = eventDate.Date,
                                             CommitPositionForCurrentDateTime = @event.OriginalPosition.GetValueOrDefault(),
                                             TargetReached = true,
                                             ForceStateSave = true,
                                             FinishProjection = true,
            };

            //TODO: We need to force a save state here
        }
        else if (this.State.CurrentDateTime.Date < eventDate.Date && this.State.TargetReached == false) {
            //New day detected
            this.State = this.State with {
                CurrentDateTime = eventDate.Date,
                CommitPositionForCurrentDateTime = @event.OriginalPosition.GetValueOrDefault()
            };
        }

        //TODO: logging
        Console.WriteLine($"currentPosition: {@event.OriginalPosition.Value.CommitPosition}");

        //TODO: This should be a new state!
        return await Task.FromResult(this.State);
    }
}