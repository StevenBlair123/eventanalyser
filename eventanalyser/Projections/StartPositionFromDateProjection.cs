namespace eventanalyser.Projections;

using EventStore.Client;

public record StartPositionFromDateState : State {
    //TODO: Current date and Position
    public DateTime CurrentDateTime { get; init; }

    public DateTime TargetDateTime { get; init; }

    //TODO: Between name for this.
    public Position CommitPositionForCurrentDateTime { get; init; }

    public Boolean TargetReached { get; init; }
}

public class StartPositionFromDateProjection : Projection<StartPositionFromDateState> {
    private readonly Options Options;
    Int64 EventCountRead = 0;
    ResolvedEvent? FirstEventOfDay = null;   // To track the first event found for the day

    private Position currentPosition; //TODO: Should this be in the state?

    public StartPositionFromDateProjection(StartPositionFromDateState state, Options options) : base(state) {
        this.Options = options;
        this.State = state with {
                               TargetDateTime = Options.EventDateFilter.GetValueOrDefault().Date
                           };
    }

    public override String GetFormattedName() {
        return $"{this.GetType().Name}";
    }

    protected override async Task<StartPositionFromDateState> HandleEvent(StartPositionFromDateState state,
                                                                          ResolvedEvent @event) {
        EventCountRead++;
        DateTime eventDate = @event.Event.Created;

        WriteLineHelper.WriteInfo($"Date: {eventDate.Date.ToString()}");

        //TODO: How do we force a SaveState?

        //TODO: How do we stop?
        //Boolean keepReading = true;

        //Scenario 1 - we find target date
        //Scenario 2 - never find target date but events before it
        //Scenario 3 - never find target date but no events before it

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

        

        //this.State.CurrentDateTime = eventDate.Date.Date;
        //this.State.CommitPositionForCurrentDateTime = @event.OriginalPosition;

        // Check if the event is on the target date
        //if (eventDate.Date == this.State.TargetDateTime.Date) {

        //    this.State = this.State with {
        //                                     CurrentDateTime = eventDate.Date,
        //                                     CommitPositionForCurrentDateTime = @event.OriginalPosition.GetValueOrDefault()
        //                                 };

        //    //TODO: How do we stop overwriting this now?


        //    //this.FirstEventOfDay = @event; // Track the first event found for that day
        //}

        

        //TODO: logging
        Console.WriteLine($"currentPosition: {@event.OriginalPosition.Value.CommitPosition}");

        // If we pass the target date (i.e., the event is earlier than the target date), stop reading
        //if (eventDate.Date < this.State.TargetDateTime.Date)
        //{
        //    // Update the current position to continue from the last event's position
        //    Position currentPosition = @event.OriginalPosition.GetValueOrDefault();



        //    if (FirstEventOfDay.HasValue == false)
        //    {
        //        FirstEventOfDay = @event; // Track the first event found for that day
        //    }

        //    //TODO: How to stop
        //    keepReading = false;  // Stop the outer loop


        //    //break;
        //}

        //// If a matching event was found, print the details
        //if (firstEventOfDay.HasValue)
        //{
        //    var matchedEvent = firstEventOfDay.Value;
        //    Position? result = firstEventOfDay.Value.OriginalPosition;
        //    Console.WriteLine($"First event on {firstEventOfDay.Value.Event.Created.ToShortDateString()} is {matchedEvent.Event.EventType} at {matchedEvent.Event.Created}");
        //    return result;
        //}

        //Console.WriteLine($"No event found on {targetDate.ToShortDateString()}");
        //return null;

        //TODO: This should be a new state!
        return await Task.FromResult(this.State);
    }
}