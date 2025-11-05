namespace eventanalyser.Projections;

using KurrentDB.Client;
using Newtonsoft.Json;
using SimpleResults;

public record State {
    public Int64 Count { get; init; }
    public Position LastPosition { get; init; }

    //TODO: Not sure this is the best approach. Could an EventHandler work nicer?
    public Boolean ForceStateSave { get; set; }
    public Boolean FinishProjection { get; set; }

    public State() {
        
    }

    public virtual String GetStateAsString() => JsonConvert.SerializeObject(this);
}

public interface IProjection {
    Task<State> Handle(ResolvedEvent @event);
    String GetFormattedName();
    State GetState();
}

public abstract class Projection<TState> : IProjection where TState : State, new() {
    protected readonly Options Options;

    public TState State { get; set; }

    public Projection(Options options) {
        this.Options = options;
        if (this.Options.ReloadState == false) {
            State = new TState();
            WriteLineHelper.WriteWarning($"Forcing new state: {this.State.GetType().Name}");
            return;
        }

        Result<TState> result = ProjectionService.LoadState<TState>(this, CancellationToken.None).Result;

        if (result.IsFailed) {
            WriteLineHelper.WriteWarning($"Failed to load state {result.Message}. Default state will be used.");
            State = new TState();
        }
        else {
            this.State = result.Data;
        }

        WriteLineHelper.WriteWarning($"Loading state: {this.State.GetType().Name}");
    }

    public virtual async Task<State> Handle(ResolvedEvent @event) {
        //Call child class Handle
        this.State = await this.HandleEvent(this.State, @event);

        this.State = this.State with {
                                         Count = this.State.Count + 1,
                                         LastPosition = @event.OriginalPosition.Value,
                                     };

        //TODO: Update checkpoint - do we call each time, or add threshold here and only call when we actually update checkpoint?
        this.UpdateCheckpoint();

        return this.State;
    }
    public virtual String GetFormattedName() => $"{this.GetType().Name}";
    public virtual State GetState() => this.State;
    protected abstract Task<TState> HandleEvent(TState state, ResolvedEvent @event);

    protected virtual void UpdateCheckpoint() {
        //TODO:
    }
}