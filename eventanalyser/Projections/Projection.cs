namespace eventanalyser.Projections;

using EventStore.Client;
using Newtonsoft.Json;

public record State {
    public Int64 Count { get; init; }
    public UInt64 LastPosition { get; init; }

    //TODO: Not sure this is the best approach. Could an EventHandler work nicer?
    public Boolean ForceStateSave { get; set; }
    public Boolean FinishProjection { get; set; }

    public State() {
        
    }

    public virtual String GetStateAsString() {
        return JsonConvert.SerializeObject(this);
    }
}

public interface IProjection {
    Task<State> Handle(ResolvedEvent @event);

    String GetFormattedName();
    State GetState();
}

public abstract class Projection<TState> : IProjection where TState : State {
    public TState State { get; set; }

    public Int64 Position { get; set; }

    public Projection(TState state) {
        this.State = state;
    }

    public virtual async Task<State> Handle(ResolvedEvent @event) {
        //Call child class Handle
        this.State = await this.HandleEvent(this.State, @event);

        this.State = this.State with {
                                         Count = this.State.Count + 1
                                     };

        //TODO: Update checkpoint - do we call each time, or add threshold here and only call when we actually update checkpoint?
        this.UpdateCheckpoint();

        return this.State;
    }

    public virtual String GetFormattedName() {
        return $"{this.GetType().Name}";
    }

    public virtual State GetState() {
        return this.State;
    }


    protected abstract Task<TState> HandleEvent(TState state, ResolvedEvent @event);

    protected virtual void UpdateCheckpoint() {
        //TODO:
    }
}