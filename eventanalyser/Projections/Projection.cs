namespace eventanalyser.Projections;

using EventStore.Client;
using Newtonsoft.Json;

public record State {
    public Int64 Count { get; init; }

    public State() {

    }

    public virtual String GetStateAsString() {
        return JsonConvert.SerializeObject(this);
    }
}

public abstract class Projection<TState> where TState : State {
    public TState State { get; set; }

    public Int64 Position { get; set; }

    protected Projection(TState state) {
        this.State = state;
    }

    public virtual async Task<TState> Handle(ResolvedEvent @event) {
        //Call child class Handle
        this.State = await this.HandleEvent(this.State, @event);

        this.State = this.State with {
                                         Count = this.State.Count + 1
                                     };

        //TODO: Update checkpoint - do we call each time, or add threshold here and only call when we actually update checkpoint?
        this.UpdateCheckpoint();

        return this.State;
    }
    protected abstract Task<TState> HandleEvent(TState state, ResolvedEvent @event);

    protected virtual void UpdateCheckpoint() {
        //TODO:
    }
}