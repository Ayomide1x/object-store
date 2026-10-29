package io.github.ayomide1x.objectstore.coordinator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.grpc.Status;
import org.springframework.stereotype.Component;

/**
 * Node liveness from heartbeats, judged only by this coordinator's monotonic
 * clock at receipt. In-memory and authoritative; rebuilt from heartbeats after
 * a restart (invariant 7), when every node starts UNKNOWN.
 *
 * <pre>
 * UNKNOWN --beat--> ALIVE --silence >= suspectAfter--> SUSPECT --beat--> ALIVE
 * ALIVE / SUSPECT --silence >= deadAfter--> DEAD
 * UNKNOWN --no beat for deadAfter since start--> DEAD
 * DEAD --recoveryBeats consecutive beats, each within suspectAfter--> ALIVE
 * </pre>
 *
 * Membership is fixed by COORDINATOR_NODES: a beat from any other node ID is
 * rejected. Each node process sends a random instance ID. A new instance is a
 * restart; if a replaced instance beats again, two processes claim the node,
 * and every beat for it is rejected until only one instance has beaten for
 * deadAfter. Rejected beats are not signs of life, so a conflicted node goes
 * SUSPECT and then DEAD on its own.
 */
@Component
public class LivenessTracker {

    /** Liveness states, matching the CHECK constraint on nodes.state. */
    public enum State { UNKNOWN, ALIVE, SUSPECT, DEAD }

    /** A state change, for logging and persisting. */
    public record Transition(String nodeId, State from, State to, long silenceNanos) {
    }

    /** Result of an accepted beat. */
    public record Beat(State state, Transition transition, boolean newInstance) {
    }

    private static final class Life {
        State state = State.UNKNOWN;
        long deadSince = -1; // monotonic nanos when the node last became DEAD
        long lastBeat = -1; // monotonic nanos of the last accepted beat; -1 = never
        int recoveryCount;
        String instance;
        final Set<String> replaced = new HashSet<>(); // instances superseded by a restart
        final Map<String, Long> lastSeenBy = new HashMap<>(); // every recent instance, accepted or not
        boolean conflicted;
    }

    private final Ticker ticker;
    private final long suspectAfter;
    private final long deadAfter;
    private final int recoveryBeats;
    private final long startedAt;
    private final Map<String, Life> nodes = new LinkedHashMap<>();

    public LivenessTracker(Ticker ticker, CoordinatorProperties properties) {
        this.ticker = ticker;
        this.suspectAfter = properties.suspectAfter().toNanos();
        this.deadAfter = properties.deadAfter().toNanos();
        this.recoveryBeats = properties.recoveryBeats();
        this.startedAt = ticker.nanos();
        for (Node n : properties.parseNodes()) {
            nodes.put(n.nodeId(), new Life());
        }
    }

    /**
     * Records a beat received now. Throws NOT_FOUND for an unregistered node
     * and FAILED_PRECONDITION while two instances claim the node; neither
     * counts as a sign of life.
     */
    public synchronized Beat beat(String nodeId, String instanceId) {
        Life life = nodes.get(nodeId);
        if (life == null) {
            throw new RequestException(Status.Code.NOT_FOUND,
                    "node %s is not in COORDINATOR_NODES".formatted(nodeId));
        }
        if (instanceId == null || instanceId.isEmpty()) {
            throw RequestException.invalid("instance_id is required");
        }
        long now = ticker.nanos();
        life.lastSeenBy.put(instanceId, now);
        life.lastSeenBy.values().removeIf(seen -> now - seen >= deadAfter);

        boolean newInstance = false;
        if (!instanceId.equals(life.instance)) {
            if (life.instance == null) {
                life.instance = instanceId;
            } else if (life.conflicted || life.replaced.contains(instanceId)) {
                // A replaced instance is still beating: two processes claim this node.
                life.conflicted = true;
            } else {
                // A new process: the old one restarted (or was replaced).
                life.replaced.add(life.instance);
                life.instance = instanceId;
                newInstance = true;
            }
        }
        if (life.conflicted) {
            if (life.lastSeenBy.size() > 1) {
                throw new RequestException(Status.Code.FAILED_PRECONDITION,
                        "node %s is claimed by %d processes (instances %s); beats rejected until only one remains"
                                .formatted(nodeId, life.lastSeenBy.size(), life.lastSeenBy.keySet()));
            }
            // Only this instance has beaten for deadAfter: it is the node.
            life.conflicted = false;
            life.instance = instanceId;
            life.replaced.clear();
        }

        long silence = life.lastBeat < 0 ? Long.MAX_VALUE : now - life.lastBeat;
        State from = life.state;
        switch (life.state) {
            case UNKNOWN, SUSPECT -> life.state = State.ALIVE;
            case ALIVE -> {
            }
            case DEAD -> {
                // Hysteresis: a flapping node does not bounce back into placement.
                life.recoveryCount = silence <= suspectAfter ? life.recoveryCount + 1 : 1;
                if (life.recoveryCount >= recoveryBeats) {
                    life.state = State.ALIVE;
                    life.recoveryCount = 0;
                }
            }
        }
        life.lastBeat = now;
        Transition t = from == life.state ? null : new Transition(nodeId, from, life.state, silence);
        return new Beat(life.state, t, newInstance);
    }

    /** Applies timeouts as of now and returns the transitions made. */
    public synchronized List<Transition> sweep() {
        long now = ticker.nanos();
        List<Transition> out = new ArrayList<>();
        for (Map.Entry<String, Life> e : nodes.entrySet()) {
            Life life = e.getValue();
            long silence = life.lastBeat < 0 ? now - startedAt : now - life.lastBeat;
            State next = switch (life.state) {
                case UNKNOWN -> silence >= deadAfter ? State.DEAD : State.UNKNOWN;
                case ALIVE -> silence >= deadAfter ? State.DEAD : silence >= suspectAfter ? State.SUSPECT : State.ALIVE;
                case SUSPECT -> silence >= deadAfter ? State.DEAD : State.SUSPECT;
                case DEAD -> State.DEAD;
            };
            if (next != life.state) {
                out.add(new Transition(e.getKey(), life.state, next, silence));
                life.state = next;
                life.recoveryCount = 0;
                if (next == State.DEAD) {
                    life.deadSince = now;
                }
            }
        }
        return out;
    }

    /**
     * How long a node has been DEAD by this coordinator's clock, or -1 if it is
     * not DEAD. After a coordinator restart this starts again from zero, which
     * delays repair, never hastens it.
     */
    public synchronized long deadForNanos(String nodeId) {
        Life life = nodes.get(nodeId);
        if (life == null || life.state != State.DEAD || life.deadSince < 0) {
            return -1;
        }
        return ticker.nanos() - life.deadSince;
    }

    public synchronized State stateOf(String nodeId) {
        Life life = nodes.get(nodeId);
        return life == null ? State.UNKNOWN : life.state;
    }

    /** The current instance ID of a node, or null if it has never beaten. */
    public synchronized String instanceOf(String nodeId) {
        Life life = nodes.get(nodeId);
        return life == null ? null : life.instance;
    }

    public synchronized Map<String, State> snapshot() {
        Map<String, State> out = new LinkedHashMap<>();
        nodes.forEach((id, life) -> out.put(id, life.state));
        return out;
    }
}
