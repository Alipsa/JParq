package se.alipsa.jparq.engine.function;

import java.time.OffsetDateTime;

/** Captures one datetime value shared by every part of a SQL execution. */
public final class StatementTime {
  private static final ThreadLocal<OffsetDateTime> ACTIVE = new ThreadLocal<>();
  private static final Scope NO_OP = new Scope(null, false);
  private final OffsetDateTime timestamp = OffsetDateTime.now(DateTimeFunctions.defaultZone());

  /** Creates a fresh execution timestamp in the active session time zone. */
  public StatementTime() {
  }

  /**
   * Returns the active execution timestamp, or the current time outside SQL
   * execution.
   *
   * @return datetime value used by SQL datetime functions
   */
  public static OffsetDateTime current() {
    OffsetDateTime active = ACTIVE.get();
    return active == null ? OffsetDateTime.now(DateTimeFunctions.defaultZone()) : active;
  }

  /**
   * Activates this execution's timestamp until the returned scope closes.
   *
   * @return scope restoring the calling thread's previous timestamp
   */
  public Scope open() {
    OffsetDateTime previous = ACTIVE.get();
    if (previous == timestamp) {
      return NO_OP;
    }
    ACTIVE.set(timestamp);
    return new Scope(previous, true);
  }

  /** Restores the previous execution after nested or interleaved query work. */
  public static final class Scope implements AutoCloseable {
    private final OffsetDateTime previous;
    private final boolean restore;

    /**
     * Retains the timestamp to restore.
     *
     * @param previous
     *          enclosing execution's timestamp, or null
     * @param restore
     *          whether closing this scope restores thread state
     */
    private Scope(OffsetDateTime previous, boolean restore) {
      this.previous = previous;
      this.restore = restore;
    }

    /** Restores the enclosing execution and removes unused thread state. */
    @Override
    public void close() {
      if (!restore) {
        return;
      }
      if (previous == null) {
        ACTIVE.remove();
      } else {
        ACTIVE.set(previous);
      }
    }
  }
}
