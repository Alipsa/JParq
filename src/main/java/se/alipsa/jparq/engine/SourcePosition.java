package se.alipsa.jparq.engine;

/**
 * Describes a location within a SQL statement using one-based line and column
 * coordinates.
 *
 * @param line
 *          one-based line number where the token begins
 * @param column
 *          one-based column number where the token begins
 */
public record SourcePosition(int line, int column) {
}
