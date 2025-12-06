package se.alipsa.jparq.cli;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Tests for verifying that the CLI user input highlighter colors input text and
 * resets afterward.
 */
class UserInputHighlighterTest {

  @Test
  void shouldWrapUserInputWithBrightColorAndReset() {
    UserInputHighlighter highlighter = new UserInputHighlighter();
    String highlighted = highlighter.highlight(null, "select * from mtcars").toAnsi();
    assertTrue(highlighted.startsWith("\u001B["));
    assertTrue(highlighted.contains("select * from mtcars"));
    assertTrue(highlighted.contains(JParqCliSession.ANSI_RESET));
  }

  @Test
  void shouldResetColorWhenBufferIsEmpty() {
    UserInputHighlighter highlighter = new UserInputHighlighter();
    String highlighted = highlighter.highlight(null, "").toAnsi();
    assertTrue(highlighted.isEmpty());
  }
}
