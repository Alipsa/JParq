package se.alipsa.jparq.cli;

import java.util.regex.Pattern;
import org.jline.reader.Highlighter;
import org.jline.reader.LineReader;
import org.jline.utils.AttributedString;

/**
 * Highlights user input to distinguish it from query results in the CLI.
 */
public class UserInputHighlighter implements Highlighter {

  /**
   * Apply a brighter ANSI color to the current input while ensuring the color is
   * reset afterward.
   *
   * @param reader
   *          the active line reader, not used for highlighting
   * @param buffer
   *          the text being entered by the user
   * @return a colored representation of the user input ready for rendering
   */
  @Override
  public AttributedString highlight(LineReader reader, String buffer) {
    String content = buffer == null ? "" : buffer;
    return AttributedString.fromAnsi(JParqCliSession.USER_INPUT + content + JParqCliSession.ANSI_RESET);
  }

  /**
   * JLine supplies error positions for syntax highlighting; not used for uniform
   * coloring.
   *
   * @param errorIndex
   *          index of the error in the current buffer
   */
  @Override
  public void setErrorIndex(int errorIndex) {
    // no-op: we tint the entire buffer uniformly
  }

  /**
   * Accepts error patterns from JLine parsing; unused because input is colored
   * uniformly.
   *
   * @param pattern
   *          the pattern identifying parsing errors
   */
  @Override
  public void setErrorPattern(Pattern pattern) {
    // no-op: no syntax-aware highlighting is performed
  }
}
