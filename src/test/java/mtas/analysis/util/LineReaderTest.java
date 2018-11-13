package mtas.analysis.util;

import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;

import static org.junit.Assert.assertEquals;

public class LineReaderTest {
  @Test
  public void readLine() throws Exception {
    LineReader r = new LineReader(new StringReader("foo\r\nbar\nbaz\r"));
    assertEquals(0, r.getPosition());
    assertEquals("foo", r.readLine());
    // 4 is the position between the \r and \n. We want this and not 5 (after \n)
    // for backward compatibility.
    assertEquals(4, r.getPosition());
    assertEquals("bar", r.readLine());
    assertEquals(9, r.getPosition());
    assertEquals("baz", r.readLine());
    assertEquals(13, r.getPosition());
  }

  // Regression test, previous code got this wrong.
  @Test
  public void noTrailingNewline() throws IOException {
    String s = "blablablablablablabla";
    LineReader r = new LineReader(new StringReader(s));
    assertEquals(s, r.readLine());
    assertEquals(s.length(), r.getPosition());
  }
}
