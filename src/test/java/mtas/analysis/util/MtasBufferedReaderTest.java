package mtas.analysis.util;

import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;

import static org.junit.Assert.assertEquals;

public class MtasBufferedReaderTest {
  @Test
  public void readLine() throws Exception {
    MtasBufferedReader r = new MtasBufferedReader(new StringReader("foo\r\nbar\nbaz\r"));
    assertEquals(0, r.getPosition());
    assertEquals("foo", r.readLine());
    assertEquals(4, r.getPosition());
    assertEquals("bar", r.readLine());
    assertEquals(9, r.getPosition());
    assertEquals("baz", r.readLine());
    assertEquals(13, r.getPosition());
  }

  @Test
  @Ignore("known bug")
  public void noTrailingNewline() throws IOException {
    String s = "blablablablablablablablabla";
    MtasBufferedReader r = new MtasBufferedReader(new StringReader(s));
    assertEquals(s, r.readLine());
    assertEquals(s.length(), r.getPosition());
  }
}