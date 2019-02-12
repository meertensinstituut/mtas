package mtas.analysis.util;

import org.junit.Test;

import javax.xml.stream.XMLStreamException;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class ConfigurationTest {
  // Regression test: previously, XML without an encoding would throw NullPointerException instead of IOException
  @Test
  public void noEncodingDeclaration() throws IOException, XMLStreamException {
    try {
      Configuration.read(new ByteArrayInputStream("<?xml version=\"1.0\"?><foo/>".getBytes()));
    } catch (Exception e) {
      if (!e.getMessage().contains("UTF-8")) {
        throw e;
      }
    }
  }
}
