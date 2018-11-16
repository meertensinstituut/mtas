package mtas.analysis.util;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

public class Configuration {
  private final String name;
  private final HashMap<String, String> attributes = new HashMap<>();
  private final List<Configuration> children = new ArrayList<>();
  private final Configuration parent;

  private Configuration(String name, Configuration parent) {
    this.name = name;
    this.parent = parent;
  }

  public static Configuration read(InputStream reader) throws IOException, XMLStreamException {
    Configuration currentConfig = null;
    XMLInputFactory factory = XMLInputFactory.newInstance();
    XMLStreamReader streamReader = factory.createXMLStreamReader(reader);
    QName qname;
    try {
      int event = streamReader.getEventType();
      while (true) {
        switch (event) {
          case XMLStreamConstants.START_DOCUMENT:
            if (!streamReader.getCharacterEncodingScheme().equals("UTF-8")) {
              throw new IOException("XML not UTF-8 encoded"); // XXX why is this?
            }
            break;
          case XMLStreamConstants.END_DOCUMENT:
          case XMLStreamConstants.SPACE:
            break;
          case XMLStreamConstants.START_ELEMENT:
            qname = streamReader.getName();
            if (currentConfig == null) {
              if (qname.getLocalPart().equals("mtas")) {
                currentConfig = new Configuration(null, null);
              } else {
                throw new IOException("no Mtas Configuration");
              }
            } else {
              Configuration parentConfig = currentConfig;
              currentConfig = new Configuration(qname.getLocalPart(), parentConfig);
              parentConfig.children.add(currentConfig);
              for (int i = 0; i < streamReader.getAttributeCount(); i++) {
                currentConfig.attributes.put(
                  streamReader.getAttributeLocalName(i),
                  streamReader.getAttributeValue(i));
              }
            }
            break;
          case XMLStreamConstants.END_ELEMENT:
            //noinspection ConstantConditions
            if (currentConfig.parent == null) {
              return currentConfig;
            } else {
              currentConfig = currentConfig.parent;
            }
            break;
          case XMLStreamConstants.CHARACTERS:
            break;
        }
        if (!streamReader.hasNext()) {
          break;
        }
        event = streamReader.next();
      }
    } finally {
      //noinspection ThrowFromFinallyBlock
      streamReader.close();
    }
    throw new IllegalStateException("not reached");
  }

  public Iterable<String> attrNames() {
    return attributes.keySet();
  }

  /**
   * Returns the value of an attribute or null if not set.
   */
  public String getAttr(String name) {
    return attributes.get(name);
  }

  public Configuration child(int i) {
    return children.get(i);
  }

  public int numChildren() {
    return children.size();
  }

  public String getName() {
    return name;
  }

  public String toString() {
    return toString(0);
  }

  private String toString(int indent) {
    String text = "";

    if (name != null) {
      text += (indent > 0 ? String.format("%" + indent + "s", "") : "")
        + "name: " + name + "\n";
    }

    for (Entry<String, String> entry : attributes.entrySet()) {
      text += (indent > 0 ? String.format("%" + indent + "s", "") : "") + entry.getKey()
        + ":" + entry.getValue() + "\n";
    }

    for (Configuration child : children) {
      text += (indent > 0 ? String.format("%" + indent + "s", "") : "")
        + child.toString(indent + 2);
    }

    return text;
  }
}
