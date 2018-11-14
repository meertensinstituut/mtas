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

  private Configuration() {
    this(null, null);
  }

  /**
   * Makes a Configuration with one child, sub.
   */
  Configuration(Configuration sub) {
    this();
    children.add(sub);
  }

  /**
   * Makes a configuration with the given parent and attributes.
   * attr must be a list of attribute names (even indices) and values (odd indices).
   */
  Configuration(String name, Configuration parent, String... attr) {
    if (attr.length % 2 != 0) {
      throw new IllegalArgumentException("attr must be a list of key-value pairs");
    }
    for (int i = 0; i < attr.length; i += 2) {
      attributes.put(attr[i], attr[i + 1]);
    }

    this.name = name;
    this.parent = parent;
  }

  public static Configuration read(InputStream reader) throws IOException, XMLStreamException {
    Configuration currentConfig = null;
    // parse xml
    XMLInputFactory factory = XMLInputFactory.newInstance();
    XMLStreamReader streamReader = factory.createXMLStreamReader(reader);
    QName qname;
    try {
      int event = streamReader.getEventType();
      while (true) {
        switch (event) {
          case XMLStreamConstants.START_DOCUMENT:
            if (!streamReader.getCharacterEncodingScheme().equals("UTF-8")) {
              throw new IOException("XML not UTF-8 encoded");
            }
            break;
          case XMLStreamConstants.END_DOCUMENT:
          case XMLStreamConstants.SPACE:
            break;
          case XMLStreamConstants.START_ELEMENT:
            // get data
            qname = streamReader.getName();
            if (currentConfig == null) {
              if (qname.getLocalPart().equals("mtas")) {
                currentConfig = new Configuration();
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
    if (attributes != null) {
      for (Entry<String, String> entry : attributes.entrySet()) {
        text += (indent > 0 ? String.format("%" + indent + "s", "") : "") + entry.getKey()
          + ":" + entry.getValue() + "\n";
      }
    }
    if (children != null) {
      for (Configuration child : children) {
        text += (indent > 0 ? String.format("%" + indent + "s", "") : "")
          + child.toString(indent + 2);
      }
    }
    return text;
  }
}
