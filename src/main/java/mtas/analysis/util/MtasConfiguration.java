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

public class MtasConfiguration {
  public String name = null;
  public HashMap<String, String> attributes = new HashMap<>();
  public List<MtasConfiguration> children = new ArrayList<>();
  public MtasConfiguration parent = null;

  public MtasConfiguration() {
  }

  public static MtasConfiguration readConfiguration(InputStream reader)
    throws IOException, XMLStreamException {
    MtasConfiguration currentConfig = null;
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
                currentConfig = new MtasConfiguration();
              } else {
                throw new IOException("no Mtas Configuration");
              }
            } else {
              MtasConfiguration parentConfig = currentConfig;
              currentConfig = new MtasConfiguration();
              parentConfig.children.add(currentConfig);
              currentConfig.parent = parentConfig;
              currentConfig.name = qname.getLocalPart();
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
      for (MtasConfiguration child : children) {
        text += (indent > 0 ? String.format("%" + indent + "s", "") : "")
          + child.toString(indent + 2);
      }
    }
    return text;
  }
}
