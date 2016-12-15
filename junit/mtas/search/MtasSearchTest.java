package mtas.search;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;


public class MtasSearchTest {

  @org.junit.Test
  public void test() {
    //constructIndex();
  }

  private void constructIndex() {
    Directory directory = new RAMDirectory();
    HashMap<String, String> files = new HashMap<String, String>(); 
    files.put("title 1", "folia-samples/beets1.xml.gz");
    try {      
      createIndex(directory,"mtas.xml", files);
    } catch (IOException e) {
      
    }
    
    
  }
  
  private void createIndex(Directory directory, String configFile, HashMap<String, String> files) throws IOException {
    //analyzer
    Map<String,String> paramsCharFilterMtas = new HashMap<String,String>();
    paramsCharFilterMtas.put("type","file");
    Map<String,String> paramsTokenizer = new HashMap<String,String>();
    paramsTokenizer.put("configFile", configFile);    
    Analyzer mtasAnalyzer = CustomAnalyzer.builder(Paths.get("docker").toAbsolutePath()).addCharFilter("mtas", paramsCharFilterMtas).withTokenizer("mtas", paramsTokenizer).build();          
    Map<String, Analyzer> analyzerPerField = new HashMap<String, Analyzer>();
    analyzerPerField.put("mtas", mtasAnalyzer);
    PerFieldAnalyzerWrapper analyzer = new PerFieldAnalyzerWrapper(
        new StandardAnalyzer(), analyzerPerField);

    //indexwriter
    IndexWriterConfig config = new IndexWriterConfig(analyzer);
    config.setUseCompoundFile(false);
    config.setCodec(Codec.forName("MtasCodec"));
    IndexWriter w = new IndexWriter(directory, config);       
    
    //delete
    w.deleteAll();
    
    //add
    for(String title : files.keySet()) {
      addDoc(w, title, files.get(title));
    }   
    w.commit();
    
    //finish
    w.close();

  }
  
  private static void addDoc(IndexWriter w, String title, String file)
      throws IOException {
    Document doc = new Document();
    doc.add(new StringField("title", title, Field.Store.YES));
    //doc.add(new TestField("mtas", file, Field.Store.YES));
    w.addDocument(doc);
  }

  
}
