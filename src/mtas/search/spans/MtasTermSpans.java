package mtas.search.spans;

import java.io.IOException;
import java.util.Collection;
import java.util.TreeSet;

import mtas.analysis.token.MtasPosition;
import mtas.codec.payload.MtasPayloadDecoder;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.payloads.PayloadSpanCollector;
import org.apache.lucene.search.spans.TermSpans;

/**
 * The Class MtasTermSpans.
 */
public class MtasTermSpans extends TermSpans {
  
  /** The mtas position. */
  protected MtasPosition mtasPosition = null;
  
  /** The assume single position. */
  private boolean assumeSinglePosition;
  
  /** The payload span collector. */
  private PayloadSpanCollector payloadSpanCollector;
  
  /**
   * Instantiates a new mtas term spans.
   *
   * @param postings the postings
   * @param term the term
   */
  public MtasTermSpans(PostingsEnum postings, Term term) {
    this(postings,term,false);
  }
  
  /**
   * Instantiates a new mtas term spans.
   *
   * @param postings the postings
   * @param term the term
   * @param assumeSinglePosition the assume single position
   */
  public MtasTermSpans(PostingsEnum postings, Term term, boolean assumeSinglePosition) {
    super(null,postings, term,1);    
    payloadSpanCollector = new PayloadSpanCollector();
    this.assumeSinglePosition = assumeSinglePosition;    
  }

  @Override
  public int endPosition() {
    if(assumeSinglePosition) {
      return super.endPosition();
    } else {
      int status = super.endPosition();      
      try {
        processEncodedPayload();
        return (status != NO_MORE_POSITIONS) ? (mtasPosition.getEnd()+1) : NO_MORE_POSITIONS;      
      } catch (IOException e) {
        return NO_MORE_POSITIONS;
      }
    }  
  }
  
  public TreeSet<Integer> getPositions() {
    TreeSet<Integer> list;
    if(assumeSinglePosition) {
      list = new TreeSet<Integer>();
      list.add(super.startPosition());
      return list;
    } else {
      try {
        processEncodedPayload();
        return mtasPosition.getPositions();    
      } catch(IOException e) {
        list = new TreeSet<Integer>();
        for(int i = super.endPosition(); i < super.endPosition(); i++)
          list.add(i);
        return list;
      }
    }
  }

  
  /**
   * Process encoded payload.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private void processEncodedPayload() throws IOException {
    if(!readPayload) {
      payloadSpanCollector.reset();
      collect(payloadSpanCollector);      
      Collection<byte[]> originalPayloadCollection = payloadSpanCollector.getPayloads();
      if(originalPayloadCollection.iterator().hasNext()) {
        byte[] payload = originalPayloadCollection.iterator().next();
        if(payload==null) {
          throw new IOException("no payload");
        }      
        MtasPayloadDecoder payloadDecoder = new MtasPayloadDecoder();
        payloadDecoder.init(startPosition(), payload); 
        mtasPosition = payloadDecoder.getMtasPosition();
      } else {
        throw new IOException("no payload");
      }        
    }         
  }

}
