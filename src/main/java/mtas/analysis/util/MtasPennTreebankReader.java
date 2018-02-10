package mtas.analysis.util;

import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.regex.Pattern;

public class MtasPennTreebankReader {

  private Reader reader;
  private int bufferPosition;
  private int readPosition;
  private boolean eof;

  private char nextBracket;
  private String[] nextStrings;
  private int counterBracket;

  private int currentEvent;
  private int currentPosition;
  private int currentStringPosition;

  private static final int EVENT_INIT = 0;
  
  public static final int EVENT_STARTBRACKET = 1;
  public static final int EVENT_ENDBRACKET = 2;
  public static final int EVENT_NODE = 3;
  public static final int EVENT_STRING = 4;
  public static final int EVENT_EOF = 5;
  
  private static final char CHARACTER_BRACKETSTART = '(';
  private static final char CHARACTER_BRACKETEND = ')';
  private static final char CHARACTER_CURLYBRACKETSTART = '{';
  private static final char CHARACTER_CURLYBRACKETEND = '}';
  
  private static final int DEFAULTCHARBUFFERSIZE = 8192;
  private int charBufferSize;
  private char[] charBuffer;

  public MtasPennTreebankReader(Reader reader) throws IOException {
    this.reader = reader;
    nextBracket = 0;
    nextStrings = null;
    counterBracket = 0;
    readPosition = -1;
    bufferPosition = 0;
    currentEvent = EVENT_INIT;
    charBuffer = null;
    charBufferSize = 0;
    eof = false;
    next();
  }

  public int getEventType() {
    return currentEvent;
  }

  public int getPosition() {
    return currentPosition;
  }

  public String getString() throws IOException {
    if (currentEvent == EVENT_NODE || currentEvent == EVENT_STRING) {
      return nextStrings[currentStringPosition];
    } else {
      throw new IOException("unexpected state");
    }
  }

  public boolean next() throws IOException {
    if (currentEvent == EVENT_EOF) {
      return false;
    } else if (currentEvent == EVENT_INIT) {
      if (findBracket()) {
        currentPosition = 0;
        currentStringPosition = -1;
        currentEvent = EVENT_STRING;       
        if (findNextString()) {
          throw new IOException("string without opening bracket");
        } else if (nextBracket == CHARACTER_BRACKETSTART) {
          currentEvent = EVENT_STARTBRACKET;
          currentPosition = readPosition;
          return true;
        } else {
          throw new IOException("unexpected state");
        }        
      } else {
        currentEvent = EVENT_EOF;
        return false;
      }      
    } else if (currentEvent == EVENT_NODE || currentEvent == EVENT_STRING) {
      currentEvent = EVENT_STRING;
      if (findNextString()) {
        return true;
      } else if (nextBracket == CHARACTER_BRACKETSTART) {
        currentEvent = EVENT_STARTBRACKET;
        currentPosition = readPosition;
        return true;
      } else if (nextBracket == CHARACTER_BRACKETEND) {
        currentEvent = EVENT_ENDBRACKET;
        currentPosition = readPosition;
        return true;
      } else {
        throw new IOException("unexpected state");
      }
    } else if (currentEvent == EVENT_STARTBRACKET
        || currentEvent == EVENT_ENDBRACKET) {
      if (findBracket()) {
        if (currentEvent == EVENT_STARTBRACKET) {
          currentEvent = EVENT_NODE;
        } else {
          currentEvent = EVENT_STRING;
        }  
        currentStringPosition = -1;
        if (findNextString()) {
          return true;
        } else if (nextBracket == CHARACTER_BRACKETSTART) {
          currentEvent = EVENT_STARTBRACKET;
          currentPosition = readPosition;
          return true;
        } else if (nextBracket == CHARACTER_BRACKETEND) {
          currentEvent = EVENT_ENDBRACKET;
          currentPosition = readPosition;
          return true;
        } else {
          throw new IOException("unexpected state");
        }
      } else {
        currentEvent = EVENT_EOF;
        return false;
      }
    } else {
      throw new IOException("unexpected state");
    }
  }

  private boolean findNextString() throws IOException {
    if (currentEvent == EVENT_NODE || currentEvent == EVENT_STRING) {
      while (currentStringPosition < (nextStrings.length - 1)) {
        currentStringPosition++;
        currentPosition++;
        if (currentStringPosition > 0) {
          currentPosition += nextStrings[currentStringPosition - 1].length();
        }
        if (!nextStrings[currentStringPosition].trim().isEmpty()) {
          return true;
        }
      }
      if (currentStringPosition > 0) {
        currentPosition += 1 + nextStrings[currentStringPosition - 1].length();
      }
      return false;
    } else {
      throw new IOException("unexpected state");
    }
  }

  private boolean findBracket() throws IOException {    
    StringBuilder sBuilder = new StringBuilder("");
    while (!eof) {
      char c = nextCharacter();
      if (c == CHARACTER_BRACKETSTART) {
        counterBracket++;
        nextStrings = createStrings(sBuilder.toString());
        nextBracket = c;
        return true;
      } else if (c == CHARACTER_BRACKETEND) {
        counterBracket--;
        nextStrings = createStrings(sBuilder.toString());
        if (counterBracket < 0) {
          throw new IOException("bracket mismatch");
        } else if (nextBracket == CHARACTER_BRACKETSTART
            && nextStrings.length == 0) {
          throw new IOException("empty brackets");
        }
        nextBracket = c;
        return true;
      } else {
        sBuilder.append(c);
      }      
    }
    if (eof) {
      if(!sBuilder.toString().trim().isEmpty()) {    
        throw new IOException("string without closing bracket");
      } else if(counterBracket!=0) {
        throw new IOException("unclosed bracket(s)");
      }
    }
    if (counterBracket != 0) {
      throw new IOException("bracket mismatch");
    } else {
      return false;
    }
  }

  private char nextCharacter() throws IOException {
    if(charBuffer==null) {
      charBuffer = new char[DEFAULTCHARBUFFERSIZE];
      charBufferSize = reader.read(charBuffer, 0, charBuffer.length);
      eof = (charBufferSize > 0) ? false : true;
    }
    if (bufferPosition < charBufferSize) {
      readPosition++;
      char c = charBuffer[bufferPosition];
      bufferPosition++;
      // refill buffer if needed
      if (bufferPosition == charBufferSize) {
        bufferPosition = 0;
        charBufferSize = reader.read(charBuffer, 0, charBuffer.length);
        if (charBufferSize <= 0) {
          eof = true;
        }
      }
      return c;
    } else {
      throw new IOException("no (more) characters in reader");
    }
  }
  
  public static String[] createStrings(String s) throws IOException {  
    return createStrings(s, null);
  }
  
  public static String[] createStrings(String s, String splitRegexp) throws IOException {  
    final String pattern = Pattern.quote(Character.toString(CHARACTER_CURLYBRACKETSTART))+".*"+Pattern.quote(Character.toString(CHARACTER_CURLYBRACKETEND));
    splitRegexp = (splitRegexp==null) ? "\\s" : splitRegexp;
    String[] initialList = s.split(splitRegexp, -1);
    if(s.indexOf(CHARACTER_CURLYBRACKETSTART)==-1) {
      return initialList;
    } else {
      String[] finalList = new String[initialList.length];
      int j = 0;
      boolean withinCurlyBracket = false;
      for(int i=0; i<initialList.length; i++) {
        if(withinCurlyBracket) {
          finalList[j-1] = finalList[j-1] + " " + initialList[i]; 
          if(initialList[i].indexOf(CHARACTER_CURLYBRACKETEND)!=-1) {
            String item = CHARACTER_CURLYBRACKETSTART+initialList[i];
            if(item.replaceAll(pattern, "").indexOf(CHARACTER_CURLYBRACKETSTART)==-1) {
              withinCurlyBracket = false;
            } 
          }
        } else {
          finalList[j] = initialList[i];
          j++;
          if(initialList[i].indexOf(CHARACTER_CURLYBRACKETSTART)!=-1) {
            String item = initialList[i];
            if(item.replaceAll(pattern, "").indexOf(CHARACTER_CURLYBRACKETSTART)!=-1) {
              withinCurlyBracket = true; 
            }
          }
        }      
      }
      if(withinCurlyBracket) {
        throw new IOException("unclosed curly bracket for "+s);
      }
      return Arrays.copyOf(finalList, j); 
    }  
  }  

}
