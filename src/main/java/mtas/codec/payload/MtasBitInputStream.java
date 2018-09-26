package mtas.codec.payload;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class MtasBitInputStream extends ByteArrayInputStream {
  private int bitBuffer = 0;
  private int bitCount = 0;

  public MtasBitInputStream(byte[] buf) {
    super(buf);
  }

  public int readBit() throws IOException {
    if (bitCount == 0) {
      bitBuffer = read();
      if (bitBuffer == -1) {
        throw new IOException("no more bits");
      }
    }
    int value = (bitBuffer >> bitCount) & 1;
    bitCount++;
    if (bitCount > 7) {
      bitCount = 0;
    }
    return value;
  }

  public byte[] readRemainingBytes() throws IOException {
    if (this.available() > 0) {
      byte[] b = new byte[this.available()];
      if (read(b) >= 0) {
        return b;
      } else {
        throw new IOException("returned negative number of remaining bytes");
      }
    } else {
      throw new IOException("no more bytes");
    }
  }

  public int readEliasGammaCodingInteger() throws IOException {
    int value = readEliasGammaCodingPositiveInteger();
    if ((value % 2) == 0) {
      return (-value) / 2;
    } else {
      return (value - 1) / 2;
    }
  }

  public int readEliasGammaCodingNonNegativeInteger() throws IOException {
    int value = readEliasGammaCodingPositiveInteger();
    return (value - 1);
  }

  public int readEliasGammaCodingPositiveInteger() throws IOException {
    int value;
    int counter = 0;
    int bit = readBit();
    while (bit == 0) {
      counter++;
      bit = readBit();
    }
    value = 1;
    for (int i = 0; i < counter; i++) {
      value = (2 * value) + readBit();
    }
    return value;
  }
}