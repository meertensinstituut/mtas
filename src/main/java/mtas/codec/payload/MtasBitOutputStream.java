package mtas.codec.payload;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class MtasBitOutputStream extends ByteArrayOutputStream {
  private int bitBuffer = 0;
  private int bitCount = 0;

  public MtasBitOutputStream() {
  }

  public void writeBit(int value) throws IOException {
    writeBit(value, 1);
  }

  public void writeBit(int value, int number) throws IOException {
    int localNumber = number;
    while (localNumber > 0) {
      localNumber--;
      bitBuffer |= ((value & 1) << bitCount++);
      if (bitCount == 8) {
        createByte();
      }
    }
  }

  public void writeEliasGammaCodingInteger(int value) throws IOException {
    if (value >= 0) {
      writeEliasGammaCodingPositiveInteger(2 * value + 1);
    } else {
      writeEliasGammaCodingPositiveInteger(-2 * value);
    }
  }

  public void writeEliasGammaCodingNonNegativeInteger(int value)
      throws IOException {
    if (value >= 0) {
      writeEliasGammaCodingPositiveInteger(value + 1);
    }
  }

  public void writeEliasGammaCodingPositiveInteger(int value)
      throws IOException {
    if (value > 0) {
      if (value == 1) {
        writeBit(1);
      } else {
        writeBit(0);
        writeEliasGammaCodingPositiveInteger(value / 2);
        writeBit(value % 2);
      }
    }
  }

  @Override
  public void close() throws IOException {
    createByte();
    super.close();
  }

  public void createByte() throws IOException {
    if (bitCount > 0) {
      bitCount = 0;
      write(bitBuffer);
      bitBuffer = 0;
    }
  }
}
