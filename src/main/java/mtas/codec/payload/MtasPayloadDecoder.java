package mtas.codec.payload;

import mtas.analysis.token.MtasOffset;
import mtas.analysis.token.MtasPosition;

import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

public class MtasPayloadDecoder {
  private MtasBitInputStream byteStream;
  private MtasPosition mtasPosition;
  private int mtasStartPosition;
  private SortedSet<Integer> mtasPositions;
  private Integer mtasId = null;
  private byte[] mtasPayloadValue = null;
  private Integer mtasParentId = null;
  private Boolean mtasPayload = null;
  private Boolean mtasParent = null;
  private String mtasPositionType = null;
  private MtasOffset mtasOffset;
  private MtasOffset mtasRealOffset;

  public void init(int startPosition, byte[] payload) throws IOException {
    byteStream = new MtasBitInputStream(payload);
    mtasStartPosition = startPosition;
    // analyse initial bits - position
    Boolean getOffset;
    Boolean getRealOffset;
    if (byteStream.readBit() == 1) {
      if (byteStream.readBit() == 1) {
        mtasPositionType = null;
      } else {
        mtasPositionType = MtasPosition.POSITION_RANGE;
      }
    } else {
      if (byteStream.readBit() == 1) {
        mtasPositionType = MtasPosition.POSITION_SET;
      } else {
        mtasPositionType = MtasPosition.POSITION_SINGLE;
      }
    }
    // analyze initial bits - offset
    getOffset = byteStream.readBit() == 1;
    // analyze initial bits - realOffset
    getRealOffset = byteStream.readBit() == 1;
    // analyze initial bits - parent
    mtasParent = byteStream.readBit() == 1;
    // analyse initial bits - payload
    mtasPayload = byteStream.readBit() == 1;
    if (byteStream.readBit() == 0) {
      // string
    } else {
      // other
    }
    // get id
    mtasId = byteStream.readEliasGammaCodingNonNegativeInteger();
    // get position info
    if (mtasPositionType != null
        && mtasPositionType.equals(MtasPosition.POSITION_SINGLE)) {
      mtasPosition = new MtasPosition(mtasStartPosition);
    } else if (mtasPositionType != null
        && mtasPositionType.equals(MtasPosition.POSITION_RANGE)) {
      mtasPosition = new MtasPosition(mtasStartPosition, (mtasStartPosition
          + byteStream.readEliasGammaCodingPositiveInteger() - 1));
    } else if (mtasPositionType != null
        && mtasPositionType.equals(MtasPosition.POSITION_SET)) {
      mtasPositions = new TreeSet<>();
      mtasPositions.add(mtasStartPosition);
      int numberOfPoints = byteStream.readEliasGammaCodingPositiveInteger();
      int[] positionList = new int[numberOfPoints];
      positionList[0] = mtasStartPosition;
      int previousPosition = 0;
      int currentPosition = mtasStartPosition;
      for (int i = 1; i < numberOfPoints; i++) {
        previousPosition = currentPosition;
        currentPosition = previousPosition
            + byteStream.readEliasGammaCodingPositiveInteger();
        positionList[i] = currentPosition;
      }
      mtasPosition = new MtasPosition(positionList);
    } else {
      mtasPosition = null;
    }
    // get offset and realOffset info
    if (getOffset) {
      int offsetStart = byteStream.readEliasGammaCodingNonNegativeInteger();
      int offsetEnd = offsetStart
          + byteStream.readEliasGammaCodingPositiveInteger() - 1;
      mtasOffset = new MtasOffset(offsetStart, offsetEnd);
      if (getRealOffset) {
        int realOffsetStart = byteStream.readEliasGammaCodingInteger()
            + offsetStart;
        int realOffsetEnd = realOffsetStart
            + byteStream.readEliasGammaCodingPositiveInteger() - 1;
        mtasRealOffset = new MtasOffset(realOffsetStart, realOffsetEnd);
      }
    } else if (getRealOffset) {
      int realOffsetStart = byteStream.readEliasGammaCodingNonNegativeInteger();
      int realOffsetEnd = realOffsetStart
          + byteStream.readEliasGammaCodingPositiveInteger() - 1;
      mtasRealOffset = new MtasOffset(realOffsetStart, realOffsetEnd);
    }
    if (mtasParent) {
      mtasParentId = byteStream.readEliasGammaCodingInteger() + mtasId;
    }
    if (mtasPayload) {
      mtasPayloadValue = byteStream.readRemainingBytes();
    }
  }

  public Integer getMtasId() {
    return mtasId;
  }

  public Integer getMtasParentId() {
    return mtasParentId;
  }

  public byte[] getMtasPayload() {
    return mtasPayload ? mtasPayloadValue : null;
  }

  public MtasPosition getMtasPosition() {
    return mtasPosition;
  }

  public MtasOffset getMtasOffset() {
    return mtasOffset;
  }

  public MtasOffset getMtasRealOffset() {
    return mtasRealOffset;
  }
}
