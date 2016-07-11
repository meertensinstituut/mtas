package mtas.codec.payload;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.TreeSet;

import mtas.analysis.token.MtasPosition;
import mtas.analysis.token.MtasToken;
import mtas.analysis.token.MtasTokenString;

import org.apache.lucene.util.BytesRef;

/**
 * The Class MtasPayloadEncoder encodes position, (real)offset, parent and
 * original payload into a new payload. <br>
 * <b>Initial bits</b><br>
 * <ul>
 * <li>bit 0,1 describe position type:
 * <ul>
 * <li>00 is single</li>
 * <li>10 is range</li>
 * <li>01 is set</li>
 * <li>11 is reserved (sub-position?)</li>
 * </ul>
 * </li>
 * <li>bit 2 describes offset:
 * <ul>
 * <li>0 is no offset</li>
 * <li>1 is offset
 * </ul>
 * </li>
 * <li>bit 3 describes real offset:
 * <ul>
 * <li>0 is follow offset</li>
 * <li>1 is separate real offset</li>
 * </ul>
 * </li>
 * <li>bit 4 describes parent:
 * <ul>
 * <li>0 is no parent</li>
 * <li>1 is parent</li>
 * </ul>
 * </li>
 * <li>bit 5 describes payload:
 * <ul>
 * <li>0 is no payload</li>
 * <li>1 is payload</li>
 * </ul>
 * </li>
 * </ul>
 * 
 * <b>Following bits</b><br>
 * 
 * <ul>
 * <li>id: add 1 and use Elias Gamma Coding, see {@link mtas.codec.payload.MtasBitOutputStream#writeEliasGammaCodingNonNegativeInteger(int)}</li>
 * <li>if range-position:
 * <ul>
 * <li>range-length: use Elias Gamma Coding, see {@link mtas.codec.payload.MtasBitOutputStream#writeEliasGammaCodingPositiveInteger(int)}</li>
 * </ul>
 * </li>
 * <li>if set-position:
 * <ul>
 * <li>number of positions: use Elias Gamma Coding, see {@link mtas.codec.payload.MtasBitOutputStream#writeEliasGammaCodingPositiveInteger(int)}</li>
 * <li>increments: use Elias Gamma Coding, see {@link mtas.codec.payload.MtasBitOutputStream#writeEliasGammaCodingPositiveInteger(int)}</li>
 * </ul>
 * </li>
 * <li>if offset:
 * <ul>
 * <li>startOffset: add 1, use Elias Gamma Coding, see {@link mtas.codec.payload.MtasBitOutputStream#writeEliasGammaCodingNonNegativeInteger(int)}</li>
 * <li>length: use Elias Gamma Coding, see {@link mtas.codec.payload.MtasBitOutputStream#writeEliasGammaCodingPositiveInteger(int)}</li> 
 * </ul>
 * </li>
 * 
 * <li>if realoffset:
 * <ul>
 * <li>if offset available:
 * <ul>
 * <li>difference of startRealOffset with startOffset: generalize for negative values, use Elias Gamma Coding, see {@link mtas.codec.payload.MtasBitOutputStream#writeEliasGammaCodingInteger(int)}</li>
 * <li>length: use Elias Gamma Coding, see {@link mtas.codec.payload.MtasBitOutputStream#writeEliasGammaCodingPositiveInteger(int)}</li>
 * </ul>
 * </li>
 * <li>if no offset available:
 * <ul>
 * <li>startOffset: add 1, use Elias Gamma Coding, see {@link mtas.codec.payload.MtasBitOutputStream#writeEliasGammaCodingNonNegativeInteger(int)}</li>
 * <li>length: use Elias Gamma Coding, see {@link mtas.codec.payload.MtasBitOutputStream#writeEliasGammaCodingPositiveInteger(int)}</li>
 * </ul>
 * </li>
 * </ul>
 * </li>
 * 
 * <li>if parent: 
 * <ul>
 * <li>increment relative from id to parent_id:
 * generalize for negative values, use Elias Gamma Coding, see {@link mtas.codec.payload.MtasBitOutputStream#writeEliasGammaCodingInteger(int)}</li>
 * </ul>
 * </li>
 * </ul>
 *
 *<b>Finally</b><br>
 *<ul>
 * <li>add minimal number of bits
 * to get multiple of 8 bits</li> 
 * <li>if payload: add payload bytes</li>
 *</ul>
 *
 */
public class MtasPayloadEncoder {

  /** The mtas token. */
  private MtasToken<?> mtasToken;

  /** The byte stream. */
  private MtasBitOutputStream byteStream;

  /** The encoding flags. */
  private int encodingFlags;

  /** The encode payload. */
  public static int ENCODE_PAYLOAD = 1;

  /** The encode offset. */
  public static int ENCODE_OFFSET = 2;

  /** The encode realoffset. */
  public static int ENCODE_REALOFFSET = 4;

  /** The encode parent. */
  public static int ENCODE_PARENT = 8;

  /** The encode default. */
  public static int ENCODE_DEFAULT = ENCODE_PAYLOAD | ENCODE_OFFSET
      | ENCODE_PARENT;

  /** The encode all. */
  public static int ENCODE_ALL = ENCODE_PAYLOAD | ENCODE_OFFSET
      | ENCODE_REALOFFSET | ENCODE_PARENT;

  /**
   * Instantiates a new mtas payload encoder.
   *
   * @param token
   *          the token
   * @param flags
   *          the flags
   */
  public MtasPayloadEncoder(MtasToken<?> token, int flags) {
    mtasToken = token;
    byteStream = new MtasBitOutputStream();
    encodingFlags = flags;
  }

  /**
   * Instantiates a new mtas payload encoder.
   *
   * @param token
   *          the token
   */
  public MtasPayloadEncoder(MtasToken<?> token) {
    this(token, ENCODE_DEFAULT);
  }

  /**
   * Gets the payload.
   *
   * @return the payload
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public BytesRef getPayload() throws IOException {

    // initial bits - position
    if (mtasToken.checkPositionType(MtasPosition.POSITION_SINGLE)) {
      byteStream.writeBit(0);
      byteStream.writeBit(0);
    } else if (mtasToken.checkPositionType(MtasPosition.POSITION_RANGE)) {
      byteStream.writeBit(1);
      byteStream.writeBit(0);
    } else if (mtasToken.checkPositionType(MtasPosition.POSITION_SET)) {
      byteStream.writeBit(0);
      byteStream.writeBit(1);
    } else {
      byteStream.writeBit(1);
      byteStream.writeBit(1);
    }
    // initial bits - offset
    if ((encodingFlags & ENCODE_OFFSET) == ENCODE_OFFSET
        && mtasToken.checkOffset()) {
      byteStream.writeBit(1);
    } else {
      byteStream.writeBit(0);
    }
    // initial bits - realOffset
    if ((encodingFlags & ENCODE_REALOFFSET) == ENCODE_REALOFFSET
        && mtasToken.checkRealOffset()) {
      byteStream.writeBit(1);
    } else {
      byteStream.writeBit(0);
    }
    // initial bits - parentId
    if ((encodingFlags & ENCODE_PARENT) == ENCODE_PARENT
        && mtasToken.checkParentId()) {
      byteStream.writeBit(1);
    } else {
      byteStream.writeBit(0);
    }
    // initial bits - original payload
    if ((encodingFlags & ENCODE_PAYLOAD) == ENCODE_PAYLOAD
        && mtasToken.getPayload() != null) {
      byteStream.writeBit(1);
    } else {
      byteStream.writeBit(0);
    }
    if (mtasToken.getType().equals(MtasTokenString.TOKEN_TYPE)) {
      byteStream.writeBit(0);
    } else {
      // to add other token types later on
      byteStream.writeBit(1);
    }
    // add id (EliasGammaCoding)
    byteStream.writeEliasGammaCodingNonNegativeInteger(mtasToken.getId());
    // add position info (EliasGammaCoding)
    if (mtasToken.checkPositionType(MtasPosition.POSITION_SINGLE)) {
      // do nothing
    } else if (mtasToken.checkPositionType(MtasPosition.POSITION_RANGE)) {
      // write length
      byteStream.writeEliasGammaCodingPositiveInteger(1
          + mtasToken.getPositionEnd() - mtasToken.getPositionStart());
    } else if (mtasToken.checkPositionType(MtasPosition.POSITION_SET)) {
      // write number of positions
      TreeSet<Integer> positionList = mtasToken.getPositions();
      byteStream.writeEliasGammaCodingPositiveInteger(positionList.size());
      int previousPosition = positionList.first();
      Iterator<Integer> it = positionList.iterator();
      if (it.hasNext()) {
        it.next(); // skip start position
        while (it.hasNext()) {
          int currentPosition = it.next();
          byteStream.writeEliasGammaCodingPositiveInteger(currentPosition
              - previousPosition);
          previousPosition = currentPosition;
        }
      }
    } else {
      // do nothing
    }
    // add offset info (EliasGammaCoding)
    if ((encodingFlags & ENCODE_OFFSET) == ENCODE_OFFSET
        && mtasToken.checkOffset()) {
      byteStream.writeEliasGammaCodingNonNegativeInteger(mtasToken
          .getOffsetStart());
      byteStream.writeEliasGammaCodingPositiveInteger(1
          + mtasToken.getOffsetEnd() - mtasToken.getOffsetStart());
    }
    // add realOffset info (EliasGammaCoding)
    if ((encodingFlags & ENCODE_REALOFFSET) == ENCODE_REALOFFSET
        && mtasToken.checkRealOffset()) {
      if ((encodingFlags & ENCODE_OFFSET) == ENCODE_OFFSET
          && mtasToken.checkOffset()) {
        byteStream.writeEliasGammaCodingInteger(mtasToken.getRealOffsetStart()
            - mtasToken.getOffsetStart());
        byteStream.writeEliasGammaCodingPositiveInteger(1
            + mtasToken.getRealOffsetEnd() - mtasToken.getRealOffsetStart());
      } else {
        byteStream.writeEliasGammaCodingNonNegativeInteger(mtasToken
            .getRealOffsetStart());
        byteStream.writeEliasGammaCodingPositiveInteger(1
            + mtasToken.getRealOffsetEnd() - mtasToken.getRealOffsetStart());
      }
    }
    // add parent info (EliasGammaCoding)
    if ((encodingFlags & ENCODE_PARENT) == ENCODE_PARENT
        && mtasToken.checkParentId()) {
      byteStream.writeEliasGammaCodingInteger(mtasToken.getParentId()
          - mtasToken.getId());
    }
    // add minimal number of zero-bits to get round number of bytes
    byteStream.createByte();
    // finally add original payload bytes
    if ((encodingFlags & ENCODE_PAYLOAD) == ENCODE_PAYLOAD
        && mtasToken.getPayload() != null) {
      BytesRef payload = mtasToken.getPayload();
      byteStream.write(Arrays.copyOfRange(payload.bytes, payload.offset,
          (payload.offset + payload.length)));
    }
    // construct new payload
    return new BytesRef(byteStream.toByteArray());
  }

}
