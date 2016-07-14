package mtas.codec.util;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.TreeMap;
import java.util.regex.Pattern;

import mtas.analysis.token.MtasToken;
import mtas.codec.MtasCodecPostingsFormat;
import mtas.codec.tree.IntervalTreeNodeData;
import mtas.codec.util.CodecComponent.ComponentFacet;
import mtas.codec.util.CodecComponent.ComponentField;
import mtas.codec.util.CodecComponent.ComponentGroup;
import mtas.codec.util.CodecComponent.ComponentKwic;
import mtas.codec.util.CodecComponent.ComponentList;
import mtas.codec.util.CodecComponent.ComponentPosition;
import mtas.codec.util.CodecComponent.ComponentSpan;
import mtas.codec.util.CodecComponent.ComponentTermVector;
import mtas.codec.util.CodecComponent.ComponentToken;
import mtas.codec.util.CodecComponent.GroupHit;
import mtas.codec.util.CodecComponent.KwicHit;
import mtas.codec.util.CodecComponent.KwicToken;
import mtas.codec.util.CodecComponent.ListHit;
import mtas.codec.util.CodecComponent.ListToken;
import mtas.codec.util.CodecComponent.Match;
import mtas.codec.util.DataCollector.MtasDataCollector;
import mtas.codec.util.CodecInfo.IndexDoc;
import mtas.codec.util.CodecSearchTree.MtasTreeHit;

import org.apache.commons.lang.ArrayUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanWeight;
import org.apache.lucene.search.spans.Spans;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LegacyNumericUtils;

/**
 * The Class CodecCollector.
 */
public class CodecCollector {
  
  /**
   * Collect.
   *
   * @param field the field
   * @param searcher the searcher
   * @param reader the reader
   * @param rawReader the raw reader
   * @param fullDocList the full doc list
   * @param fullDocSet the full doc set
   * @param fieldInfo the field info
   * @param spansQueryWeight the spans query weight
   * @throws IllegalAccessException the illegal access exception
   * @throws IllegalArgumentException the illegal argument exception
   * @throws InvocationTargetException the invocation target exception
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public static void collect(String field, IndexSearcher searcher,
      IndexReader reader, IndexReader rawReader, ArrayList<Integer> fullDocList,
      ArrayList<Integer> fullDocSet, ComponentField fieldInfo,
      HashMap<SpanQuery, SpanWeight> spansQueryWeight)
      throws IllegalAccessException, IllegalArgumentException,
      InvocationTargetException, IOException {

    ListIterator<LeafReaderContext> iterator = reader.leaves().listIterator();
    while (iterator.hasNext()) {
      LeafReaderContext lrc = iterator.next();
      LeafReader r = lrc.reader();

      // compute relevant docSet/docList
      List<Integer> docSet = null;
      List<Integer> docList = null;
      if (fullDocSet != null) {
        docSet = new ArrayList<Integer>();
        Iterator<Integer> docSetIterator = fullDocSet.iterator();
        Integer docSetId = null;
        while (docSetIterator.hasNext()) {
          docSetId = docSetIterator.next();
          if ((docSetId >= lrc.docBase)
              && (docSetId < lrc.docBase + lrc.reader().maxDoc())) {
            docSet.add(docSetId);
          }
        }
        Collections.sort(docSet);
      }
      if (fullDocList != null) {
        docList = new ArrayList<Integer>();
        Iterator<Integer> docListIterator = fullDocList.iterator();
        Integer docListId = null;
        while (docListIterator.hasNext()) {
          docListId = docListIterator.next();
          if ((docListId >= lrc.docBase)
              && (docListId < lrc.docBase + lrc.reader().maxDoc())) {
            docList.add(docListId);
          }
        }
        Collections.sort(docList);
      }

      Terms t = rawReader.leaves().get(lrc.ord).reader().terms(field);
      CodecInfo mtasCodecInfo = t == null ? null
          : CodecInfo.getCodecInfoFromTerms(t);

      collectSpansPositionsAndTokens(spansQueryWeight, searcher, mtasCodecInfo, r,
          lrc, field, t, docSet, docList, fieldInfo);
      collectPrefixes(rawReader.leaves().get(lrc.ord).reader().getFieldInfos(),
          field, fieldInfo);      
    }
    
  }

  /**
   * Collect spans positions and tokens.
   *
   * @param spansQueryWeight the spans query weight
   * @param searcher the searcher
   * @param mtasCodecInfo the mtas codec info
   * @param r the r
   * @param lrc the lrc
   * @param field the field
   * @param t the t
   * @param docSet the doc set
   * @param docList the doc list
   * @param fieldInfo the field info
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private static void collectSpansPositionsAndTokens(
      HashMap<SpanQuery, SpanWeight> spansQueryWeight, IndexSearcher searcher,
      CodecInfo mtasCodecInfo, LeafReader r, LeafReaderContext lrc,
      String field, Terms t, List<Integer> docSet, List<Integer> docList,
      ComponentField fieldInfo) throws IOException {

    boolean needSpans = false;
    boolean needPositions = false;
    boolean needTokens = false;
    
    // results
    HashMap<Integer, Integer> positionsData = null;
    HashMap<Integer, Integer> tokensData = null;
    HashMap<SpanQuery, HashMap<Integer, Integer>> spansNumberData = null;
    HashMap<SpanQuery, HashMap<Integer, ArrayList<Match>>> spansMatchData = null;
    HashMap<String, TreeMap<String, int[]>> facetData = null;
    HashMap<String, String> facetDataType = null;

    // collect position stats
    if (fieldInfo.statsPositionList.size() > 0) {
      needPositions = true;
    }
    // collect token stats
    if (fieldInfo.statsTokenList.size() > 0) {
      needTokens = true;
    }
    if (fieldInfo.termVectorList.size() > 0) {
      for (ComponentTermVector ctv : fieldInfo.termVectorList) {
        needPositions = !needPositions ? ctv.functionParser.needPositions()
            : needPositions;
      }
    }

    // compute from spans for selected docs
    if (fieldInfo.spanQueryList.size() > 0) {
      // check for statsSpans
      spansNumberData = new HashMap<SpanQuery, HashMap<Integer, Integer>>();
      spansMatchData = new HashMap<SpanQuery, HashMap<Integer, ArrayList<Match>>>();
      facetData = new HashMap<String, TreeMap<String, int[]>>();
      facetDataType = new HashMap<String, String>();
      // spans
      if (fieldInfo.statsSpanList.size() > 0) {
        for (ComponentSpan cs : fieldInfo.statsSpanList) {
          needPositions = (!needPositions) ? cs.functionParser.needPositions()
              : needPositions;
          needSpans = (!needSpans) ? cs.functionParser.needArgumentsNumber() > 0
              : needSpans;
          Integer[] arguments = cs.functionParser.needArguments();
          for (int a : arguments) {
            if (cs.queries.length > a) {
              SpanQuery q = cs.queries[a];
              if (!spansNumberData.containsKey(q)) {
                spansNumberData.put(q, new HashMap<Integer, Integer>());
              }
            }
          }
        }
      }
      // kwic
      if (fieldInfo.kwicList.size() > 0) {
        needSpans = true;
        for (ComponentKwic ck : fieldInfo.kwicList) {
          if (!spansMatchData.containsKey(ck.query)) {
            spansMatchData.put(ck.query,
                new HashMap<Integer, ArrayList<Match>>());
          }
        }
      }
      // list
      if (fieldInfo.listList.size() > 0) {
        needSpans = true;
        for (ComponentList cl : fieldInfo.listList) {
          if (!spansMatchData.containsKey(cl.spanQuery)) {
            if (cl.number > 0) {
              // only if needed
              if (cl.position < (cl.start + cl.number)) {
                spansMatchData.put(cl.spanQuery,
                    new HashMap<Integer, ArrayList<Match>>());
              } else {
                spansNumberData.put(cl.spanQuery,
                    new HashMap<Integer, Integer>());
              }
            } else if (!spansNumberData.containsKey(cl.spanQuery)) {
              spansNumberData.put(cl.spanQuery,
                  new HashMap<Integer, Integer>());
            }
          }
        }
      }
      // group
      if (fieldInfo.groupList.size() > 0) {
        needSpans = true;
        for (ComponentGroup cg : fieldInfo.groupList) {
          if (!spansMatchData.containsKey(cg.spanQuery)) {
            spansMatchData.put(cg.spanQuery,
                new HashMap<Integer, ArrayList<Match>>());
          }
        }
      }
      // facet
      if (fieldInfo.facetList.size() > 0) {
        for (ComponentFacet cf : fieldInfo.facetList) {
          for (int i = 0; i < cf.baseFields.length; i++) {
            needPositions = (!needPositions)
                ? cf.baseFunctionParsers[i].needPositions() : needPositions;
            needSpans = (!needSpans)
                ? cf.baseFunctionParsers[i].needArgumentsNumber() > 0
                : needSpans;
            Integer[] arguments = cf.baseFunctionParsers[i].needArguments();
            for (int a : arguments) {
              if (cf.spanQueries.length > a) {
                SpanQuery q = cf.spanQueries[a];
                if (!spansNumberData.containsKey(q)) {
                  spansNumberData.put(q, new HashMap<Integer, Integer>());
                }
              }
            }
            if (!facetData.containsKey(cf.baseFields[i])) {
              facetData.put(cf.baseFields[i], new TreeMap<String, int[]>());
              facetDataType.put(cf.baseFields[i], cf.baseFieldTypes[i]);
            }
          }
        }
      }
      // termvector
      if (fieldInfo.termVectorList.size() > 0) {
        for (ComponentTermVector ctv : fieldInfo.termVectorList) {
          if (ctv.functionParser.needPositions()) {
            needPositions = true;
          }
        }
      }
    }

    if (needSpans) {
      HashMap<Integer, Integer> numberData;
      HashMap<Integer, ArrayList<Match>> matchData;

      // collect values for facetFields
      for (String facetField : facetData.keySet()) {
        Terms fft = r.terms(facetField);
        if (fft != null) {
          TermsEnum termsEnum = fft.iterator();
          BytesRef term = null;
          PostingsEnum postingsEnum = null;
          TreeMap<String, int[]> facetDataList = facetData.get(facetField);
          while ((term = termsEnum.next()) != null) {
            // TODO: check for strange int/doubles etc : skip
            int docId, termDocId = -1;
            int[] facetDataSublist = new int[docSet.size()];
            int facetDataSublistCounter = 0;
            Iterator<Integer> docIterator = docSet.iterator();
            postingsEnum = termsEnum.postings(postingsEnum);
            while (docIterator.hasNext()) {
              docId = docIterator.next() - lrc.docBase;
              if (docId >= termDocId) {
                if ((docId == termDocId)
                    || ((termDocId = postingsEnum.advance(docId)) == docId)) {
                  facetDataSublist[facetDataSublistCounter] = docId
                      + lrc.docBase;
                  facetDataSublistCounter++;
                }
              }
            }
            if (facetDataSublistCounter > 0) {
              String termValue = null;
              if (facetDataType.get(facetField)
                  .equals(ComponentFacet.TYPE_INTEGER)) {
                // only values without shifting bits
                if (term.bytes[term.offset] == LegacyNumericUtils.SHIFT_START_INT) {
                  termValue = Integer
                      .toString(LegacyNumericUtils.prefixCodedToInt(term));
                } else {
                  continue;
                }
              } else if (facetDataType.get(facetField)
                  .equals(ComponentFacet.TYPE_LONG)) {
                if (term.bytes[term.offset] == LegacyNumericUtils.SHIFT_START_LONG) {
                  termValue = Long
                      .toString(LegacyNumericUtils.prefixCodedToLong(term));
                } else {
                  continue;
                }
              } else {
                termValue = term.utf8ToString();
              }
              if (!facetDataList.containsKey(termValue)) {
                facetDataList.put(termValue,
                    Arrays.copyOf(facetDataSublist, facetDataSublistCounter));
              } else {
                int[] oldList = facetDataList.get(termValue);
                int[] newList = new int[oldList.length
                    + facetDataSublistCounter];
                System.arraycopy(oldList, 0, newList, 0, oldList.length);
                System.arraycopy(facetDataSublist, 0, newList, oldList.length,
                    facetDataSublistCounter);
                facetDataList.put(termValue, newList);
              }
            }
          }
        }
      }

      for (SpanQuery sq : fieldInfo.spanQueryList) {
        // what to collect
        if (spansNumberData.containsKey(sq)) {
          numberData = spansNumberData.get(sq);
        } else {
          numberData = null;
        }
        if (spansMatchData.containsKey(sq)) {
          matchData = spansMatchData.get(sq);
        } else {
          matchData = null;
        }
        if ((numberData != null) || (matchData != null)) {
          Spans spans = spansQueryWeight.get(sq).getSpans(lrc,
              SpanWeight.Postings.POSITIONS);
          if (spans != null) {
            Iterator<Integer> it;
            if (docSet != null) {
              it = docSet.iterator();
            } else {
              it = docList.iterator();
            }
            if (it.hasNext()) {
              int docId = it.next();
              int number;
              ArrayList<Match> matchDataList;
              Integer spansDocId = null;
              while (docId != DocIdSetIterator.NO_MORE_DOCS) {
                if (spans.advance(
                    (docId - lrc.docBase)) == DocIdSetIterator.NO_MORE_DOCS) {
                  break;
                }
                spansDocId = spans.docID() + lrc.docBase;
                while ((docId < spansDocId) && it.hasNext()) {
                  docId = it.next();
                }
                if (docId < spansDocId) {
                  break;
                }
                if (spansDocId.equals(docId)) {
                  number = 0;
                  matchDataList = new ArrayList<Match>();
                  while (spans.nextStartPosition() != Spans.NO_MORE_POSITIONS) {
                    number++;
                    if (matchData != null) {
                      Match m = new Match(spans.startPosition(),
                          spans.endPosition());
                      matchDataList.add(m);
                    }
                  }
                  if ((numberData != null)) {
                    numberData.put(spansDocId, number);
                  }
                  if ((matchData != null)) {
                    matchData.put(spansDocId, matchDataList);
                  }
                  if (it.hasNext()) {
                    docId = it.next();
                  } else {
                    break;
                  }
                }
              }
            }
          }
        }
      }
    }

    // collect position stats
    if (needPositions) {
      if (mtasCodecInfo != null) {
        // for relatively small numbers, compute only what is needed
        if (docSet.size() < Math.log(r.maxDoc())) {
          positionsData = new HashMap<Integer, Integer>();
          for (int docId : docSet) {
            positionsData.put(docId,
                mtasCodecInfo.getNumberOfPositions(field, (docId - lrc.docBase)));            
          }
          // compute everything, only use what is needed
        } else {
          positionsData = mtasCodecInfo.getAllNumberOfPositions(field,
              lrc.docBase);
          for (int docId : docSet) {
            if (!positionsData.containsKey(docId)) {
              positionsData.put(docId, 0);
            }
          }
        }
      } else {
        positionsData = new HashMap<Integer, Integer>();
        for (int docId : docSet) {
          positionsData.put(docId, 0);
        }
      }
    }
    
    // collect token stats
    if (needTokens) {
      if (mtasCodecInfo != null) {
        // for relatively small numbers, compute only what is needed
        if (docSet.size() < Math.log(r.maxDoc())) {
          tokensData = new HashMap<Integer, Integer>();
          for (int docId : docSet) {
            tokensData.put(docId,
                mtasCodecInfo.getNumberOfTokens(field, (docId - lrc.docBase)));            
          }
          // compute everything, only use what is needed
        } else {
          tokensData = mtasCodecInfo.getAllNumberOfTokens(field,
              lrc.docBase);
          for (int docId : docSet) {
            if (!tokensData.containsKey(docId)) {
              tokensData.put(docId, 0);
            }
          }
        }
      } else {
        tokensData = new HashMap<Integer, Integer>();
        for (int docId : docSet) {
          tokensData.put(docId, 0);
        }
      }
    }
    
    if (fieldInfo.statsPositionList.size() > 0) {
      // create positions
      createPositions(fieldInfo.statsPositionList, positionsData, docSet);
    }
    
    if (fieldInfo.statsTokenList.size() > 0) {
      // create positions
      createTokens(fieldInfo.statsTokenList, tokensData, docSet);
    }

    if (fieldInfo.spanQueryList.size() > 0) {
      if (fieldInfo.statsSpanList.size() > 0) {
        // create stats
        createStats(fieldInfo.statsSpanList, positionsData, spansNumberData,
            docSet.toArray(new Integer[docSet.size()]));
      }
      if (fieldInfo.listList.size() > 0) {
        // create list
        createList(fieldInfo.listList, spansNumberData, spansMatchData, docSet,
            field, lrc.docBase, fieldInfo.uniqueKeyField, mtasCodecInfo,
            searcher);
      }
      if (fieldInfo.groupList.size() > 0) {
        // create group
        createGroup(fieldInfo.groupList, spansMatchData, docSet, field,
            lrc.docBase, mtasCodecInfo);
      }
      if (fieldInfo.kwicList.size() > 0) {
        // create kwic
        createKwic(fieldInfo.kwicList, spansMatchData, docList, field,
            lrc.docBase, fieldInfo.uniqueKeyField, mtasCodecInfo, searcher);
      }
      if (fieldInfo.facetList.size() > 0) {
        // create facets
        createFacet(fieldInfo.facetList, positionsData, spansNumberData,
            facetData, docSet, field, lrc.docBase, fieldInfo.uniqueKeyField,
            mtasCodecInfo, searcher);
      }
    }
    if (fieldInfo.termVectorList.size() > 0) {
      createTermvector(fieldInfo.termVectorList, positionsData, docSet, field,
          t, r, lrc);
    }
  }

  /**
   * Collect prefixes.
   *
   * @param fieldInfos the field infos
   * @param field the field
   * @param fieldInfo the field info
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private static void collectPrefixes(FieldInfos fieldInfos, String field,
      ComponentField fieldInfo) throws IOException {
    if (fieldInfo.prefix != null) {
      FieldInfo fi = fieldInfos.fieldInfo(field);
      if (fi != null) {
        String singlePositionPrefixes = fi.getAttribute(
            MtasCodecPostingsFormat.MTAS_FIELDINFO_ATTRIBUTE_PREFIX_SINGLE_POSITION);
        String multiplePositionPrefixes = fi.getAttribute(
            MtasCodecPostingsFormat.MTAS_FIELDINFO_ATTRIBUTE_PREFIX_MULTIPLE_POSITION);
        if (singlePositionPrefixes != null) {
          String[] prefixes = singlePositionPrefixes
              .split(Pattern.quote(MtasToken.DELIMITER));
          for (int i = 0; i < prefixes.length; i++) {
            fieldInfo.prefix.addSinglePosition(prefixes[i]);
          }
        }
        if (multiplePositionPrefixes != null) {
          String[] prefixes = multiplePositionPrefixes
              .split(Pattern.quote(MtasToken.DELIMITER));
          for (int i = 0; i < prefixes.length; i++) {
            fieldInfo.prefix.addMultiplePosition(prefixes[i]);
          }
        }
      }
    }
  }
  

  /**
   * Compute arguments.
   *
   * @param spansNumberData the spans number data
   * @param queries the queries
   * @param docSet the doc set
   * @return the hash map
   */
  private static HashMap<Integer, long[]> computeArguments(
      HashMap<SpanQuery, HashMap<Integer, Integer>> spansNumberData,
      SpanQuery[] queries, Integer[] docSet) {
    HashMap<Integer, long[]> args = new HashMap<Integer, long[]>();
    for (int q = 0; q < queries.length; q++) {
      HashMap<Integer, Integer> tmpData = spansNumberData.get(queries[q]);
      long[] tmpList = null;
      for (int docId : docSet) {
        if (tmpData != null && tmpData.containsKey(docId)) {
          if (!args.containsKey(docId)) {
            tmpList = new long[queries.length];
          } else {
            tmpList = args.get(docId);
          }
          tmpList[q] = tmpData.get(docId);
          args.put(docId, tmpList);
        } else if (!args.containsKey(docId)) {
          tmpList = new long[queries.length];
          args.put(docId, tmpList);
        }
      }
    }
    return args;
  }

  /**
   * Intersected doc list.
   *
   * @param facetDocList the facet doc list
   * @param docSet the doc set
   * @return the integer[]
   */
  private static Integer[] intersectedDocList(int[] facetDocList,
      Integer[] docSet) {
    if (facetDocList != null) {
      if (docSet != null) {
        Integer[] c = new Integer[Math.min(facetDocList.length, docSet.length)];
        int ai = 0, bi = 0, ci = 0;
        while (ai < facetDocList.length && bi < docSet.length) {
          if (facetDocList[ai] < docSet[bi]) {
            ai++;
          } else if (facetDocList[ai] > docSet[bi]) {
            bi++;
          } else {
            if (ci == 0 || facetDocList[ai] != c[ci - 1]) {
              c[ci++] = facetDocList[ai];
            }
            ai++;
            bi++;
          }
        }
        return Arrays.copyOfRange(c, 0, ci);
      }
    }
    return null;
  }

  /**
   * Creates the positions.
   *
   * @param statsPositionList the stats position list
   * @param positionsData the positions data
   * @param docSet the doc set
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private static void createPositions(List<ComponentPosition> statsPositionList,
      HashMap<Integer, Integer> positionsData, List<Integer> docSet)
      throws IOException {
    if (statsPositionList != null) {
      for (ComponentPosition position : statsPositionList) {
        position.dataCollector.initNewList(1);
        Integer tmpValue;
        long[] values = new long[docSet.size()];
        int value, number = 0;
        for (int docId : docSet) {
          tmpValue = positionsData.get(docId);
          value = tmpValue == null ? 0 : tmpValue.intValue();
          if (((position.minimumLong == null)
              || (value >= position.minimumLong))
              && ((position.maximumLong == null)
                  || (value <= position.maximumLong))) {
            values[number] = value;
            number++;
          }
        }
        if (number > 0) {
          position.dataCollector.add(values, number);
        }
        position.dataCollector.closeNewList();
      }
    }
  }

  /**
   * Creates the tokens.
   *
   * @param statsTokenList the stats token list
   * @param tokensData the tokens data
   * @param docSet the doc set
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private static void createTokens(List<ComponentToken> statsTokenList,
      HashMap<Integer, Integer> tokensData, List<Integer> docSet)
      throws IOException {
    if (statsTokenList != null) {
      for (ComponentToken token : statsTokenList) {
        token.dataCollector.initNewList(1);
        Integer tmpValue;
        long[] values = new long[docSet.size()];
        int value, number = 0;
        if(tokensData!=null) {
          for (int docId : docSet) {
            tmpValue = tokensData.get(docId);
            value = tmpValue == null ? 0 : tmpValue.intValue();
            if (((token.minimumLong == null)
                || (value >= token.minimumLong))
                && ((token.maximumLong == null)
                    || (value <= token.maximumLong))) {
              values[number] = value;
              number++;
            }
          }
        }  
        if (number > 0) {
          token.dataCollector.add(values, number);
        }
        token.dataCollector.closeNewList();
      }
    }
  }

  /**
   * Creates the stats.
   *
   * @param statsSpanList the stats span list
   * @param positionsData the positions data
   * @param spansNumberData the spans number data
   * @param docSet the doc set
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private static void createStats(List<ComponentSpan> statsSpanList,
      HashMap<Integer, Integer> positionsData,
      HashMap<SpanQuery, HashMap<Integer, Integer>> spansNumberData,
      Integer[] docSet) throws IOException {
    if (statsSpanList != null) {
      for (ComponentSpan span : statsSpanList) {
        if (span.functionParser.needArgumentsNumber() > span.queries.length) {
          throw new IOException(
              "function " + span.functionParser + " expects (at least) "
                  + span.functionParser.needArgumentsNumber() + " queries");
        }
        // collect
        HashMap<Integer, long[]> args = computeArguments(spansNumberData,
            span.queries, docSet);
        if (span.dataType.equals(CodecUtil.DATA_TYPE_LONG)
            || span.dataType.equals(CodecUtil.DATA_TYPE_DOUBLE)) {
          // try to call functionParser as little as possible
          if (span.statsType.equals(CodecUtil.STATS_BASIC)
              && span.functionParser.sumRule() && (span.minimumLong == null)
              && (span.maximumLong == null)) {
            // initialise
            int length = span.functionParser.needArgumentsNumber();
            long[] valueSum = new long[length];
            long valuePositions = 0;
            // collect
            if (docSet.length > 0) {
              long[] tmpArgs;
              span.dataCollector.initNewList(1);
              for (int docId : docSet) {
                tmpArgs = args.get(docId);
                valuePositions += (positionsData == null) ? 0
                    : positionsData.get(docId);
                if (tmpArgs != null) {
                  for (int i = 0; i < length; i++) {
                    valueSum[i] += tmpArgs[i];
                  }
                }
              }
              if (span.dataType.equals(CodecUtil.DATA_TYPE_LONG)) {
                long value;
                try {
                  value = span.functionParser.getValueLong(valueSum,
                      valuePositions);
                  span.dataCollector.add(value, docSet.length);
                } catch (IOException e) {
                  span.dataCollector.error(e.getMessage());
                }
              } else {
                double value;
                try {
                  value = span.functionParser.getValueDouble(valueSum,
                      valuePositions);
                  span.dataCollector.add(value, docSet.length);
                } catch (IOException e) {
                  span.dataCollector.error(e.getMessage());
                }
              }
              span.dataCollector.closeNewList();
            }
          } else if (span.dataType.equals(CodecUtil.DATA_TYPE_LONG)) {
            // collect
            if (docSet.length > 0) {
              int number = 0, positions;
              long value;
              long values[] = new long[docSet.length];
              span.dataCollector.initNewList(1);
              for (int docId : docSet) {
                positions = (positionsData == null) ? 0
                    : positionsData.get(docId);
                try {
                  value = span.functionParser.getValueLong(args.get(docId),
                      positions);
                  if (((span.minimumLong == null)
                      || (value >= span.minimumLong))
                      && ((span.maximumLong == null)
                          || (value <= span.maximumLong))) {
                    values[number] = value;
                    number++;
                  }
                } catch (IOException e) {
                  span.dataCollector.error(e.getMessage());
                }
              }
              if (number > 0) {
                span.dataCollector.add(values, number);
              }
              span.dataCollector.closeNewList();
            }
          } else if (span.dataType.equals(CodecUtil.DATA_TYPE_DOUBLE)) {
            // collect
            if (docSet.length > 0) {
              int number = 0, positions;
              double value;
              double values[] = new double[docSet.length];
              span.dataCollector.initNewList(1);
              for (int docId : docSet) {
                positions = (positionsData == null) ? 0
                    : positionsData.get(docId);
                try {
                  value = span.functionParser.getValueDouble(args.get(docId),
                      positions);
                  if (((span.minimumLong == null)
                      || (value >= span.minimumLong))
                      && ((span.maximumLong == null)
                          || (value <= span.maximumLong))) {
                    values[number] = value;
                    number++;
                  }
                } catch (IOException e) {
                  span.dataCollector.error(e.getMessage());
                }
              }
              if (number > 0) {
                span.dataCollector.add(values, number);
              }
              span.dataCollector.closeNewList();
            }
          }
        } else {
          throw new IOException("unknown dataType " + span.dataType);
        }
      }
    }
  }

  /**
   * Creates the list.
   *
   * @param listList the list list
   * @param spansNumberData the spans number data
   * @param spansMatchData the spans match data
   * @param docSet the doc set
   * @param field the field
   * @param docBase the doc base
   * @param uniqueKeyField the unique key field
   * @param mtasCodecInfo the mtas codec info
   * @param searcher the searcher
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private static void createList(List<ComponentList> listList,
      HashMap<SpanQuery, HashMap<Integer, Integer>> spansNumberData,
      HashMap<SpanQuery, HashMap<Integer, ArrayList<Match>>> spansMatchData,
      List<Integer> docSet, String field, int docBase, String uniqueKeyField,
      CodecInfo mtasCodecInfo, IndexSearcher searcher) throws IOException {
    // System.out.println(Thread.currentThread().getId()+" createList");
    if (listList != null) {
      for (ComponentList list : listList) {
        // collect not only stats
        if (list.number > 0) {
          HashMap<Integer, ArrayList<Match>> matchData = spansMatchData
              .get(list.spanQuery);
          HashMap<Integer, Integer> numberData = spansNumberData
              .get(list.spanQuery);
          ArrayList<Match> matchList;
          Integer matchNumber;
          if (list.output.equals(ComponentList.LIST_OUTPUT_HIT)) {
            for (int docId : docSet) {
              if (matchData != null
                  && (matchList = matchData.get(docId)) != null) {
                if (list.number == 0) {
                  list.position += matchList.size();
                } else if (list.position < (list.start + list.number)) {
                  boolean getDoc = false;
                  Match m;
                  for (int i = 0; i < matchList.size(); i++) {
                    if ((list.position >= list.start)
                        && (list.position < (list.start + list.number))) {
                      m = matchList.get(i);
                      getDoc = true;
                      int startPosition = m.startPosition;
                      int endPosition = m.endPosition - 1;
                      ArrayList<MtasTreeHit<String>> terms = mtasCodecInfo
                          .getPositionedTermsByPrefixesAndPositionRange(field,
                              (docId - docBase), list.prefixes,
                              startPosition - list.left,
                              endPosition + list.right);
                      // construct hit
                      HashMap<Integer, ArrayList<String>> kwicListHits = new HashMap<Integer, ArrayList<String>>();
                      for (int position = Math.max(0,
                          startPosition - list.left); position <= (endPosition
                              + list.right); position++) {
                        kwicListHits.put(position, new ArrayList<String>());
                      }
                      ArrayList<String> termList;
                      for (MtasTreeHit<String> term : terms) {
                        for (int position = Math.max(
                            (startPosition - list.left),
                            term.startPosition); position <= Math.min(
                                (endPosition + list.right),
                                term.endPosition); position++) {
                          termList = kwicListHits.get(position);
                          termList.add(term.data);
                        }
                      }
                      list.hits.add(new ListHit(docId, i, m, kwicListHits));
                    }
                    list.position++;
                  }
                  if (getDoc) {
                    // get unique id
                    Document doc = searcher.doc(docId,
                        new HashSet<String>(Arrays.asList(uniqueKeyField)));
                    IndexableField indxfld = doc.getField(uniqueKeyField);
                    // get other doc info
                    if (indxfld != null) {
                      list.uniqueKey.put(docId, indxfld.stringValue());
                    }
                    list.subTotal.put(docId, matchList.size());
                    IndexDoc mDoc = mtasCodecInfo.getDoc(field,
                        (docId - docBase));
                    if (mDoc != null) {
                      list.minPosition.put(docId, mDoc.minPosition);
                      list.maxPosition.put(docId, mDoc.maxPosition);
                    }
                  }
                } else {
                  list.position += matchList.size();
                }
              } else if (numberData != null
                  && (matchNumber = numberData.get(docId)) != null) {
                if (matchNumber != null) {
                  list.position += matchNumber;
                }
              }
            }
            list.total = list.position;
          } else if (list.output.equals(ComponentList.LIST_OUTPUT_TOKEN)) {
            for (int docId : docSet) {
              if (matchData != null
                  && (matchList = matchData.get(docId)) != null) {
                if (list.number == 0) {
                  list.position += matchList.size();
                } else if (list.position < (list.start + list.number)) {
                  boolean getDoc = false;
                  Match m;
                  for (int i = 0; i < matchList.size(); i++) {
                    if ((list.position >= list.start)
                        && (list.position < (list.start + list.number))) {
                      m = matchList.get(i);
                      getDoc = true;
                      int startPosition = m.startPosition;
                      int endPosition = m.endPosition - 1;
                      ArrayList<MtasToken<String>> tokens;
                      tokens = mtasCodecInfo
                          .getPrefixFilteredObjectsByPositions(field,
                              (docId - docBase), list.prefixes,
                              startPosition - list.left,
                              endPosition + list.right);
                      list.tokens.add(new ListToken(docId, i, m, tokens));
                    }
                    list.position++;
                  }
                  if (getDoc) {
                    // get unique id
                    Document doc = searcher.doc(docId,
                        new HashSet<String>(Arrays.asList(uniqueKeyField)));
                    IndexableField indxfld = doc.getField(uniqueKeyField);
                    // get other doc info
                    if (indxfld != null) {
                      list.uniqueKey.put(docId, indxfld.stringValue());
                    }
                    list.subTotal.put(docId, matchList.size());
                    IndexDoc mDoc = mtasCodecInfo.getDoc(field,
                        (docId - docBase));
                    if (mDoc != null) {
                      list.minPosition.put(docId, mDoc.minPosition);
                      list.maxPosition.put(docId, mDoc.maxPosition);
                    }
                  }
                } else {
                  list.position += matchList.size();
                }
              } else if (numberData != null
                  && (matchNumber = numberData.get(docId)) != null) {
                if (matchNumber != null) {
                  list.position += matchNumber;
                }
              }
            }
            list.total = list.position;
          }

        } else {
          HashMap<Integer, Integer> data = spansNumberData.get(list.spanQuery);
          if (data != null) {
            for (int docId : docSet) {
              Integer matchNumber = data.get(docId);
              if (matchNumber != null) {
                list.position += matchNumber;
              }
            }
            list.total = list.position;
          }
        }
      }
    }
  }

  /**
   * Creates the group.
   *
   * @param groupList the group list
   * @param spansMatchData the spans match data
   * @param docSet the doc set
   * @param field the field
   * @param docBase the doc base
   * @param mtasCodecInfo the mtas codec info
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private static void createGroup(List<ComponentGroup> groupList,
      HashMap<SpanQuery, HashMap<Integer, ArrayList<Match>>> spansMatchData,
      List<Integer> docSet, String field, int docBase, CodecInfo mtasCodecInfo)
      throws IOException {

    if (mtasCodecInfo != null && groupList != null) {
      ArrayList<Match> matchList;
      Integer start, end;
      HashMap<Integer, ArrayList<Match>> matchData;

      for (ComponentGroup group : groupList) {
        if (group.prefixes.size() > 0) {
          // compute prefixIds
          matchData = spansMatchData.get(group.spanQuery);
          group.dataCollector.initNewList(1);
          for (int docId : docSet) {
            if (matchData != null
                && (matchList = matchData.get(docId)) != null) {
              // administration
              HashMap<String, Long> occurences = new HashMap<String, Long>();
              // loop over matches
              Iterator<Match> it = matchList.listIterator();
              ArrayList<IntervalTreeNodeData> positionsHits = new ArrayList<IntervalTreeNodeData>();
              while (it.hasNext()) {
                Match m = it.next();
                if (group.hitInside != null) {
                  start = m.startPosition;
                  end = m.endPosition - 1;
                } else {
                  start = null;
                  end = null;
                }
                if (group.hitLeft != null) {
                  start = start == null ? m.startPosition
                      : Math.min(start, m.startPosition);
                  end = end == null ? m.startPosition + group.hitLeft.length - 1
                      : Math.max(m.startPosition + group.hitLeft.length - 1,
                          end);
                }
                if (group.hitRight != null) {
                  start = start == null
                      ? m.endPosition - group.hitRight.length + 1
                      : Math.min(m.endPosition - group.hitRight.length + 1,
                          start);
                  end = end == null ? m.endPosition
                      : Math.max(end, m.endPosition);
                }
                if (group.hitInsideLeft != null) {
                  start = start == null ? m.startPosition
                      : Math.min(start, m.startPosition);
                  end = end == null
                      ? Math.min(
                          m.startPosition + group.hitInsideLeft.length - 1,
                          m.endPosition - 1)
                      : Math.max(Math.min(
                          m.startPosition + group.hitInsideLeft.length - 1,
                          m.endPosition - 1), end);
                }
                if (group.hitInsideRight != null) {
                  start = start == null
                      ? Math.max(m.startPosition,
                          m.endPosition - group.hitInsideRight.length + 1)
                      : Math.min(
                          Math.max(m.startPosition,
                              m.endPosition - group.hitInsideRight.length + 1),
                          start);
                  end = end == null ? m.endPosition
                      : Math.max(end, m.endPosition);
                }
                if (group.left != null) {
                  start = start == null ? m.startPosition - group.left.length
                      : Math.min(m.startPosition - group.left.length, start);
                  end = end == null ? m.startPosition - 1
                      : Math.max(m.startPosition - 1, end);
                }
                if (group.right != null) {
                  start = start == null ? m.endPosition + 1
                      : Math.min(m.endPosition + 1, start);
                  end = end == null ? m.endPosition + group.right.length
                      : Math.max(m.endPosition + group.right.length, end);
                }
                positionsHits.add(new IntervalTreeNodeData<String>(start,end,m.startPosition,m.endPosition-1));                                
              }
              if(1>2) {
                for(IntervalTreeNodeData positionHit : positionsHits) {
                  ArrayList<MtasTreeHit<String>> list = mtasCodecInfo
                      .getPositionedTermsByPrefixesAndPositionRange(field,
                          (docId - docBase), group.prefixes, positionHit.start, positionHit.end);
                  GroupHit hit = new GroupHit(list, positionHit.start, positionHit.end, positionHit.hitStart, positionHit.hitEnd, group);
                  String key = hit.toString();
                  if (key != null) {
                    if (occurences.containsKey(key)) {
                      occurences.put(key, occurences.get(key) + 1);
                    } else {
                      occurences.put(key, Long.valueOf(1));
                    }
                  }
                }                              
                for (String key : occurences.keySet()) {
                  group.dataCollector.add(new String[] { key },
                      ArrayUtils.toPrimitive(new Long[] { occurences.get(key) }),
                      1);
                }
              } else {
                                
                mtasCodecInfo.collectTermsByPrefixesForListOfHitPositions(field,
                    (docId - docBase), group.prefixes, positionsHits);
                
                for(IntervalTreeNodeData<String> positionHit : positionsHits) {
                  GroupHit hit = new GroupHit(positionHit.list, positionHit.start, positionHit.end, positionHit.hitStart, positionHit.hitEnd, group);
                  String key = hit.toString();
                  if (key != null) {
                    if (occurences.containsKey(key)) {
                      occurences.put(key, occurences.get(key) + 1);
                    } else {
                      occurences.put(key, Long.valueOf(1));
                    }
                  } 
                }                              
                for (String key : occurences.keySet()) {
                  group.dataCollector.add(new String[] { key },
                      ArrayUtils.toPrimitive(new Long[] { occurences.get(key) }),
                      1);
                }
              }
            }
          }
          group.dataCollector.closeNewList();
        }
      }
    }
  }

  /**
   * Creates the kwic.
   *
   * @param kwicList the kwic list
   * @param spansMatchData the spans match data
   * @param docList the doc list
   * @param field the field
   * @param docBase the doc base
   * @param uniqueKeyField the unique key field
   * @param mtasCodecInfo the mtas codec info
   * @param searcher the searcher
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private static void createKwic(List<ComponentKwic> kwicList,
      HashMap<SpanQuery, HashMap<Integer, ArrayList<Match>>> spansMatchData,
      List<Integer> docList, String field, int docBase, String uniqueKeyField,
      CodecInfo mtasCodecInfo, IndexSearcher searcher) throws IOException {
    if (kwicList != null) {
      for (ComponentKwic kwic : kwicList) {
        HashMap<Integer, ArrayList<Match>> matchData = spansMatchData
            .get(kwic.query);
        ArrayList<Match> matchList;
        if (kwic.output.equals(ComponentKwic.KWIC_OUTPUT_HIT)) {
          for (int docId : docList) {
            if (matchData != null
                && (matchList = matchData.get(docId)) != null) {
              // get unique id
              Document doc = searcher.doc(docId,
                  new HashSet<String>(Arrays.asList(uniqueKeyField)));
              IndexableField indxfld = doc.getField(uniqueKeyField);
              // get other doc info
              if (indxfld != null) {
                kwic.uniqueKey.put(docId, indxfld.stringValue());
              }
              kwic.subTotal.put(docId, matchList.size());
              IndexDoc mDoc = mtasCodecInfo.getDoc(field, (docId - docBase));
              if (mDoc != null) {
                kwic.minPosition.put(docId, mDoc.minPosition);
                kwic.maxPosition.put(docId, mDoc.maxPosition);
              }
              // kwiclist
              ArrayList<KwicHit> kwicItemList = new ArrayList<KwicHit>();
              int number = 0;
              for (Match m : matchList) {
                if (kwic.number != null) {
                  if (number >= (kwic.start + kwic.number)) {
                    break;
                  }
                }
                if (number >= kwic.start) {
                  int startPosition = m.startPosition;
                  int endPosition = m.endPosition - 1;
                  ArrayList<MtasTreeHit<String>> terms = mtasCodecInfo
                      .getPositionedTermsByPrefixesAndPositionRange(field,
                          (docId - docBase), kwic.prefixes,
                          Math.max(mDoc.minPosition, startPosition - kwic.left),
                          Math.min(mDoc.maxPosition, endPosition + kwic.right));
                  // construct hit
                  HashMap<Integer, ArrayList<String>> kwicListHits = new HashMap<Integer, ArrayList<String>>();
                  for (int position = Math.max(mDoc.minPosition,
                      startPosition - kwic.left); position <= Math.min(
                          mDoc.maxPosition,
                          endPosition + kwic.right); position++) {
                    kwicListHits.put(position, new ArrayList<String>());
                  }
                  ArrayList<String> termList;
                  for (MtasTreeHit<String> term : terms) {
                    for (int position = Math.max((startPosition - kwic.left),
                        term.startPosition); position <= Math.min(
                            (endPosition + kwic.right),
                            term.endPosition); position++) {
                      termList = kwicListHits.get(position);
                      termList.add(term.data);
                    }
                  }
                  kwicItemList.add(new KwicHit(m, kwicListHits));
                }
                number++;
              }
              kwic.hits.put(docId, kwicItemList);
            }
          }
        } else if (kwic.output.equals(ComponentKwic.KWIC_OUTPUT_TOKEN)) {
          for (int docId : docList) {
            if (matchData != null
                && (matchList = matchData.get(docId)) != null) {
              // get unique id
              Document doc = searcher.doc(docId,
                  new HashSet<String>(Arrays.asList(uniqueKeyField)));
              // get other doc info
              IndexableField indxfld = doc.getField(uniqueKeyField);
              if (indxfld != null) {
                kwic.uniqueKey.put(docId, indxfld.stringValue());
              }
              kwic.subTotal.put(docId, matchList.size());
              IndexDoc mDoc = mtasCodecInfo.getDoc(field, (docId - docBase));
              if (mDoc != null) {
                kwic.minPosition.put(docId, mDoc.minPosition);
                kwic.maxPosition.put(docId, mDoc.maxPosition);
              }
              ArrayList<KwicToken> kwicItemList = new ArrayList<KwicToken>();
              int number = 0;
              for (Match m : matchList) {
                if (kwic.number != null) {
                  if (number >= kwic.number) {
                    break;
                  }
                }
                int startPosition = m.startPosition;
                int endPosition = m.endPosition - 1;
                ArrayList<MtasToken<String>> tokens;
                tokens = mtasCodecInfo.getPrefixFilteredObjectsByPositions(
                    field, (docId - docBase), kwic.prefixes,
                    Math.max(mDoc.minPosition, startPosition - kwic.left),
                    Math.min(mDoc.maxPosition, endPosition + kwic.right));
                kwicItemList.add(new KwicToken(m, tokens));
                number++;
              }
              kwic.tokens.put(docId, kwicItemList);
            }
          }
        }
      }
    }
  }

  /**
   * Creates the facet base.
   *
   * @param cf the cf
   * @param level the level
   * @param dataCollector the data collector
   * @param positionsData the positions data
   * @param spansNumberData the spans number data
   * @param facetData the facet data
   * @param docSet the doc set
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private static void createFacetBase(ComponentFacet cf, int level,
      MtasDataCollector<?, ?> dataCollector,
      HashMap<Integer, Integer> positionsData,
      HashMap<SpanQuery, HashMap<Integer, Integer>> spansNumberData,
      HashMap<String, TreeMap<String, int[]>> facetData, Integer[] docSet)
      throws IOException {
    if (cf.baseFunctionParsers[level]
        .needArgumentsNumber() > cf.spanQueries.length) {
      throw new IOException("function " + cf.baseFunctionParsers[level]
          + " expects (at least) "
          + cf.baseFunctionParsers[level].needArgumentsNumber() + " queries");
    }
    TreeMap<String, int[]> list = facetData.get(cf.baseFields[level]);
    if (dataCollector != null) {
      MtasDataCollector<?, ?>[] subDataCollectors = null;
      dataCollector.initNewList(cf.baseFields.length - level);
      // check type
      if (dataCollector.getCollectorType()
          .equals(DataCollector.COLLECTOR_TYPE_LIST)) {
        // only if documents and facets
        if (docSet.length > 0 && list.size() > 0) {
          HashMap<String, Integer[]> docLists = new HashMap<String, Integer[]>();
          boolean documentsInFacets = false;
          // compute intersections
          for (String key : list.keySet()) {
            // intersect docSet with docList
            Integer[] docList = intersectedDocList(list.get(key), docSet);
            if (docList.length > 0) {
              documentsInFacets = true;
            }
            docLists.put(key, docList);
          }
          // compute stats for each key
          if (documentsInFacets) {
            HashMap<Integer, long[]> args = computeArguments(spansNumberData,
                cf.spanQueries, docSet);
            if (cf.baseDataTypes[level].equals(CodecUtil.DATA_TYPE_LONG)
                || cf.baseDataTypes[level].equals(CodecUtil.DATA_TYPE_DOUBLE)) {
              // sumrule
              if (cf.baseStatsTypes[level].equals(CodecUtil.STATS_BASIC)
                  && cf.baseFunctionParsers[level].sumRule()
                  && (cf.baseMinimumLongs[level] == null)
                  && (cf.baseMaximumLongs[level] == null)) {
                for (String key : list.keySet()) {
                  if (docLists.get(key).length > 0) {
                    // initialise
                    String[] keys = new String[] { key };
                    Integer[] subDocSet = docLists.get(key);
                    int length = cf.baseFunctionParsers[level]
                        .needArgumentsNumber();
                    long[] valueSum = new long[length];
                    long valuePositions = 0;
                    // collect
                    if (subDocSet.length > 0) {
                      long[] tmpArgs;
                      for (int docId : subDocSet) {
                        tmpArgs = args.get(docId);
                        valuePositions += (positionsData == null) ? 0
                            : positionsData.get(docId);
                        if (tmpArgs != null) {
                          for (int i = 0; i < length; i++) {
                            valueSum[i] += tmpArgs[i];
                          }
                        }
                      }
                      if (cf.baseDataTypes[level]
                          .equals(CodecUtil.DATA_TYPE_LONG)) {
                        long value;
                        try {
                          value = cf.baseFunctionParsers[level]
                              .getValueLong(valueSum, valuePositions);
                          subDataCollectors = dataCollector.add(keys, value,
                              subDocSet.length);
                        } catch (IOException e) {
                          dataCollector.error(keys, e.getMessage());
                          subDataCollectors = null;
                        }
                        if (subDataCollectors != null) {
                          for (MtasDataCollector<?, ?> subDataCollector : subDataCollectors) {
                            if (subDataCollector != null) {
                              createFacetBase(cf, (level + 1), subDataCollector,
                                  positionsData, spansNumberData, facetData,
                                  subDocSet);
                            }
                          }
                        }
                      } else if (cf.baseDataTypes[level]
                          .equals(CodecUtil.DATA_TYPE_DOUBLE)) {
                        double value;
                        try {
                          value = cf.baseFunctionParsers[level]
                              .getValueDouble(valueSum, valuePositions);
                          subDataCollectors = dataCollector.add(keys, value,
                              subDocSet.length);
                        } catch (IOException e) {
                          dataCollector.error(keys, e.getMessage());
                          subDataCollectors = null;
                        }
                        if (subDataCollectors != null) {
                          for (MtasDataCollector<?, ?> subDataCollector : subDataCollectors) {
                            if (subDataCollector != null) {
                              createFacetBase(cf, (level + 1), subDataCollector,
                                  positionsData, spansNumberData, facetData,
                                  subDocSet);
                            }
                          }
                        }
                      } else {
                        throw new IOException(
                            "unrecognized dataType " + cf.baseDataTypes[level]);
                      }
                    }
                  }
                }
                // normal
              } else {
                for (String key : list.keySet()) {
                  if (docLists.get(key).length > 0) {
                    // initialise
                    String[] keys = new String[] { key };
                    Integer[] subDocSet = docLists.get(key);
                    // collect
                    if (subDocSet.length > 0) {
                      if (cf.baseDataTypes[level]
                          .equals(CodecUtil.DATA_TYPE_LONG)) {
                        int number = 0;
                        Integer[] restrictedSubDocSet = new Integer[subDocSet.length];
                        long value;
                        long[] values = new long[subDocSet.length];
                        long[] tmpArgs;
                        int tmpPositions;
                        for (int docId : subDocSet) {
                          try {
                            tmpArgs = args.get(docId);
                            tmpPositions = (positionsData == null) ? 0
                                : positionsData.get(docId);
                            value = cf.baseFunctionParsers[level]
                                .getValueLong(tmpArgs, tmpPositions);
                            if ((cf.baseMinimumLongs[level] == null
                                || value >= cf.baseMinimumLongs[level])
                                && (cf.baseMaximumLongs[level] == null
                                    || value <= cf.baseMaximumLongs[level])) {
                              values[number] = value;
                              restrictedSubDocSet[number] = docId;
                              number++;
                            }
                          } catch (IOException e) {
                            dataCollector.error(keys, e.getMessage());
                          }
                        }
                        if (number > 0) {
                          subDataCollectors = dataCollector.add(keys, values,
                              number);
                          if (subDataCollectors != null) {
                            for (MtasDataCollector<?, ?> subDataCollector : subDataCollectors) {
                              if (subDataCollector != null) {
                                createFacetBase(cf, (level + 1),
                                    subDataCollector, positionsData,
                                    spansNumberData, facetData,
                                    Arrays.copyOfRange(restrictedSubDocSet, 0,
                                        number));
                              }
                            }
                          }
                        }
                      } else if (cf.baseDataTypes[level]
                          .equals(CodecUtil.DATA_TYPE_DOUBLE)) {
                        int number = 0;
                        Integer[] restrictedSubDocSet = new Integer[subDocSet.length];
                        double value;
                        double[] values = new double[subDocSet.length];
                        long[] tmpArgs;
                        int tmpPositions;
                        for (int docId : subDocSet) {
                          try {
                            tmpArgs = args.get(docId);
                            tmpPositions = (positionsData == null) ? 0
                                : positionsData.get(docId);
                            value = cf.baseFunctionParsers[level]
                                .getValueDouble(tmpArgs, tmpPositions);
                            if ((cf.baseMinimumDoubles[level] == null
                                || value >= cf.baseMinimumDoubles[level])
                                && (cf.baseMaximumDoubles[level] == null
                                    || value <= cf.baseMaximumDoubles[level])) {
                              values[number] = value;
                              restrictedSubDocSet[number] = docId;
                              number++;
                            }
                          } catch (IOException e) {
                            dataCollector.error(keys, e.getMessage());
                          }
                        }
                        if (number > 0) {
                          subDataCollectors = dataCollector.add(keys, values,
                              number);
                          if (subDataCollectors != null) {
                            for (MtasDataCollector<?, ?> subDataCollector : subDataCollectors) {
                              if (subDataCollector != null) {
                                createFacetBase(cf, (level + 1),
                                    subDataCollector, positionsData,
                                    spansNumberData, facetData,
                                    Arrays.copyOfRange(restrictedSubDocSet, 0,
                                        number));
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            } else {
              throw new IOException(
                  "unknown dataType " + cf.baseDataTypes[level]);
            }
          }
        }
      } else {
        throw new IOException(
            "unknown type " + dataCollector.getCollectorType());
      }
      dataCollector.closeNewList();
    }
  }

  /**
   * Creates the facet.
   *
   * @param facetList the facet list
   * @param positionsData the positions data
   * @param spansNumberData the spans number data
   * @param facetData the facet data
   * @param docSet the doc set
   * @param field the field
   * @param docBase the doc base
   * @param uniqueKeyField the unique key field
   * @param mtasCodecInfo the mtas codec info
   * @param searcher the searcher
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private static void createFacet(List<ComponentFacet> facetList,
      HashMap<Integer, Integer> positionsData,
      HashMap<SpanQuery, HashMap<Integer, Integer>> spansNumberData,
      HashMap<String, TreeMap<String, int[]>> facetData, List<Integer> docSet,
      String field, int docBase, String uniqueKeyField, CodecInfo mtasCodecInfo,
      IndexSearcher searcher) throws IOException {

    if (facetList != null) {
      for (ComponentFacet cf : facetList) {
        if (cf.baseFields.length > 0) {
          createFacetBase(cf, 0, cf.dataCollector, positionsData,
              spansNumberData, facetData,
              docSet.toArray(new Integer[docSet.size()]));
        }
      }
    }
  }

  // private static void createTermVector(LeafReader r, LeafReaderContext lrc,
  // Terms t, List<Integer> docSet, ComponentField fieldInfo)

  /**
   * Creates the termvector.
   *
   * @param termVectorList the term vector list
   * @param positionsData the positions data
   * @param docSet the doc set
   * @param field the field
   * @param t the t
   * @param r the r
   * @param lrc the lrc
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private static void createTermvector(List<ComponentTermVector> termVectorList,
      HashMap<Integer, Integer> positionsData, List<Integer> docSet,
      String field, Terms t, LeafReader r, LeafReaderContext lrc)
      throws IOException {
    if (t != null) {
      BytesRef term;
      TermsEnum termsEnum;
      PostingsEnum postingsEnum = null;
      for (ComponentTermVector termVector : termVectorList) {
        termsEnum = t.intersect(termVector.compiledAutomaton, null);
        termVector.dataCollector.initNewList((int) t.size());
        if (docSet.size() > 0) {
          while ((term = termsEnum.next()) != null) {
            int termDocId = -1;
            String key = MtasToken.getPostfixFromValue(term.utf8ToString());
            String[] keys = new String[] { key };
            if (termVector.statsType.equals(CodecUtil.STATS_BASIC)
                && termVector.functionParser.sumRule()
                && (termVector.minimumLong == null)
                && (termVector.maximumLong == null)
                && !termVector.functionParser.needPositions()) {
              long[] valueSum = new long[] { 0 };
              int docNumber = 0;
              boolean subtractValue = false;
              if (docSet.size() == r.numDocs()) {
                valueSum[0] = termsEnum.totalTermFreq();
                docNumber = termsEnum.docFreq();
              } else {
                if (docSet.size() > Math.round(r.numDocs() / 2)) {
                  valueSum[0] = termsEnum.totalTermFreq();
                  subtractValue = true;
                }
                Iterator<Integer> docIterator = docSet.iterator();
                postingsEnum = termsEnum.postings(postingsEnum,
                    PostingsEnum.FREQS);
                while (docIterator.hasNext()) {
                  int docId = docIterator.next() - lrc.docBase;
                  if (docId >= termDocId) {
                    if ((docId == termDocId) || ((termDocId = postingsEnum
                        .advance(docId)) == docId)) {
                      docNumber++;
                      if (subtractValue) {
                        valueSum[0] -= postingsEnum.freq();
                      } else {
                        valueSum[0] += postingsEnum.freq();
                      }
                    }
                  }
                }
              }
              if (docNumber > 0) {
                if (termVector.dataCollector.getDataType()
                    .equals(CodecUtil.DATA_TYPE_LONG)) {
                  long value = termVector.functionParser.getValueLong(valueSum,
                      0);
                  termVector.dataCollector.add(keys, value, docNumber);
                } else if (termVector.dataCollector.getDataType()
                    .equals(CodecUtil.DATA_TYPE_LONG)) {
                  double value = termVector.functionParser
                      .getValueDouble(valueSum, 0);
                  termVector.dataCollector.add(keys, value, docNumber);
                } else {
                  throw new IOException("unknown dataType "
                      + termVector.dataCollector.getDataType());
                }
              }
            } else {
              Iterator<Integer> docIterator = docSet.iterator();
              postingsEnum = termsEnum.postings(postingsEnum,
                  PostingsEnum.FREQS);
              if (termVector.dataCollector.getDataType()
                  .equals(CodecUtil.DATA_TYPE_LONG)) {
                long[] args = new long[1];
                long value;
                long[] values = new long[docSet.size()];
                int positions, number = 0;
                while (docIterator.hasNext()) {
                  int docId = docIterator.next() - lrc.docBase;
                  if (docId >= termDocId) {
                    if ((docId == termDocId) || ((termDocId = postingsEnum
                        .advance(docId)) == docId)) {
                      args[0] = postingsEnum.freq();
                      positions = (positionsData == null) ? 0
                          : positionsData.get(docId + lrc.docBase);
                      try {
                        value = termVector.functionParser.getValueLong(args,
                            positions);
                        values[number] = value;
                      } catch (IOException e) {
                        termVector.dataCollector.error(keys, e.getMessage());
                      }
                      number++;
                    }
                  }
                }
                if (number > 0) {
                  termVector.dataCollector.add(keys, values, number);
                }
              } else if (termVector.dataCollector.getDataType()
                  .equals(CodecUtil.DATA_TYPE_DOUBLE)) {
                long[] args = new long[1];
                double value;
                double[] values = new double[docSet.size()];
                int positions, number = 0;
                while (docIterator.hasNext()) {
                  int docId = docIterator.next() - lrc.docBase;
                  if (docId >= termDocId) {
                    if ((docId == termDocId) || ((termDocId = postingsEnum
                        .advance(docId)) == docId)) {
                      args[0] = postingsEnum.freq();
                      positions = (positionsData == null) ? 0
                          : positionsData.get(docId + lrc.docBase);
                      try {
                        value = termVector.functionParser.getValueDouble(args,
                            positions);
                        values[number] = value;
                      } catch (IOException e) {
                        termVector.dataCollector.error(keys, e.getMessage());
                      }
                      number++;
                    }
                  }
                }
                if (number > 0) {
                  termVector.dataCollector.add(keys, values, number);
                }
              }
            }
          }
        }
        termVector.dataCollector.closeNewList();
      }
    }
  }

}
