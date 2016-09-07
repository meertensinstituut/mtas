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
import mtas.codec.util.CodecComponent.SubComponentFunction;
import mtas.codec.util.CodecInfo.IndexDoc;
import mtas.codec.util.CodecSearchTree.MtasTreeHit;
import mtas.codec.util.collector.MtasDataCollector;
import mtas.parser.function.ParseException;
import mtas.parser.function.util.MtasFunctionParserFunction;
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
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LegacyNumericUtils;
import org.apache.lucene.util.automaton.CompiledAutomaton;

/**
 * The Class CodecCollector.
 */
@SuppressWarnings("deprecation")
public class CodecCollector {

  /**
   * Collect.
   *
   * @param field
   *          the field
   * @param searcher
   *          the searcher
   * @param reader
   *          the reader
   * @param rawReader
   *          the raw reader
   * @param fullDocList
   *          the full doc list
   * @param fullDocSet
   *          the full doc set
   * @param fieldInfo
   *          the field info
   * @param spansQueryWeight
   *          the spans query weight
   * @throws IllegalAccessException
   *           the illegal access exception
   * @throws IllegalArgumentException
   *           the illegal argument exception
   * @throws InvocationTargetException
   *           the invocation target exception
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public static void collect(String field, IndexSearcher searcher,
      IndexReader reader, IndexReader rawReader, ArrayList<Integer> fullDocList,
      ArrayList<Integer> fullDocSet, ComponentField fieldInfo,
      HashMap<SpanQuery, SpanWeight> spansQueryWeight)
      throws IllegalAccessException, IllegalArgumentException,
      InvocationTargetException, IOException {

    HashMap<Integer, List<Integer>> docSets = new HashMap<Integer, List<Integer>>();

    ListIterator<LeafReaderContext> iterator = reader.leaves().listIterator();
    while (iterator.hasNext()) {
      LeafReaderContext lrc = iterator.next();
      LeafReader r = lrc.reader();

      // compute relevant docSet/docList
      List<Integer> docSet = null;
      List<Integer> docList = null;
      if (fullDocSet != null) {
        docSet = new ArrayList<Integer>();
        docSets.put(lrc.ord, docSet);
        Iterator<Integer> docSetIterator = fullDocSet.iterator();
        Integer docSetId = null;
        Bits liveDocs = lrc.reader().getLiveDocs();
        while (docSetIterator.hasNext()) {
          docSetId = docSetIterator.next();
          if ((docSetId >= lrc.docBase)
              && (docSetId < lrc.docBase + lrc.reader().maxDoc())) {
            // just to make sure to ignore deleted documents
            if (liveDocs == null || liveDocs.get((docSetId - lrc.docBase))) {
              docSet.add(docSetId);
            }
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

      collectSpansPositionsAndTokens(spansQueryWeight, searcher, mtasCodecInfo,
          r, lrc, field, t, docSet, docList, fieldInfo);
      collectPrefixes(rawReader.leaves().get(lrc.ord).reader().getFieldInfos(),
          field, fieldInfo);
    }

    // check termvectors
    if (fieldInfo.termVectorList.size() > 0) {
      if (needSecondRoundTermvector(fieldInfo.termVectorList)) {
        // check positions
        boolean needPositions = false;
        if (fieldInfo.termVectorList.size() > 0) {
          for (ComponentTermVector ctv : fieldInfo.termVectorList) {
            needPositions = !needPositions ? (ctv.functions != null
                ? ctv.functionNeedPositions() : needPositions) : needPositions;
          }
        }
        HashMap<Integer, Integer> positionsData = null;

        // loop
        iterator = reader.leaves().listIterator();
        while (iterator.hasNext()) {
          LeafReaderContext lrc = iterator.next();
          LeafReader r = lrc.reader();
          List<Integer> docSet = docSets.get(lrc.ord);
          Terms t = rawReader.leaves().get(lrc.ord).reader().terms(field);
          if (needPositions) {
            CodecInfo mtasCodecInfo = t == null ? null
                : CodecInfo.getCodecInfoFromTerms(t);
            positionsData = computePositions(mtasCodecInfo, r, lrc, field, t,
                docSet);
          }
          createTermvectorSecondRound(fieldInfo.termVectorList, positionsData,
              docSets.get(lrc.ord), field, t, r, lrc);
        }
      }
    }
  }

  /**
   * Collect spans positions and tokens.
   *
   * @param spansQueryWeight
   *          the spans query weight
   * @param searcher
   *          the searcher
   * @param mtasCodecInfo
   *          the mtas codec info
   * @param r
   *          the r
   * @param lrc
   *          the lrc
   * @param field
   *          the field
   * @param t
   *          the t
   * @param docSet
   *          the doc set
   * @param docList
   *          the doc list
   * @param fieldInfo
   *          the field info
   * @throws IOException
   *           Signals that an I/O exception has occurred.
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
        needPositions = !needPositions ? (ctv.functions == null
            ? ctv.subComponentFunction.parserFunction.needPositions()
            : ctv.functionNeedPositions()) : needPositions;
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
          needPositions = (!needPositions) ? cs.parser.needPositions()
              : needPositions;
          needPositions = (!needPositions) ? cs.functionNeedPositions()
              : needPositions;
          needSpans = (!needSpans) ? cs.parser.needArgumentsNumber() > 0
              : needSpans;
          HashSet<Integer> arguments = cs.parser.needArgument();
          arguments.addAll(cs.functionNeedArguments());
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
          needPositions = !needPositions ? cf.baseParserNeedPositions()
              : needPositions;
          needPositions = !needPositions ? cf.functionNeedPositions()
              : needPositions;
          for (int i = 0; i < cf.baseFields.length; i++) {
            needSpans = !needSpans ? cf.baseParsers[i].needArgumentsNumber() > 0
                : needSpans;
            HashSet<Integer> arguments = cf.baseParsers[i].needArgument();
            for (int a : arguments) {
              if (cf.spanQueries.length > a) {
                SpanQuery q = cf.spanQueries[a];
                if (!spansNumberData.containsKey(q)) {
                  spansNumberData.put(q, new HashMap<Integer, Integer>());
                }
              }
            }
            for (MtasFunctionParserFunction function : cf.baseFunctionParserFunctions[i]) {
              needSpans = !needSpans ? function.needArgumentsNumber() > 0
                  : needSpans;
              arguments = function.needArgument();
              for (int a : arguments) {
                if (cf.spanQueries.length > a) {
                  SpanQuery q = cf.spanQueries[a];
                  if (!spansNumberData.containsKey(q)) {
                    spansNumberData.put(q, new HashMap<Integer, Integer>());
                  }
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
          if ((ctv.subComponentFunction.parserFunction != null
              && ctv.subComponentFunction.parserFunction.needPositions())
              || (ctv.functions != null && ctv.functionNeedPositions())) {
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
            positionsData.put(docId, mtasCodecInfo.getNumberOfPositions(field,
                (docId - lrc.docBase)));
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
          tokensData = mtasCodecInfo.getAllNumberOfTokens(field, lrc.docBase);
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
   * @param fieldInfos
   *          the field infos
   * @param field
   *          the field
   * @param fieldInfo
   *          the field info
   * @throws IOException
   *           Signals that an I/O exception has occurred.
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
        String setPositionPrefixes = fi.getAttribute(
            MtasCodecPostingsFormat.MTAS_FIELDINFO_ATTRIBUTE_PREFIX_SET_POSITION);
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
        if (setPositionPrefixes != null) {
          String[] prefixes = setPositionPrefixes
              .split(Pattern.quote(MtasToken.DELIMITER));
          for (int i = 0; i < prefixes.length; i++) {
            fieldInfo.prefix.addSetPosition(prefixes[i]);
          }
        }
      }
    }
  }

  /**
   * Compute positions.
   *
   * @param mtasCodecInfo
   *          the mtas codec info
   * @param r
   *          the r
   * @param lrc
   *          the lrc
   * @param field
   *          the field
   * @param t
   *          the t
   * @param docSet
   *          the doc set
   * @return the hash map
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private static HashMap<Integer, Integer> computePositions(
      CodecInfo mtasCodecInfo, LeafReader r, LeafReaderContext lrc,
      String field, Terms t, List<Integer> docSet) throws IOException {
    HashMap<Integer, Integer> positionsData;
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
    return positionsData;
  }

  /**
   * Compute arguments.
   *
   * @param spansNumberData
   *          the spans number data
   * @param queries
   *          the queries
   * @param docSet
   *          the doc set
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
   * @param facetDocList
   *          the facet doc list
   * @param docSet
   *          the doc set
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
   * @param statsPositionList
   *          the stats position list
   * @param positionsData
   *          the positions data
   * @param docSet
   *          the doc set
   * @throws IOException
   *           Signals that an I/O exception has occurred.
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
   * @param statsTokenList
   *          the stats token list
   * @param tokensData
   *          the tokens data
   * @param docSet
   *          the doc set
   * @throws IOException
   *           Signals that an I/O exception has occurred.
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
        if (tokensData != null) {
          for (int docId : docSet) {
            tmpValue = tokensData.get(docId);
            value = tmpValue == null ? 0 : tmpValue.intValue();
            if (((token.minimumLong == null) || (value >= token.minimumLong))
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
   * @param statsSpanList
   *          the stats span list
   * @param positionsData
   *          the positions data
   * @param spansNumberData
   *          the spans number data
   * @param docSet
   *          the doc set
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private static void createStats(List<ComponentSpan> statsSpanList,
      HashMap<Integer, Integer> positionsData,
      HashMap<SpanQuery, HashMap<Integer, Integer>> spansNumberData,
      Integer[] docSet) throws IOException {
    if (statsSpanList != null) {
      for (ComponentSpan span : statsSpanList) {
        if (span.parser.needArgumentsNumber() > span.queries.length) {
          throw new IOException(
              "function " + span.parser + " expects (at least) "
                  + span.parser.needArgumentsNumber() + " queries");
        }
        // collect
        HashMap<Integer, long[]> args = computeArguments(spansNumberData,
            span.queries, docSet);
        if (span.dataType.equals(CodecUtil.DATA_TYPE_LONG)) {
          // try to call functionParser as little as possible
          if (span.statsType.equals(CodecUtil.STATS_BASIC)
              && (span.minimumLong == null) && (span.maximumLong == null)
              && (span.functions == null || (span.functionSumRule()
                  && !span.functionNeedPositions()))) {
            // initialise
            int length = span.parser.needArgumentsNumber();
            long[] valueSum = new long[length];
            long valuePositions = 0;
            // collect
            if (docSet.length > 0) {
              long[] tmpArgs;
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
              long valueLong;
              span.dataCollector.initNewList(1);
              try {
                valueLong = span.parser.getValueLong(valueSum, valuePositions);
                span.dataCollector.add(valueLong, docSet.length);
              } catch (IOException e) {
                span.dataCollector.error(e.getMessage());
              }
              if (span.functions != null) {
                for (SubComponentFunction function : span.functions) {
                  function.dataCollector.initNewList(1);
                  if (function.dataType.equals(CodecUtil.DATA_TYPE_LONG)) {
                    try {
                      valueLong = function.parserFunction.getValueLong(valueSum,
                          valuePositions);
                      function.dataCollector.add(valueLong, docSet.length);
                    } catch (IOException e) {
                      function.dataCollector.error(e.getMessage());
                    }
                  } else if (function.dataType
                      .equals(CodecUtil.DATA_TYPE_DOUBLE)) {
                    try {
                      double valueDouble = function.parserFunction
                          .getValueDouble(valueSum, valuePositions);
                      function.dataCollector.add(valueDouble, docSet.length);
                    } catch (IOException e) {
                      function.dataCollector.error(e.getMessage());
                    }
                  } else {
                    throw new IOException(
                        "can't handle function dataType " + function.dataType);
                  }
                  function.dataCollector.closeNewList();
                }
              }
              span.dataCollector.closeNewList();
            }
          } else {
            // collect
            if (docSet.length > 0) {
              int number = 0, positions;
              long valueLong;
              double valueDouble;
              long values[] = new long[docSet.length];
              long functionValuesLong[][] = null;
              double functionValuesDouble[][] = null;
              span.dataCollector.initNewList(1);
              if (span.functions != null) {
                functionValuesLong = new long[span.functions.size()][];
                functionValuesDouble = new double[span.functions.size()][];
                for (int i = 0; i < span.functions.size(); i++) {
                  SubComponentFunction function = span.functions.get(i);
                  if (function.dataType.equals(CodecUtil.DATA_TYPE_LONG)) {
                    functionValuesLong[i] = new long[docSet.length];
                    functionValuesDouble[i] = null;
                  } else if (function.dataType
                      .equals(CodecUtil.DATA_TYPE_DOUBLE)) {
                    functionValuesLong[i] = null;
                    functionValuesDouble[i] = new double[docSet.length];
                  }
                  function.dataCollector.initNewList(1);
                }
              }
              for (int docId : docSet) {
                positions = (positionsData == null) ? 0
                    : (positionsData.get(docId) == null ? 0
                        : positionsData.get(docId));
                try {
                  valueLong = span.parser.getValueLong(args.get(docId),
                      positions);
                  if (((span.minimumLong == null)
                      || (valueLong >= span.minimumLong))
                      && ((span.maximumLong == null)
                          || (valueLong <= span.maximumLong))) {
                    values[number] = valueLong;
                    if (span.functions != null) {
                      for (int i = 0; i < span.functions.size(); i++) {
                        SubComponentFunction function = span.functions.get(i);
                        try {
                          if (function.dataType
                              .equals(CodecUtil.DATA_TYPE_LONG)) {
                            valueLong = function.parserFunction
                                .getValueLong(args.get(docId), positions);
                            functionValuesLong[i][number] = valueLong;
                          } else if (function.dataType
                              .equals(CodecUtil.DATA_TYPE_DOUBLE)) {
                            valueDouble = function.parserFunction
                                .getValueDouble(args.get(docId), positions);
                            functionValuesDouble[i][number] = valueDouble;
                          }
                        } catch (IOException e) {
                          function.dataCollector.error(e.getMessage());
                        }
                      }
                    }
                    number++;
                  }
                } catch (IOException e) {
                  span.dataCollector.error(e.getMessage());
                }
              }
              if (number > 0) {
                span.dataCollector.add(values, number);
                if (span.functions != null) {
                  for (int i = 0; i < span.functions.size(); i++) {
                    SubComponentFunction function = span.functions.get(i);
                    if (function.dataType.equals(CodecUtil.DATA_TYPE_LONG)) {
                      function.dataCollector.add(functionValuesLong[i], number);
                    } else if (function.dataType
                        .equals(CodecUtil.DATA_TYPE_DOUBLE)) {
                      function.dataCollector.add(functionValuesDouble[i],
                          number);
                    }
                  }
                }
              }
              span.dataCollector.closeNewList();
              if (span.functions != null) {
                for (SubComponentFunction function : span.functions) {
                  function.dataCollector.closeNewList();
                }
              }
            }
          }
        } else {
          throw new IOException("unexpected dataType " + span.dataType);
        }
      }
    }
  }

  /**
   * Creates the list.
   *
   * @param listList
   *          the list list
   * @param spansNumberData
   *          the spans number data
   * @param spansMatchData
   *          the spans match data
   * @param docSet
   *          the doc set
   * @param field
   *          the field
   * @param docBase
   *          the doc base
   * @param uniqueKeyField
   *          the unique key field
   * @param mtasCodecInfo
   *          the mtas codec info
   * @param searcher
   *          the searcher
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private static void createList(List<ComponentList> listList,
      HashMap<SpanQuery, HashMap<Integer, Integer>> spansNumberData,
      HashMap<SpanQuery, HashMap<Integer, ArrayList<Match>>> spansMatchData,
      List<Integer> docSet, String field, int docBase, String uniqueKeyField,
      CodecInfo mtasCodecInfo, IndexSearcher searcher) throws IOException {
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
   * @param groupList
   *          the group list
   * @param spansMatchData
   *          the spans match data
   * @param docSet
   *          the doc set
   * @param field
   *          the field
   * @param docBase
   *          the doc base
   * @param mtasCodecInfo
   *          the mtas codec info
   * @throws IOException
   *           Signals that an I/O exception has occurred.
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
              ArrayList<IntervalTreeNodeData<String>> positionsHits = new ArrayList<IntervalTreeNodeData<String>>();
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
                positionsHits.add(new IntervalTreeNodeData<String>(start, end,
                    m.startPosition, m.endPosition - 1));
              }
              // if(1>2) {
              // for(IntervalTreeNodeData positionHit :
              // positionsHits) {
              // ArrayList<MtasTreeHit<String>> list =
              // mtasCodecInfo
              // .getPositionedTermsByPrefixesAndPositionRange(field,
              // (docId - docBase), group.prefixes,
              // positionHit.start,
              // positionHit.end);
              // GroupHit hit = new GroupHit(list,
              // positionHit.start,
              // positionHit.end, positionHit.hitStart,
              // positionHit.hitEnd,
              // group);
              // String key = hit.toString();
              // if (key != null) {
              // if (occurences.containsKey(key)) {
              // occurences.put(key, occurences.get(key) + 1);
              // } else {
              // occurences.put(key, Long.valueOf(1));
              // }
              // }
              // }
              // for (String key : occurences.keySet()) {
              // group.dataCollector.add(new String[] { key },
              // ArrayUtils.toPrimitive(new Long[] {
              // occurences.get(key) }),
              // 1);
              // }
              // } else {

              mtasCodecInfo.collectTermsByPrefixesForListOfHitPositions(field,
                  (docId - docBase), group.prefixes, positionsHits);

              for (IntervalTreeNodeData<String> positionHit : positionsHits) {
                GroupHit hit = new GroupHit(positionHit.list, positionHit.start,
                    positionHit.end, positionHit.hitStart, positionHit.hitEnd,
                    group);
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
              // }
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
   * @param kwicList
   *          the kwic list
   * @param spansMatchData
   *          the spans match data
   * @param docList
   *          the doc list
   * @param field
   *          the field
   * @param docBase
   *          the doc base
   * @param uniqueKeyField
   *          the unique key field
   * @param mtasCodecInfo
   *          the mtas codec info
   * @param searcher
   *          the searcher
   * @throws IOException
   *           Signals that an I/O exception has occurred.
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
   * @param cf
   *          the cf
   * @param level
   *          the level
   * @param dataCollector
   *          the data collector
   * @param positionsData
   *          the positions data
   * @param spansNumberData
   *          the spans number data
   * @param facetData
   *          the facet data
   * @param docSet
   *          the doc set
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private static void createFacetBase(ComponentFacet cf, int level,
      MtasDataCollector<?, ?> dataCollector,
      HashMap<Integer, Integer> positionsData,
      HashMap<SpanQuery, HashMap<Integer, Integer>> spansNumberData,
      HashMap<String, TreeMap<String, int[]>> facetData, Integer[] docSet)
      throws IOException {
    for (MtasFunctionParserFunction function : cf.baseFunctionParserFunctions[level]) {
      if (function.needArgumentsNumber() > cf.spanQueries.length) {
        throw new IOException("function " + function + " expects (at least) "
            + function.needArgumentsNumber() + " queries");
      }
    }
    TreeMap<String, int[]> list = facetData.get(cf.baseFields[level]);
    if (dataCollector != null) {
      MtasDataCollector<?, ?>[] subDataCollectors = null;
      dataCollector.initNewList(1);
      if (cf.baseFunctionList[level] != null) {
        SubComponentFunction[] tmpList;
        if (!cf.baseFunctionList[level].containsKey(dataCollector)) {
          tmpList = new SubComponentFunction[cf.baseFunctionParserFunctions[level].length];
          cf.baseFunctionList[level].put(dataCollector, tmpList);
          for (int i = 0; i < cf.baseFunctionParserFunctions[level].length; i++) {
            try {
              tmpList[i] = new SubComponentFunction(
                  DataCollector.COLLECTOR_TYPE_LIST,
                  cf.baseFunctionKeys[level][i], cf.baseFunctionTypes[level][i],
                  cf.baseFunctionParserFunctions[level][i], null, null, 0,
                  Integer.MAX_VALUE, null, null);

            } catch (ParseException e) {
              throw new IOException(e.getMessage());
            }
          }
        } else {
          tmpList = cf.baseFunctionList[level].get(dataCollector);
        }
        for (SubComponentFunction function : tmpList) {
          function.dataCollector.initNewList(1);
        }
      }
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
            if (cf.baseDataTypes[level].equals(CodecUtil.DATA_TYPE_LONG)) {
              // sumrule
              if (cf.baseStatsTypes[level].equals(CodecUtil.STATS_BASIC)
                  && cf.baseParsers[level].sumRule()
                  && (cf.baseMinimumLongs[level] == null)
                  && (cf.baseMaximumLongs[level] == null)) {
                for (String key : list.keySet()) {
                  if (docLists.get(key).length > 0) {
                    // initialise
                    String[] keys = new String[] { key };
                    Integer[] subDocSet = docLists.get(key);
                    int length = cf.baseParsers[level].needArgumentsNumber();
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
                      long value;
                      try {
                        value = cf.baseParsers[level].getValueLong(valueSum,
                            valuePositions);
                        subDataCollectors = dataCollector.add(keys, value,
                            subDocSet.length);
                      } catch (IOException e) {
                        dataCollector.error(keys, e.getMessage());
                        subDataCollectors = null;
                      }
                      if (cf.baseFunctionList[level] != null
                          && cf.baseFunctionList[level]
                              .containsKey(dataCollector)) {
                        SubComponentFunction[] functionList = cf.baseFunctionList[level]
                            .get(dataCollector);
                        for (SubComponentFunction function : functionList) {
                          if (function.dataType
                              .equals(CodecUtil.DATA_TYPE_LONG)) {
                            try {
                              long valueLong = function.parserFunction
                                  .getValueLong(valueSum, valuePositions);
                              function.dataCollector.add(keys, valueLong,
                                  subDocSet.length);
                            } catch (IOException e) {
                              function.dataCollector.error(keys,
                                  e.getMessage());
                            }
                          } else if (function.dataType
                              .equals(CodecUtil.DATA_TYPE_DOUBLE)) {
                            try {
                              double valueDouble = function.parserFunction
                                  .getValueDouble(valueSum, valuePositions);
                              function.dataCollector.add(keys, valueDouble,
                                  subDocSet.length);
                            } catch (IOException e) {
                              function.dataCollector.error(keys,
                                  e.getMessage());
                            }
                          }
                        }
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
                    }
                  }
                }
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
                        // check for functions
                        long[][] functionValuesLong = null;
                        double[][] functionValuesDouble = null;
                        int[] functionNumber = null;
                        SubComponentFunction[] functionList = null;
                        if (cf.baseFunctionList[level] != null
                            && cf.baseFunctionList[level]
                                .containsKey(dataCollector)) {
                          functionList = cf.baseFunctionList[level]
                              .get(dataCollector);
                          functionValuesLong = new long[functionList.length][];
                          functionValuesDouble = new double[functionList.length][];
                          functionNumber = new int[functionList.length];
                          for (int i = 0; i < functionList.length; i++) {
                            functionValuesLong[i] = new long[subDocSet.length];
                            functionValuesDouble[i] = new double[subDocSet.length];
                          }
                        }
                        // check main
                        int number = 0;
                        Integer[] restrictedSubDocSet = new Integer[subDocSet.length];
                        long[] values = new long[subDocSet.length];
                        for (int docId : subDocSet) {
                          try {
                            long[] tmpArgs = args.get(docId);
                            int tmpPositions = (positionsData == null) ? 0
                                : positionsData.get(docId);
                            long value = cf.baseParsers[level]
                                .getValueLong(tmpArgs, tmpPositions);
                            if ((cf.baseMinimumLongs[level] == null
                                || value >= cf.baseMinimumLongs[level])
                                && (cf.baseMaximumLongs[level] == null
                                    || value <= cf.baseMaximumLongs[level])) {
                              values[number] = value;
                              restrictedSubDocSet[number] = docId;
                              number++;
                              if (functionList != null) {
                                for (int i = 0; i < functionList.length; i++) {
                                  SubComponentFunction function = functionList[i];
                                  if (function.dataType
                                      .equals(CodecUtil.DATA_TYPE_LONG)) {
                                    try {
                                      functionValuesLong[i][functionNumber[i]] = function.parserFunction
                                          .getValueLong(tmpArgs, tmpPositions);
                                      functionNumber[i]++;
                                    } catch (IOException e) {
                                      function.dataCollector.error(keys,
                                          e.getMessage());
                                    }
                                  } else if (function.dataType
                                      .equals(CodecUtil.DATA_TYPE_DOUBLE)) {
                                    try {
                                      functionValuesDouble[i][functionNumber[i]] = function.parserFunction
                                          .getValueDouble(tmpArgs,
                                              tmpPositions);
                                      functionNumber[i]++;
                                    } catch (IOException e) {
                                      function.dataCollector.error(keys,
                                          e.getMessage());
                                    }
                                  }
                                }
                              }
                            }
                          } catch (IOException e) {
                            dataCollector.error(keys, e.getMessage());
                          }
                        }
                        if (number > 0) {
                          subDataCollectors = dataCollector.add(keys, values,
                              number);
                          if (cf.baseFunctionList[level] != null
                              && cf.baseFunctionList[level]
                                  .containsKey(dataCollector)) {
                            for (int i = 0; i < functionList.length; i++) {
                              SubComponentFunction function = functionList[i];
                              if (function.dataType
                                  .equals(CodecUtil.DATA_TYPE_LONG)) {
                                function.dataCollector.add(keys,
                                    functionValuesLong[i], functionNumber[i]);
                              } else if (function.dataType
                                  .equals(CodecUtil.DATA_TYPE_DOUBLE)) {
                                function.dataCollector.add(keys,
                                    functionValuesDouble[i], functionNumber[i]);
                              }
                            }
                          }
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
                  "unexpected dataType " + cf.baseDataTypes[level]);
            }
          }
        }
      } else {
        throw new IOException(
            "unexpected type " + dataCollector.getCollectorType());
      }
      dataCollector.closeNewList();
      if (cf.baseFunctionList[level] != null
          && cf.baseFunctionList[level].containsKey(dataCollector)) {
        SubComponentFunction[] tmpList = cf.baseFunctionList[level]
            .get(dataCollector);
        for (SubComponentFunction function : tmpList) {
          function.dataCollector.closeNewList();
        }
      }
    }

  }

  /**
   * Creates the facet.
   *
   * @param facetList
   *          the facet list
   * @param positionsData
   *          the positions data
   * @param spansNumberData
   *          the spans number data
   * @param facetData
   *          the facet data
   * @param docSet
   *          the doc set
   * @param field
   *          the field
   * @param docBase
   *          the doc base
   * @param uniqueKeyField
   *          the unique key field
   * @param mtasCodecInfo
   *          the mtas codec info
   * @param searcher
   *          the searcher
   * @throws IOException
   *           Signals that an I/O exception has occurred.
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

  /**
   * Creates the termvector.
   *
   * @param termVectorList
   *          the term vector list
   * @param positionsData
   *          the positions data
   * @param docSet
   *          the doc set
   * @param field
   *          the field
   * @param t
   *          the t
   * @param r
   *          the r
   * @param lrc
   *          the lrc
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private static void createTermvector(List<ComponentTermVector> termVectorList,
      HashMap<Integer, Integer> positionsData, List<Integer> docSet,
      String field, Terms t, LeafReader r, LeafReaderContext lrc)
      throws IOException {
    createTermvectorFirstRound(termVectorList, positionsData, docSet, field, t,
        r, lrc);
  }

  /**
   * Creates the termvector first round.
   *
   * @param termVectorList
   *          the term vector list
   * @param positionsData
   *          the positions data
   * @param docSet
   *          the doc set
   * @param field
   *          the field
   * @param t
   *          the t
   * @param r
   *          the r
   * @param lrc
   *          the lrc
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private static void createTermvectorFirstRound(
      List<ComponentTermVector> termVectorList,
      HashMap<Integer, Integer> positionsData, List<Integer> docSet,
      String field, Terms t, LeafReader r, LeafReaderContext lrc)
      throws IOException {
    if (t != null) {
      BytesRef term;
      TermsEnum termsEnum;
      PostingsEnum postingsEnum = null;
      String segmentName = "segment" + lrc.ord;
      int segmentNumber = lrc.parent.leaves().size();
      // loop over termvectors
      for (ComponentTermVector termVector : termVectorList) {
        termsEnum = t.intersect(termVector.compiledAutomaton, null);
        termVector.subComponentFunction.dataCollector.initNewList(
            (int) t.size(), segmentName, segmentNumber, termVector.boundary);
        if (termVector.functions != null) {
          for (SubComponentFunction function : termVector.functions) {
            function.dataCollector.initNewList((int) t.size());
          }
        }
        // only if documents
        if (docSet.size() > 0) {
          int termDocId;
          int termNumberMaximum = termVector.number;
          HashMap<BytesRef, RegisterStatus> computeFullList = new HashMap<BytesRef, RegisterStatus>();
          RegisterStatus registerStatus;
          // basic, don't need full values
          if (termVector.subComponentFunction.sortType
              .equals(CodecUtil.SORT_TERM)
              || termVector.subComponentFunction.sortType
                  .equals(CodecUtil.STATS_TYPE_SUM)
              || termVector.subComponentFunction.sortType
                  .equals(CodecUtil.STATS_TYPE_N)) {
            int termCounter = 0;
            // loop over terms
            while ((term = termsEnum.next()) != null) {
              termDocId = -1;
              // compute numbers;
              TermvectorNumberBasic numberBasic = computeTermvectorNumberBasic(
                  docSet, termDocId, termsEnum, r, lrc, postingsEnum);
              // register
              if (numberBasic.docNumber > 0) {
                termCounter++;
                registerStatus = registerValue(term, termVector, numberBasic,
                    termNumberMaximum, segmentNumber, false);
                if (registerStatus != null) {
                  computeFullList.put(BytesRef.deepCopyOf(term),
                      registerStatus);
                }
              }
              // stop after termCounterMaximum
              if (termVector.subComponentFunction.sortType.equals(
                  CodecUtil.SORT_TERM) && termCounter >= termNumberMaximum) {
                break;
              }
            }
            // rerun for full
            if (computeFullList.size() > 0) {
              termsEnum = t.intersect(termVector.compiledAutomaton, null);
              while ((term = termsEnum.next()) != null) {
                termDocId = -1;
                // only if (probably) needed
                if (computeFullList.containsKey(term)) {
                  registerStatus = computeFullList.get(term);
                  if (termVector.subComponentFunction.sortType
                      .equals(CodecUtil.SORT_TERM) || termVector.list != null
                      || termVector.boundaryRegistration || registerStatus.force
                      || termVector.subComponentFunction.dataCollector
                          .validateSegmentBoundary(registerStatus.sortValue)) {
                    TermvectorNumberFull numberFull = computeTermvectorNumberFull(
                        docSet, termDocId, termsEnum, r, lrc, postingsEnum,
                        positionsData);
                    if (numberFull.docNumber > 0) {
                      termCounter++;
                      registerValue(term, termVector, numberFull,
                          termNumberMaximum, segmentNumber);
                    }
                  }
                }
              }
              computeFullList.clear();
            }
          } else {
            throw new IOException(
                "sort '" + termVector.subComponentFunction.sortType + " "
                    + termVector.subComponentFunction.sortDirection
                    + "' not supported");
          }
          // finish if segments are used
          termVector.subComponentFunction.dataCollector
              .closeSegmentKeyValueRegistration();
          if (termVector.functions != null) {
            for (SubComponentFunction function : termVector.functions) {
              function.dataCollector.closeSegmentKeyValueRegistration();
            }
          }

        }
        termVector.subComponentFunction.dataCollector.closeNewList();
        if (termVector.functions != null) {
          for (SubComponentFunction function : termVector.functions) {
            function.dataCollector.closeNewList();
          }
        }
      }
    }

  }

  /**
   * Creates the termvector second round.
   *
   * @param termVectorList
   *          the term vector list
   * @param positionsData
   *          the positions data
   * @param docSet
   *          the doc set
   * @param field
   *          the field
   * @param t
   *          the t
   * @param r
   *          the r
   * @param lrc
   *          the lrc
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private static void createTermvectorSecondRound(
      List<ComponentTermVector> termVectorList,
      HashMap<Integer, Integer> positionsData, List<Integer> docSet,
      String field, Terms t, LeafReader r, LeafReaderContext lrc)
      throws IOException {
    if (t != null) {
      BytesRef term;
      TermsEnum termsEnum;
      PostingsEnum postingsEnum = null;
      String segmentName = "segment" + lrc.ord;
      int segmentNumber = lrc.parent.leaves().size();
      for (ComponentTermVector termVector : termVectorList) {
        if (termVector.subComponentFunction.dataCollector.segmentRecomputeKeyList != null
            && termVector.subComponentFunction.dataCollector.segmentRecomputeKeyList
                .containsKey(segmentName)) {
          HashSet<String> recomputeKeyList = termVector.subComponentFunction.dataCollector.segmentRecomputeKeyList
              .get(segmentName);
          if (recomputeKeyList.size() > 0) {
            List<CompiledAutomaton> listCompiledAutomata = MtasToken
                .createAutomata(termVector.prefix,
                    new ArrayList<String>(recomputeKeyList));
            for (CompiledAutomaton compiledAutomaton : listCompiledAutomata) {
              termsEnum = t.intersect(compiledAutomaton, null);
              termVector.subComponentFunction.dataCollector
                  .initNewList(
                      termVector.subComponentFunction.dataCollector.segmentKeys
                          .size(),
                      segmentName, segmentNumber, termVector.boundary);
              RegisterStatus registerStatus = null;
              if (termVector.functions != null) {
                for (SubComponentFunction function : termVector.functions) {
                  function.dataCollector.initNewList((int) t.size(),
                      segmentName, segmentNumber, null);
                }
              }
              if (docSet.size() > 0) {
                int termDocId;
                while ((term = termsEnum.next()) != null) {
                  termDocId = -1;
                  // compute numbers;
                  TermvectorNumberBasic numberBasic = computeTermvectorNumberBasic(
                      docSet, termDocId, termsEnum, r, lrc, postingsEnum);
                  if (numberBasic.docNumber > 0) {
                    registerStatus = registerValue(term, termVector,
                        numberBasic, 0, segmentNumber, true);
                    if (registerStatus != null) {
                      TermvectorNumberFull numberFull = computeTermvectorNumberFull(
                          docSet, termDocId, termsEnum, r, lrc, postingsEnum,
                          positionsData);
                      if (numberFull.docNumber > 0) {
                        registerValue(term, termVector, numberFull, 0,
                            segmentNumber);
                      }
                    }
                  }
                }
              }
              termVector.subComponentFunction.dataCollector.closeNewList();
              if (termVector.functions != null) {
                for (SubComponentFunction function : termVector.functions) {
                  function.dataCollector.closeNewList();
                }
              }
            }
          }
        }
      }
    }

  }

  /**
   * Need second round termvector.
   *
   * @param termVectorList
   *          the term vector list
   * @return true, if successful
   * @throws IOException
   */
  private static boolean needSecondRoundTermvector(
      List<ComponentTermVector> termVectorList) throws IOException {
    boolean needSecondRound = false;
    for (ComponentTermVector termVector : termVectorList) {
      if (termVector.subComponentFunction.dataCollector.segmentRegistration != null
          && (termVector.subComponentFunction.dataCollector.segmentRegistration
              .equals(MtasDataCollector.SEGMENT_SORT_ASC)
              || termVector.subComponentFunction.dataCollector.segmentRegistration
                  .equals(MtasDataCollector.SEGMENT_SORT_DESC))
          && termVector.number > 0) {
        termVector.subComponentFunction.dataCollector.recomputeSegmentKeys();
        if (!termVector.subComponentFunction.dataCollector
            .checkExistenceNecessaryKeys()) {
          needSecondRound = true;
        }
        termVector.subComponentFunction.dataCollector.reduceToSegmentKeys();
      } else if (termVector.subComponentFunction.dataCollector.segmentRegistration != null
          && (termVector.subComponentFunction.dataCollector.segmentRegistration
              .equals(MtasDataCollector.SEGMENT_BOUNDARY_ASC)
              || termVector.subComponentFunction.dataCollector.segmentRegistration
                  .equals(MtasDataCollector.SEGMENT_BOUNDARY_DESC))
          && termVector.number > 0) {
        termVector.subComponentFunction.dataCollector.recomputeSegmentKeys();
        if (!termVector.subComponentFunction.dataCollector
            .checkExistenceNecessaryKeys()) {
          needSecondRound = true;
        }
        termVector.subComponentFunction.dataCollector.reduceToSegmentKeys();
      }      
    }    
    return needSecondRound;
  }

  /**
   * The Class TermvectorNumberBasic.
   */
  private static class TermvectorNumberBasic {

    /** The value sum. */
    public long[] valueSum;

    /** The doc number. */
    public int docNumber;

    /**
     * Instantiates a new termvector number basic.
     */
    TermvectorNumberBasic() {
      valueSum = new long[] { 0 };
      docNumber = 0;
    }
  }

  /**
   * The Class TermvectorNumberFull.
   */
  private static class TermvectorNumberFull {

    /** The args. */
    public long[] args;

    /** The positions. */
    public int[] positions;

    /** The doc number. */
    public int docNumber;

    /**
     * Instantiates a new termvector number full.
     *
     * @param maxSize
     *          the max size
     */
    TermvectorNumberFull(int maxSize) {
      args = new long[maxSize];
      positions = new int[maxSize];
      docNumber = 0;
    }
  }

  private static class RegisterStatus {
    public long sortValue;
    public boolean force;

    RegisterStatus(long sortValue, boolean force) {
      this.sortValue = sortValue;
      this.force = force;
    }
  }

  /**
   * Register basic value.
   *
   * @param keys
   *          the keys
   * @param termVector
   *          the term vector
   * @param number
   *          the numbers
   * @param termNumberMaximum
   *          the term number maximum
   * @param segmentNumber
   *          the segment number
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  @SuppressWarnings("unchecked")
  private static RegisterStatus registerValue(BytesRef term,
      ComponentTermVector termVector, TermvectorNumberBasic number,
      Integer termNumberMaximum, Integer segmentNumber, boolean forceAccept)
      throws IOException {
    long value = termVector.subComponentFunction.parserFunction
        .getValueLong(number.valueSum, 0);
    long sortValue = 0;
    if (termVector.subComponentFunction.sortType
        .equals(CodecUtil.STATS_TYPE_SUM)) {
      sortValue = value;
    } else if (termVector.subComponentFunction.sortType
        .equals(CodecUtil.STATS_TYPE_N)) {
      sortValue = Long.valueOf(number.docNumber);
    }
    boolean addItem = false, addItemForced = false;
    String key = null;
    MtasDataCollector<Long, ?> dataCollector = (MtasDataCollector<Long, ?>) termVector.subComponentFunction.dataCollector;
    if (termVector.subComponentFunction.sortType.equals(CodecUtil.SORT_TERM)) {
      if (forceAccept) {
        addItem = true;
      } else if (termVector.list != null) {
        key = MtasToken.getPostfixFromValue(term);
        addItem = termVector.list.contains(key);
      } else {
        addItem = true;
      }
      addItemForced = addItem;
    } else if (termVector.subComponentFunction.sortType
        .equals(CodecUtil.STATS_TYPE_SUM)
        || termVector.subComponentFunction.sortType
            .equals(CodecUtil.STATS_TYPE_N)) {
      if (forceAccept) {
        addItem = true;
        addItemForced = addItem;
      } else if (termVector.list != null) {
        key = MtasToken.getPostfixFromValue(term);
        addItem = termVector.list.contains(key);
        addItemForced = addItem;
      } else if (termVector.boundaryRegistration) {
        addItem = dataCollector.validateSegmentBoundary(sortValue);
        if (addItem) {
          key = MtasToken.getPostfixFromValue(term);
          String segmentStatus = dataCollector.validateSegmentValue(key,
              sortValue, termNumberMaximum, segmentNumber);
          if (segmentStatus != null) {
            addItem = true;
            if (segmentStatus.equals(MtasDataCollector.SEGMENT_KEY)) {
              addItemForced = addItem;
            }
          } else {
            // shouldn't happen
          }
        }
      } else {
        key = MtasToken.getPostfixFromValue(term);
        String segmentStatus = dataCollector.validateSegmentValue(key,
            sortValue, termNumberMaximum, segmentNumber);
        if (segmentStatus != null) {
          addItem = true;
          if (segmentStatus.equals(MtasDataCollector.SEGMENT_KEY)) {
            addItemForced = addItem;
          }
        }
      }
    } else {
      addItem = false;
    }
    if (addItem) {
      boolean computeFull = false;
      if (key == null) {
        key = MtasToken.getPostfixFromValue(term);
      }
      if (termVector.subComponentFunction.statsType
          .equals(CodecUtil.STATS_BASIC)) {
        dataCollector.add(new String[] { key }, value, number.docNumber);
      } else {
        computeFull = true;
      }
      if (termVector.functions != null) {
        for (SubComponentFunction function : termVector.functions) {
          if (function.parserFunction.sumRule()
              && !function.parserFunction.needPositions()
              && function.statsType.equals(CodecUtil.STATS_BASIC)) {
            if (function.dataType.equals(CodecUtil.DATA_TYPE_LONG)) {
              long valueFunction = function.parserFunction
                  .getValueLong(number.valueSum, 0);
              function.dataCollector.add(new String[] { key }, valueFunction,
                  number.docNumber);
            } else if (function.dataType.equals(CodecUtil.DATA_TYPE_DOUBLE)) {
              double valueFunction = function.parserFunction
                  .getValueDouble(number.valueSum, 0);
              function.dataCollector.add(new String[] { key }, valueFunction,
                  number.docNumber);
            }
          } else {
            computeFull = true;
          }
        }
      }
      return computeFull ? new RegisterStatus(sortValue, addItemForced) : null;
    } else {
      return null;
    }
  }

  /**
   * Register full values.
   *
   * @param keys
   *          the keys
   * @param termVector
   *          the term vector
   * @param number
   *          the numbers
   * @param termNumberMaximum
   *          the term number maximum
   * @param segmentNumber
   *          the segment number
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  @SuppressWarnings("unchecked")
  private static void registerValue(BytesRef term,
      ComponentTermVector termVector, TermvectorNumberFull number,
      Integer termNumberMaximum, Integer segmentNumber) throws IOException {
    if (number.docNumber > 0) {
      String key = MtasToken.getPostfixFromValue(term);
      MtasDataCollector<Long, ?> dataCollector = (MtasDataCollector<Long, ?>) termVector.subComponentFunction.dataCollector;
      long[] valuesLong = new long[number.docNumber];
      for (int i = 0; i < number.docNumber; i++) {
        try {
          valuesLong[i] = termVector.subComponentFunction.parserFunction
              .getValueLong(new long[] { number.args[i] }, number.positions[i]);
        } catch (IOException e) {
          dataCollector.error(new String[] { key }, e.getMessage());
        }
      }
      if (!termVector.subComponentFunction.statsType
          .equals(CodecUtil.STATS_BASIC)) {
        dataCollector.add(new String[] { key }, valuesLong, valuesLong.length);
      }
      for (SubComponentFunction function : termVector.functions) {
        if (!function.parserFunction.sumRule()
            || function.parserFunction.needPositions()
            || !function.statsType.equals(CodecUtil.STATS_BASIC)) {
          if (function.dataType.equals(CodecUtil.DATA_TYPE_LONG)) {
            valuesLong = new long[number.docNumber];
            for (int i = 0; i < number.docNumber; i++) {
              try {
                valuesLong[i] = function.parserFunction.getValueLong(
                    new long[] { number.args[i] }, number.positions[i]);
              } catch (IOException e) {
                function.dataCollector.error(new String[] { key },
                    e.getMessage());
              }
            }
            function.dataCollector.add(new String[] { key }, valuesLong,
                valuesLong.length);
          } else if (function.dataType.equals(CodecUtil.DATA_TYPE_DOUBLE)) {
            double[] valuesDouble = new double[number.docNumber];
            for (int i = 0; i < number.docNumber; i++) {
              try {
                valuesDouble[i] = function.parserFunction.getValueDouble(
                    new long[] { number.args[i] }, number.positions[i]);
              } catch (IOException e) {
                function.dataCollector.error(new String[] { key },
                    e.getMessage());
              }
            }
            function.dataCollector.add(new String[] { key }, valuesDouble,
                valuesDouble.length);
          }
        }
      }

    }

  }

  /**
   * Compute termvector number basic.
   *
   * @param docSet
   *          the doc set
   * @param termDocId
   *          the term doc id
   * @param termsEnum
   *          the terms enum
   * @param r
   *          the r
   * @param lrc
   *          the lrc
   * @param postingsEnum
   *          the postings enum
   * @return the termvector number basic
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private static TermvectorNumberBasic computeTermvectorNumberBasic(
      List<Integer> docSet, int termDocId, TermsEnum termsEnum, LeafReader r,
      LeafReaderContext lrc, PostingsEnum postingsEnum) throws IOException {
    TermvectorNumberBasic result = new TermvectorNumberBasic();
    boolean hasDeletedDocuments = (r.getLiveDocs() != null);
    boolean subtractValue = false;
    if ((docSet.size() == r.numDocs()) && !hasDeletedDocuments) {
      result.valueSum[0] = termsEnum.totalTermFreq();
      result.docNumber = termsEnum.docFreq();
      if (result.valueSum[0] > -1) {
        return result;
      }
    }
    if (docSet.size() > Math.round(r.numDocs() / 2) && !hasDeletedDocuments) {
      result.valueSum[0] = termsEnum.totalTermFreq();
      subtractValue = true;
    }
    Iterator<Integer> docIterator = docSet.iterator();
    postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.FREQS);
    while (docIterator.hasNext()) {
      int docId = docIterator.next() - lrc.docBase;
      if (docId >= termDocId) {
        if ((docId == termDocId)
            || ((termDocId = postingsEnum.advance(docId)) == docId)) {
          result.docNumber++;
          if (subtractValue) {
            result.valueSum[0] -= postingsEnum.freq();
          } else {
            result.valueSum[0] += postingsEnum.freq();
          }
        }
      }
    }
    return result;
  }

  /**
   * Compute termvector number full.
   *
   * @param docSet
   *          the doc set
   * @param termDocId
   *          the term doc id
   * @param termsEnum
   *          the terms enum
   * @param r
   *          the r
   * @param lrc
   *          the lrc
   * @param postingsEnum
   *          the postings enum
   * @param positionsData
   *          the positions data
   * @return the termvector number full
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private static TermvectorNumberFull computeTermvectorNumberFull(
      List<Integer> docSet, int termDocId, TermsEnum termsEnum, LeafReader r,
      LeafReaderContext lrc, PostingsEnum postingsEnum,
      HashMap<Integer, Integer> positionsData) throws IOException {
    TermvectorNumberFull result = new TermvectorNumberFull(docSet.size());
    Iterator<Integer> docIterator = docSet.iterator();
    postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.FREQS);
    while (docIterator.hasNext()) {
      int docId = docIterator.next() - lrc.docBase;
      if (docId >= termDocId) {
        if ((docId == termDocId)
            || ((termDocId = postingsEnum.advance(docId)) == docId)) {
          result.args[result.docNumber] = postingsEnum.freq();
          result.positions[result.docNumber] = (positionsData == null) ? 0
              : positionsData.get(docId + lrc.docBase);
          result.docNumber++;
        }
      }
    }

    return result;
  }

}
