package mtas.solr.search;

import org.apache.solr.search.SyntaxError;

/**
 * Reports a syntax error in CQL.
 * <p>
 * This is a separate class, so callers can distinguish CQL errors from Solr errors easily.
 */
class CQLSyntaxError extends SyntaxError {
  CQLSyntaxError(String msg) {
    super(msg);
  }

  CQLSyntaxError(Throwable e) {
    super(e);
  }
}
