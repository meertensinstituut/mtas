package mtas.codec.util.collector;

/**
 * The Interface MtasDataOperations.
 *
 * @param <T1>
 *          the generic type
 * @param <T2>
 *          the generic type
 */
abstract interface MtasDataOperations<T1 extends Number, T2 extends Number> {

  /**
   * Product11.
   *
   * @param arg1
   *          the arg1
   * @param arg2
   *          the arg2
   * @return the t1
   */
  public T1 product11(T1 arg1, T1 arg2);

  /**
   * Add11.
   *
   * @param arg1
   *          the arg1
   * @param arg2
   *          the arg2
   * @return the t1
   */
  public T1 add11(T1 arg1, T1 arg2);

  /**
   * Add22.
   *
   * @param arg1
   *          the arg1
   * @param arg2
   *          the arg2
   * @return the t2
   */
  public T2 add22(T2 arg1, T2 arg2);

  /**
   * Subtract12.
   *
   * @param arg1
   *          the arg1
   * @param arg2
   *          the arg2
   * @return the t2
   */
  public T2 subtract12(T1 arg1, T2 arg2);

  /**
   * Divide1.
   *
   * @param arg1
   *          the arg1
   * @param arg2
   *          the arg2
   * @return the t2
   */
  public T2 divide1(T1 arg1, long arg2);

  /**
   * Divide2.
   *
   * @param arg1
   *          the arg1
   * @param arg2
   *          the arg2
   * @return the t2
   */
  public T2 divide2(T2 arg1, long arg2);

  /**
   * Exp2.
   *
   * @param arg1
   *          the arg1
   * @return the t2
   */
  public T2 exp2(T2 arg1);

  /**
   * Sqrt2.
   *
   * @param arg1
   *          the arg1
   * @return the t2
   */
  public T2 sqrt2(T2 arg1);

  /**
   * Log1.
   *
   * @param arg1
   *          the arg1
   * @return the t2
   */
  public T2 log1(T1 arg1);

  /**
   * Min11.
   *
   * @param arg1
   *          the arg1
   * @param arg2
   *          the arg2
   * @return the t1
   */
  public T1 min11(T1 arg1, T1 arg2);

  /**
   * Max11.
   *
   * @param arg1
   *          the arg1
   * @param arg2
   *          the arg2
   * @return the t1
   */
  public T1 max11(T1 arg1, T1 arg2);

  /**
   * Creates the vector1.
   *
   * @param length
   *          the length
   * @return the t1[]
   */
  public T1[] createVector1(int length);

  /**
   * Creates the vector2.
   *
   * @param length
   *          the length
   * @return the t2[]
   */
  public T2[] createVector2(int length);

  /**
   * Creates the matrix1.
   *
   * @param length
   *          the length
   * @return the t1[][]
   */
  public T1[][] createMatrix1(int length);

  /**
   * Gets the zero1.
   *
   * @return the zero1
   */
  public T1 getZero1();

  /**
   * Gets the zero2.
   *
   * @return the zero2
   */
  public T2 getZero2();

}
