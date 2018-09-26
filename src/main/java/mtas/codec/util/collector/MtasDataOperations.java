package mtas.codec.util.collector;

interface MtasDataOperations<T1 extends Number, T2 extends Number> {
  T1 product11(T1 arg1, T1 arg2);
  T1 add11(T1 arg1, T1 arg2);
  T2 add22(T2 arg1, T2 arg2);
  T2 subtract12(T1 arg1, T2 arg2);
  T2 divide1(T1 arg1, long arg2);
  T2 divide2(T2 arg1, long arg2);
  T2 exp2(T2 arg1);
  T2 sqrt2(T2 arg1);
  T2 log1(T1 arg1);
  T1 min11(T1 arg1, T1 arg2);
  T1 max11(T1 arg1, T1 arg2);
  T1[] createVector1(int length);
  T2[] createVector2(int length);
  T1[][] createMatrix1(int length);
  T1 getZero1();
  T2 getZero2();
}
