package io.univalence.education_spark._20_spark_sql;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Cast;
import org.apache.spark.unsafe.types.UTF8String;

public class _30_codegen_result {
/* 001 */ public Object generate(Object[] references) {
/* 002 */   return new GeneratedIteratorForCodegenStage1(references);
/* 003 */ }
/* 004 */
/* 005 */ // codegenStageId=1
/* 006 */ final class GeneratedIteratorForCodegenStage1 extends org.apache.spark.sql.execution.BufferedRowIterator {
/* 007 */   private Object[] references;
/* 008 */   private scala.collection.Iterator[] inputs;
/* 009 */   private scala.collection.Iterator inputadapter_input_0;
/* 010 */   private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[] filter_mutableStateArray_0 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[1];
/* 011 */
/* 012 */   public GeneratedIteratorForCodegenStage1(Object[] references) {
/* 013 */     this.references = references;
/* 014 */   }
/* 015 */
/* 016 */   public void init(int index, scala.collection.Iterator[] inputs) {
/* 017 */     partitionIndex = index;
/* 018 */     this.inputs = inputs;
/* 019 */     inputadapter_input_0 = inputs[0];
/* 020 */     filter_mutableStateArray_0[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(10, 320);
/* 021 */
/* 022 */   }
/* 023 */
/* 024 */   protected void processNext() throws java.io.IOException {
/* 025 */     while ( inputadapter_input_0.hasNext()) {
/* 026 */       InternalRow inputadapter_row_0 = (InternalRow) inputadapter_input_0.next();
/* 027 */
/* 028 */       do {
/* 029 */         boolean inputadapter_isNull_1 = inputadapter_row_0.isNullAt(1);
/* 030 */         UTF8String inputadapter_value_1 = inputadapter_isNull_1 ?
/* 031 */         null : (inputadapter_row_0.getUTF8String(1));
/* 032 */
/* 033 */         boolean filter_value_2 = !inputadapter_isNull_1;
/* 034 */         if (!filter_value_2) continue;
/* 035 */
/* 036 */         boolean filter_isNull_2 = true;
/* 037 */         boolean filter_value_3 = false;
/* 038 */         boolean filter_isNull_3 = inputadapter_isNull_1;
/* 039 */         double filter_value_4 = -1.0;
/* 040 */         if (!inputadapter_isNull_1) {
/* 041 */           final String filter_doubleStr_0 = inputadapter_value_1.toString();
/* 042 */           try {
/* 043 */             filter_value_4 = Double.valueOf(filter_doubleStr_0);
/* 044 */           } catch (java.lang.NumberFormatException e) {
/* 045 */             final Double d = (Double) Cast.processFloatingPointSpecialLiterals(filter_doubleStr_0, false);
/* 046 */             if (d == null) {
/* 047 */               filter_isNull_3 = true;
/* 048 */             } else {
/* 049 */               filter_value_4 = d.doubleValue();
/* 050 */             }
/* 051 */           }
/* 052 */         }
/* 053 */         if (!filter_isNull_3) {
/* 054 */           filter_isNull_2 = false; // resultCode could change nullability.
/* 055 */           filter_value_3 = org.apache.spark.sql.catalyst.util.SQLOrderingUtil.compareDoubles(filter_value_4, 4.0D) >= 0;
/* 056 */
/* 057 */         }
/* 058 */         if (filter_isNull_2 || !filter_value_3) continue;
/* 059 */
/* 060 */         ((org.apache.spark.sql.execution.metric.SQLMetric) references[0] /* numOutputRows */).add(1);
/* 061 */
/* 062 */         boolean inputadapter_isNull_0 = inputadapter_row_0.isNullAt(0);
/* 063 */         UTF8String inputadapter_value_0 = inputadapter_isNull_0 ?
/* 064 */         null : (inputadapter_row_0.getUTF8String(0));
/* 065 */         boolean inputadapter_isNull_2 = inputadapter_row_0.isNullAt(2);
/* 066 */         UTF8String inputadapter_value_2 = inputadapter_isNull_2 ?
/* 067 */         null : (inputadapter_row_0.getUTF8String(2));
/* 068 */         boolean inputadapter_isNull_3 = inputadapter_row_0.isNullAt(3);
/* 069 */         UTF8String inputadapter_value_3 = inputadapter_isNull_3 ?
/* 070 */         null : (inputadapter_row_0.getUTF8String(3));
/* 071 */         boolean inputadapter_isNull_4 = inputadapter_row_0.isNullAt(4);
/* 072 */         UTF8String inputadapter_value_4 = inputadapter_isNull_4 ?
/* 073 */         null : (inputadapter_row_0.getUTF8String(4));
/* 074 */         boolean inputadapter_isNull_5 = inputadapter_row_0.isNullAt(5);
/* 075 */         UTF8String inputadapter_value_5 = inputadapter_isNull_5 ?
/* 076 */         null : (inputadapter_row_0.getUTF8String(5));
/* 077 */         boolean inputadapter_isNull_6 = inputadapter_row_0.isNullAt(6);
/* 078 */         UTF8String inputadapter_value_6 = inputadapter_isNull_6 ?
/* 079 */         null : (inputadapter_row_0.getUTF8String(6));
/* 080 */         boolean inputadapter_isNull_7 = inputadapter_row_0.isNullAt(7);
/* 081 */         UTF8String inputadapter_value_7 = inputadapter_isNull_7 ?
/* 082 */         null : (inputadapter_row_0.getUTF8String(7));
/* 083 */         boolean inputadapter_isNull_8 = inputadapter_row_0.isNullAt(8);
/* 084 */         UTF8String inputadapter_value_8 = inputadapter_isNull_8 ?
/* 085 */         null : (inputadapter_row_0.getUTF8String(8));
/* 086 */         boolean inputadapter_isNull_9 = inputadapter_row_0.isNullAt(9);
/* 087 */         UTF8String inputadapter_value_9 = inputadapter_isNull_9 ?
/* 088 */         null : (inputadapter_row_0.getUTF8String(9));
/* 089 */         filter_mutableStateArray_0[0].reset();
/* 090 */
/* 091 */         filter_mutableStateArray_0[0].zeroOutNullBytes();
/* 092 */
/* 093 */         if (inputadapter_isNull_0) {
/* 094 */           filter_mutableStateArray_0[0].setNullAt(0);
/* 095 */         } else {
/* 096 */           filter_mutableStateArray_0[0].write(0, inputadapter_value_0);
/* 097 */         }
/* 098 */
/* 099 */         filter_mutableStateArray_0[0].write(1, inputadapter_value_1);
/* 100 */
/* 101 */         if (inputadapter_isNull_2) {
/* 102 */           filter_mutableStateArray_0[0].setNullAt(2);
/* 103 */         } else {
/* 104 */           filter_mutableStateArray_0[0].write(2, inputadapter_value_2);
/* 105 */         }
/* 106 */
/* 107 */         if (inputadapter_isNull_3) {
/* 108 */           filter_mutableStateArray_0[0].setNullAt(3);
/* 109 */         } else {
/* 110 */           filter_mutableStateArray_0[0].write(3, inputadapter_value_3);
/* 111 */         }
/* 112 */
/* 113 */         if (inputadapter_isNull_4) {
/* 114 */           filter_mutableStateArray_0[0].setNullAt(4);
/* 115 */         } else {
/* 116 */           filter_mutableStateArray_0[0].write(4, inputadapter_value_4);
/* 117 */         }
/* 118 */
/* 119 */         if (inputadapter_isNull_5) {
/* 120 */           filter_mutableStateArray_0[0].setNullAt(5);
/* 121 */         } else {
/* 122 */           filter_mutableStateArray_0[0].write(5, inputadapter_value_5);
/* 123 */         }
/* 124 */
/* 125 */         if (inputadapter_isNull_6) {
/* 126 */           filter_mutableStateArray_0[0].setNullAt(6);
/* 127 */         } else {
/* 128 */           filter_mutableStateArray_0[0].write(6, inputadapter_value_6);
/* 129 */         }
/* 130 */
/* 131 */         if (inputadapter_isNull_7) {
/* 132 */           filter_mutableStateArray_0[0].setNullAt(7);
/* 133 */         } else {
/* 134 */           filter_mutableStateArray_0[0].write(7, inputadapter_value_7);
/* 135 */         }
/* 136 */
/* 137 */         if (inputadapter_isNull_8) {
/* 138 */           filter_mutableStateArray_0[0].setNullAt(8);
/* 139 */         } else {
/* 140 */           filter_mutableStateArray_0[0].write(8, inputadapter_value_8);
/* 141 */         }
/* 142 */
/* 143 */         if (inputadapter_isNull_9) {
/* 144 */           filter_mutableStateArray_0[0].setNullAt(9);
/* 145 */         } else {
/* 146 */           filter_mutableStateArray_0[0].write(9, inputadapter_value_9);
/* 147 */         }
/* 148 */         append((filter_mutableStateArray_0[0].getRow()));
/* 149 */
/* 150 */       } while(false);
/* 151 */       if (shouldStop()) return;
/* 152 */     }
/* 153 */   }
/* 154 */
/* 155 */ }
}
