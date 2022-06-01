package ru.asergeenko.flink.ml.iris;

import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

public class DenseVectorMapFunction extends ScalarFunction {
    public @DataTypeHint(value = "RAW", bridgedTo = DenseVector.class)
    DenseVector eval(Double a, Double b, Double c, Double d) {
        return  Vectors.dense(a, b, c, d);
    }
}
