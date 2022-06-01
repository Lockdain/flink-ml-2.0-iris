package ru.asergeenko.flink.ml.iris;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.linalg.typeinfo.DenseVectorTypeInfo;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.extraction.TypeInferenceExtractor;
import org.apache.flink.table.types.inference.TypeInference;

import java.util.ArrayList;
import java.util.List;

public class DenseVectorMapFunction extends ScalarFunction {
    public @DataTypeHint("ARRAY<DOUBLE>")
    ArrayList<Double> eval(Double a, Double b, Double c, Double d) {
        ArrayList vector = new ArrayList<Double>(4);
        vector.add(a);
        vector.add(b);
        vector.add(c);
        vector.add(d);
        return vector;
    }
}
