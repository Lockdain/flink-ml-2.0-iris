package ru.asergeenko.flink.ml.iris;

import org.apache.flink.ml.classification.logisticregression.LogisticRegression;
import org.apache.flink.ml.classification.logisticregression.LogisticRegressionModel;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.functions.ScalarFunction;

public class App {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE iris_csv " +
                "(" +
                "sepal_length DOUBLE," +
                "sepal_width DOUBLE," +
                "petal_length DOUBLE," +
                "petal_width DOUBLE," +
                "species DOUBLE" +
                ") WITH (" +
                "'connector'='filesystem'," +
                "'path'='src/main/resources/iris_dataset0.csv'," +
                "'format'='csv'," +
                "'csv.ignore-parse-errors'='true'," +
                "'csv.allow-comments'='true'" +
                ")"
        );

        Table irisCsvTable = tableEnv.from("iris_csv");
        irisCsvTable.execute();
        ScalarFunction dense = new DenseVectorMapFunction();


        tableEnv.createTemporarySystemFunction("dense", dense);

        Table features = tableEnv.sqlQuery("SELECT dense(sepal_length, sepal_width, petal_length, petal_width) as features, species as labels FROM iris_csv");
        features.execute().print();
        //virginica = 3; versicolor = 2; setosa = 1;
        LogisticRegression logisticRegression = new LogisticRegression()
                .setFeaturesCol("features")
                .setLabelCol("labels")
                .setMaxIter(15);

        LogisticRegressionModel model = logisticRegression.fit(features);


        Table output = model.transform(features)[0];
        Table features1 = output.select("features");
        TableResult result = features1.execute();
        result.print();
    }
}
