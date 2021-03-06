package ru.asergeenko.flink.ml.iris;

import org.apache.flink.ml.classification.knn.Knn;
import org.apache.flink.ml.classification.knn.KnnModel;
import org.apache.flink.ml.classification.logisticregression.LogisticRegression;
import org.apache.flink.ml.classification.logisticregression.LogisticRegressionModel;
import org.apache.flink.ml.classification.naivebayes.NaiveBayes;
import org.apache.flink.ml.classification.naivebayes.NaiveBayesModel;
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

        tableEnv.executeSql("CREATE TABLE iris_csv_train " +
                "(" +
                "sepal_length DOUBLE," +
                "sepal_width DOUBLE," +
                "petal_length DOUBLE," +
                "petal_width DOUBLE," +
                "species DOUBLE" +
                ") WITH (" +
                "'connector'='filesystem'," +
                "'path'='src/main/resources/iris_dataset_train.csv'," +
                "'format'='csv'," +
                "'csv.ignore-parse-errors'='true'," +
                "'csv.allow-comments'='true'" +
                ")"
        );

        tableEnv.executeSql("CREATE TABLE iris_csv_validation " +
                "(" +
                "sepal_length DOUBLE," +
                "sepal_width DOUBLE," +
                "petal_length DOUBLE," +
                "petal_width DOUBLE," +
                "species DOUBLE" +
                ") WITH (" +
                "'connector'='filesystem'," +
                "'path'='src/main/resources/iris_dataset_validation.csv'," +
                "'format'='csv'," +
                "'csv.ignore-parse-errors'='true'," +
                "'csv.allow-comments'='true'" +
                ")"
        );

        ScalarFunction dense = new DenseVectorMapFunction();
        tableEnv.createTemporarySystemFunction("dense", dense);

        Table trainFeatures = tableEnv.sqlQuery("SELECT dense(sepal_length, sepal_width, petal_length, petal_width) as features, species as label FROM iris_csv_train");
        Table validationFeatures = tableEnv.sqlQuery("SELECT dense(sepal_length, sepal_width, petal_length, petal_width) as features FROM iris_csv_validation");
        trainFeatures.execute().print();
        validationFeatures.execute().print();

        //virginica = 3; versicolor = 2; setosa = 1;
        Knn knn = new Knn();
        KnnModel knnModel = knn.fit(trainFeatures);
        Table output = knnModel.transform(validationFeatures)[0];

        output.execute().print();

    }
}
