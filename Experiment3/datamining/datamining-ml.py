from pyspark.sql import SparkSession
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark import SparkConf, SparkContext
spark = SparkSession.builder.master('local[1]').appName('learn_ml').getOrCreate()
def getData(Path):
    # 载入数据
    df0 = spark.read.csv(Path, header=True, inferSchema=True,encoding='utf-8')
    # 查看是否有缺失值
    # df0.toPandas().isna().sum()
    df0.toPandas().isna().values.any()
    # False 没有缺失值
    # df0.show(10, truncate=0)
    # 先使用StringIndexer将字符转化为数值，然后将特征整合到一起
    from pyspark.ml.feature import StringIndexer, VectorAssembler
    old_columns_names = df0.columns
    vecAss = VectorAssembler(inputCols=old_columns_names[1:4], outputCol='features')
    df0 = vecAss.transform(df0)
    # 更换label列名
    df0 = df0.withColumnRenamed(old_columns_names[4], 'label')
    # 创建新的只有label和features的表
    dfi = df0.select(['label', 'features'])
    # 数据概观
    dfi.show(10, truncate=0)
    return dfi
def printMetrics(result):
    ev = BinaryClassificationEvaluator(rawPredictionCol="probability")
    evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
    areaPR = ev.evaluate(result, {ev.metricName: "areaUnderPR"})
    areaROC = ev.evaluate(result, {ev.metricName: "areaUnderROC"})
    accuracy = evaluator.evaluate(result, {evaluator.metricName: "accuracy"})
    precision = evaluator.evaluate(result, {evaluator.metricName: "weightedPrecision"})
    recall = evaluator.evaluate(result, {evaluator.metricName: "weightedRecall"})
    print('areaUnderPR:', areaPR)
    print('areaUnderROC:', areaROC)
    print('Accuracy:', accuracy)
    print('precision:', precision)
    print('recall:', recall)
train_data=getData('file:///home/whw/PycharmProjects/spark-test/train_after.csv')
test_data=getData('file:///home/whw/PycharmProjects/spark-test/test_after.csv')

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import BinaryLogisticRegressionTrainingSummary
blor = LogisticRegression()
blorModel = blor.fit(train_data)
result = blorModel.transform(train_data)
predictresult=blorModel.transform(test_data)
predictresult.show()
print('LogistRegression:')
printMetrics(result)
print('\n')
# result.show()
# 计算准确率
# print(result.filter(result.label == result.prediction).count()/result.count())

from pyspark.ml.classification import DecisionTreeClassifier
dt = DecisionTreeClassifier(maxDepth=5,maxBins=1600)
dtModel = dt.fit(train_data)
result = dtModel.transform(train_data)
predictresult=dtModel.transform(test_data)
predictresult.show()
print('DecisionTree:')
printMetrics(result)
print('\n')
# accuracy
# print(result.filter(result.label == result.prediction).count()/result.count())

from pyspark.ml.classification import GBTClassifier
gbt = GBTClassifier(maxDepth=5,maxBins=1600)
gbtModel = gbt.fit(train_data)
result = gbtModel.transform(train_data)
predictresult=gbtModel.transform(test_data)
predictresult.show()
print('GBTClassifier:')
printMetrics(result)
print('\n')
# accuracy
# print(result.filter(result.label == result.prediction).count()/result.count())


from pyspark.ml.classification import RandomForestClassifier
rf = RandomForestClassifier(numTrees=10, maxDepth=5)
rfModel = rf.fit(train_data)
result = rfModel.transform(train_data)
predictresult=rfModel.transform(test_data)
predictresult.show()
print('RandomForestClassifier:')
printMetrics(result)
print('\n')
# accuracy
# result.filter(result.label == result.prediction).count()/result.count()

from pyspark.ml.classification import NaiveBayes
nb = NaiveBayes()
nbModel = nb.fit(train_data)
result = nbModel.transform(train_data)
predictresult=nbModel.transform(test_data)
predictresult.show()
print('NaiveBayes:')
printMetrics(result)
print('\n')
#accuracy
# result.filter(result.label == result.prediction).count()/result.count()



from pyspark.ml.classification import LinearSVC
svm = LinearSVC()
svmModel = svm.fit(train_data)
result = svmModel.transform(train_data)
predictresult=svmModel.transform(test_data)
predictresult.show()
print('LinearSVC:')
# ev = BinaryClassificationEvaluator(rawPredictionCol="probability")
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
# areaPR = ev.evaluate(result, {ev.metricName: "areaUnderPR"})
# areaROC = ev.evaluate(result, {ev.metricName: "areaUnderROC"})
accuracy = evaluator.evaluate(result, {evaluator.metricName: "accuracy"})
precision = evaluator.evaluate(result, {evaluator.metricName: "weightedPrecision"})
recall = evaluator.evaluate(result, {evaluator.metricName: "weightedRecall"})
# print('areaUnderPR:', areaPR)
# print('areaUnderROC:', areaROC)
print('Accuracy:', accuracy)
print('precision:', precision)
print('recall:', recall)
print('\n')
# accuracy
# result.filter(result.label == result.prediction).count()/result.count()




