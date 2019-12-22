from pyspark.python.pyspark.shell import sc
def getData(Path):
    ## 读取train.tsv
    print("开始导入数据...")
    rData = sc.textFile(Path)
    head = rData.first()
    rData = rData.filter(lambda line: line != head)
    ## 以逗号分割每一行
    lines = rData.map(lambda x: x.split(","))
    print("共有：" + str(lines.count()) + "项数据")
    ## 取出前2项数据
    print(rData.take(2))

    ##  处理特征
    import numpy as np

    def convert_float(v):
        """处理数值, 将字符串转化为float"""
        return float(v)

    def process_features(line, featureEnd):
        ## 处理数值特征
        Features = [convert_float(value) for value in line[:featureEnd]]
        Features = Features[1:len(Features)]
        # 返回拼接的总特征列表
        return Features

    ## 处理标签
    def process_label(line):
        return float(line[-1])  # 最后一个字段为类别标签, 设置从0开始

    process_label(lines.first())

    ## 构建LabeledPoint数据：
    from pyspark.mllib.regression import LabeledPoint

    labelpointRDD = lines.map(lambda r: LabeledPoint(process_label(r), process_features(r, len(r) - 1)))
    ## 查看labelpoint第一项数据
    # print(labelpointRDD.take(10))
    return labelpointRDD
train_data=getData(Path="file:///home/whw/PycharmProjects/spark-test/train_after.csv")
test_data=getData(Path="file:///home/whw/PycharmProjects/spark-test/test_after.csv")

from pyspark.mllib.evaluation import MulticlassMetrics

def getPredictionsLabels(model, test_data):
    predictions = model.predict(test_data.map(lambda r: r.features))
    return predictions.zip(test_data.map(lambda r: r.label))

from pyspark.mllib.evaluation import BinaryClassificationMetrics
def printMetrics(predictions_and_labels):
    metrics = MulticlassMetrics(predictions_and_labels)
    metrics2= BinaryClassificationMetrics(predictions_and_labels)
    print('Precision of True ', metrics.precision(1))
    print('Precision of False', metrics.precision(0))
    print('Recall of True    ', metrics.recall(1))
    print('Recall of False   ', metrics.recall(0))
    print('areaUnderROC      ',metrics2.areaUnderROC)
    print('areaUnderPR       ', metrics2.areaUnderPR)
    # print('F-1 Score         ', metrics.fMeasure())
    # print('Confusion Matrix\n', metrics.confusionMatrix().toArray())

## 定义模型评估函数
def ModelAccuracy(model, validationData):
    ## 计算模型的准确率
    predict = model.predict(validationData.map(lambda p:p.features))
    predict = predict.map(lambda p: float(p))
    ## 拼接预测值和实际值
    predict_real = predict.zip(validationData.map(lambda p: p.label))
    matched = predict_real.filter(lambda p:p[0]==p[1])
    accuracy =  float(matched.count()) / float(predict_real.count())
    return accuracy

## 使用决策数模型进行训练
from pyspark.mllib.tree import DecisionTree
model = DecisionTree.trainClassifier(train_data, numClasses=2,categoricalFeaturesInfo={}, impurity="entropy", maxDepth=5,maxBins=5)
print("DecisionTree")
"""模型性能评估"""
predictions_and_labels = getPredictionsLabels(model, train_data)
printMetrics(predictions_and_labels)
## 调用函数求模型在验证集上的准确率
acc =  ModelAccuracy(model, train_data)
print("准确率accuracy="+str(acc))

predict=model.predict(test_data.map(lambda p:p.features))
predict_all=predict.zip(test_data.map(lambda p:p.features))
for i in range(0,10):
    print(predict_all.collect()[i])
print('\n')

from pyspark.mllib.tree import RandomForest
model=RandomForest.trainClassifier(train_data,numClasses=2,categoricalFeaturesInfo={},numTrees=20)
print('RandomForest')
predictions_and_labels=getPredictionsLabels(model,train_data)
printMetrics(predictions_and_labels)
acc=ModelAccuracy(model,train_data)
print("准确率accuracy="+str(acc))

predict=model.predict(test_data.map(lambda p:p.features))
predict_all=predict.zip(test_data.map(lambda p:p.features))
for i in range(0,10):
    print(predict_all.collect()[i])
print('\n')

from pyspark.mllib.tree import GradientBoostedTrees
model=GradientBoostedTrees.trainClassifier(train_data,categoricalFeaturesInfo={},maxDepth=5)
print('GradientBoostedTrees')
predictions_and_labels=getPredictionsLabels(model,train_data)
printMetrics(predictions_and_labels)
acc=ModelAccuracy(model,train_data)
print("准确率accuracy="+str(acc))

predict=model.predict(test_data.map(lambda p:p.features))
predict_all=predict.zip(test_data.map(lambda p:p.features))
for i in range(0,10):
    print(predict_all.collect()[i])