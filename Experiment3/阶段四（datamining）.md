<h1><center>阶段四（datamining）</center></h1>
### 1、针对预处理后的训练集和测试集，基于MapReduce或Spark MLlib编写程序预测回头客

#### （1）基于Spark ML的预测（pyspark.ml）

​			**使用pyspark.ml库中的分类包**

##### LogistRegression：

模型预测结果（部分）：

<img src="images/31.png">

模型评估参数值：

<img src="images/21.png">

##### DecisionTree:

模型预测结果（部分）：

<img src="images/22.png">

模型评估参数值：

<img src="images/33.png">

##### GBTClassifier:

模型预测结果（部分）：

<img src="images/23.png">

模型评估参数值：

<img src="images/24.png">

##### RandomForestClassifier:

模型预测结果（部分）：

<img src="images/25.png">

模型评估参数值：

<img src="images/26.png">

##### NaiveBayes:

模型预测结果（部分）：

<img src="images/27.png">

模型评估参数值：

<img src="images/28.png">

##### LinearSVC:

模型预测结果（部分）：

<img src="images/29.png">

模型评估参数值：

<img src="images/30.png">

##### 模型比较：

<img src="images/Figure_1.png">

根据各个模型的Roc和Pr画出折线图可以看出

GBT和DecisionTree这两个模型的拟合效果比较好。

#### （2）基于Spark MLlib的预测（pyspark.mllib）

**说明：**以下模型中由于label为0是False，所以Precision/Recall of false是预测模型的指数

##### DecisionTree：

预测评估参数以及部分预测结果：

<img src="images/32.png">

##### GradientBoostedTrees：

部分预测结果和预测模型参数评估：

<img src="images/34.png">

##### RandomForest:

部分预测结果和预测模型参数评估：

<img src="images/35.png">

