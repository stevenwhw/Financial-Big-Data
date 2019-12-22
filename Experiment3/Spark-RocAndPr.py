from pylab import *
x=['LogistRegression','DecisionTree','GBTClassifier','RandomForestClassifier','NaiveBayes']
y=[0.06976779948244662,0.14351379092496935,0.1943642699159455,0.07605168377168794,0.0602391594269824]
yRoc=[0.5441220664743313,0.5238205233308246,0.7047150508009895,0.54070826837616,0.5166326761459356]
plt.plot(x,yRoc,'ro-', color='#4169E1', alpha=0.8,label='Roc')
plt.plot(x,y,'ro-', color='#FF0000', alpha=0.8,label='Pr')
plt.legend(loc="upper right")
plt.show()