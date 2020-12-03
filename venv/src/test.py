# from pyspark.sql import SparkSession
#
# spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
#
#
#
# export PYSPARK_PYTHON=python3
# pyspark
#
# #
# 地域线路特征1周	滑动窗口1周	始发地-到达地-到达地(省份）	victor	2020/9/2
# 地域线路特征1周	滑动窗口1周	始发地-到达地-到达地(城市）	victor	2020/9/2
# 地域线路特征1周	滑动窗口1周	各个关键道路占比（7918+国道）	victor	2020/9/3

# remind to add constraints for the data range
from typing import List

import spark_df_profiling
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark import *

spark = SparkSession.builder.appName("data_Label").enableHiveSupport().getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", 400)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.broadcastTimeout", 36000)
spark.conf.set("spark.rpc.askTimeout", 300)
spark.conf.set("spark.dynamicAllocation.enabled", "true")







def main():
data = spark.sql("select engine_serial_number, occurrence_date_time ,province, highway_code, roadname, occurrence_date_time, report_date from pri_tel.tel_hb_labeled where report_date >= '20200708'")

addWeekDaysDF = data.withColumn("First_Day_Of_Week", date_sub(next_day(data.occurrence_date_time,'Mon'),7))

countDF = addWeekDaysDF.groupBy("engine_serial_number", "highway_code", "First_Day_Of_Week").count()


# a reference dataframe stores all the total number of each day
totalNumber = addWeekDaysDF.groupBy("First_Day_Of_Week", "engine_serial_number").count().withColumnRenamed("count", "totalNumber")

joinDF = totalNumber.join(countDF, on=["First_Day_Of_Week", "engine_serial_number"], how="right")

# Typical_Road_Percentage%
percentageDF = joinDF.withColumn("Typical_Road_Percentage", (joinDF["count"].cast("double")/joinDF["totalNumber"].cast("double")*100).cast("string"))

df.write.format('parquet') \
    .mode("overwrite") \
    .saveAsTable('bucketed_table') \


def getWarehousePath(warehouseRootPath, database ,tableName):
  dbname = database.lower()
  return warehouseRootPath.lower() + "/"  + dbname + "/" + tableName.lower()


hivePath = "wasbs://westlake@dldevblob4hdistorage.blob.core.chinacloudapi.cn/westlake_data"
def saveData(df, db, tbl):
  df.write. \
  mode("overwrite").format("parquet"). \
  save(hivePath + "/data_label/city_percentage/")














if __name__ == "__main__":
    main()

spark-submit --master yarn --driver-memory 16g --num-executors 8 dictionary_summary.py


class Solution:
    def solveNQueens(self, n: int) -> List[List[str]]:
        n= 4
        board = [[ "." for _ in range(n)] for _ in range(n)]
        board


        def back_track(board, n):
            if back_track



    def isValid(self, row ,col, board):
        #检查列中是否有重复的Q-> row递增
        for i in range(row):
            if board[i][col] == "Q":
                return False
        #检查行中是否有重复值->col递增
        for j in range(col):
            if board[row][j] == "Q":
                return False



class Solution:
    def lengthOfLongestSubstring(self, s: str) -> int:
        if not s:
            return 0
        max_length = 0
        for i in range(len(s)):
            j = i+1

            while j < len(s)+1:
                print(i, j, s[i:j])
                if len(s[i:j]) != len(set(s[i:j])):

                    max_length = max(max_length, len(s[i:j-1]))
                    print("max", max_length)
                    break
                j += 1
        return max_length

s=Solution()
s.lengthOfLongestSubstring(' ')
a = 'abcabcbb'
a[0:3]


class TreeNode():
    def __init__(self, x):
        self.val = x
        self.left = None
        self.right = None

class Solution:
    def mergeTrees(self, t1: TreeNode, t2: TreeNode) -> TreeNode:

        def dfs(t1, t2):
            if not t1:
                return t2
            if not t2:
                return t1

            MergeNode = TreeNode(t1.val+t2.val)
            MergeNode.left = dfs(t1.left, t2.left)
            MergeNode.right = dfs(t1.right, t2.right)

        return dfs(t1, t2)

class Solution:
    def mergeTrees(self, t1: TreeNode, t2: TreeNode) -> TreeNode:

        if not t1:
            return t2
        if not t2:
            return t1

        t1.val= t1.val+t2.val
        t1.left = self.mergeTrees(t1.left,t2.left)
        t1.right = self.mergeTrees(t1.right, t2.right)
        return t1





