# Average Score

## 难易程度:  \* 易

## 待完成:

* 请在 DSPPCode.mapreduce.average_score.impl 中创建 ScoreMapperImpl, 继承 ScoreMapper, 实现抽象方法

* 请在 DSPPCode.mapreduce.average_score.impl 中创建 ScoreReducerImpl, 继承 ScoreReducer, 实现抽象方法

## 题目描述:

* 某班级有若干个学生，每个学生共修读三门课程。现给定该班级中每个学生每门课程的分数，请计算每门课程的班级平均分（级平均分向下取整）。

* 输入格式：学号,数学分析分数,概率论分数,实变函数分数。例如，10160001,98,80,75 代表学号为10160001的学生, 数学分析98分, 概率论80分, 实变函数75分。

  ```
  10160001,98,80,75
  10160002,53,94,77
  10160003,61,86,91
  ```

* 输出格式：课程名 班级平均分

  ```
  Function of Real Variable 81
  Mathematical analysis 70
  Probability Theory 86
  ```

  (课程名通过 Util.getCourseName 获得)
