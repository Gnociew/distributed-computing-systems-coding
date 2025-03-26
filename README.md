# Semi Join

## 难易程度： **中

## 待完成

- 请在DSPPCode.mapreduce.semi_join.impl中创建SemiJoinMapperImpl，继承SemiJoinMapper，实现抽象方法
- 请在DSPPCode.mapreduce.semi_join.impl中创建SemiJoinRunnerImpl，继承SemiJoinRunner，实现抽象方法

## 问题描述

学院为了衡量精品课程的建设情况，决定使用如下SQL语句
**select S#,C#,Score from SC where C# in (select C# from Course);**
统计同学们的精品课程的成绩情况。然而由于学生选课记录太多了，学院的单机数据库并不能支撑这么大的计算量，现在只能请求你使用MapReduce实现上述SQL语句。

输入格式：

输入一共两个文件Course和SC，分别表示精品课程信息表Course，学生选课信息表SC。

- Course

  输入格式：精品课程信息表，第一行为表字段名：C#，CName，分别表示课程号和课程名，使用制表符隔开。剩余每一行表示一条课程记录。如：C1	Principles of Computer Construction ，表示课程号为C1的课程名称为Principles of Computer Construction。

  ```
  C#	CName
  C1	Principles of Computer Construction
  C2	Computer Network
  ```
  
- SC

  输入格式：C学生选课信息表，第一行为表字段名：S#,C#,Score，分别表示学生学号，课程号，成绩，使用制表符隔开。剩余每一行表示一个选课记录，各个字段使用制表符隔开，如：S1 C1  99 表示学号为S1的学生选修了C1课程，该课程成绩为99

  ```
  S#	C#	Score
  S1	C1	43
  S1	C8	46
  S1	C6	51
  S2	C2	66
  S2	C4	53
  S2	C6	58
  S3	C3	97
  ```

输出格式：请输出满足SQL语句的SC记录，按照S#,C#,Score形式输出，其中第一行为字段名，剩余行为结果，每列之间使用制表符分割		

```
S#	C#	Score
S1	C1	43
S2	C2	66
```

