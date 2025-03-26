# Revised PageRank

## 难易程度:  ** 中

## 待完成
- 请在DSPPCode.mapreduce.revised_pagerank.impl中创建PageRankCountMapperImpl，继承PageRankCountMapper，实现抽象方法
- 请在DSPPCode.mapreduce.revised_pagerank.impl中创建PageRankCountReducerImpl，继承PageRankCountReducer，实现抽象方法
- 请在DSPPCode.mapreduce.revised_pagerank.impl中创建PageRankCountRunnerImpl，继承PageRankCountRunner，实现抽象方法
- 请在DSPPCode.mapreduce.revised_pagerank.impl中创建PageRankReducerImpl，继承PageRankReducer，实现抽象方法
- 请在DSPPCode.mapreduce.revised_pagerank.impl中根据需要添加新的类

## 题目描述：

- 基于一个输入文本（网页链接关系、初始的网页排名）实现网页链接排名算法（阻尼系数以0.85计算）。 
- 本题中，网页总数N需要你根据输入文件通过一个MapReduce过程计算获得，并将网页总数N输出到文件中。
- 本题对网页排名值的收敛条件做了简化，如果当某一网页当前排名值与上一轮迭代排名值之间差值的绝对值小于1e-6，那么认为该网页的排名值已经收敛。
迭代停止的条件为达到最大迭代次数或某次迭代中所有网页均收敛。
* 输入格式：输入文本中的每一行为一项网页信息，其组织形式为（网页名 网页排名值（出站链接...))

  如(A 1.0 B D)表示一个名称为A的网页，当前排名值为1.0,该网页链接至名称为B和D的网页（所有网页权重默认为1.0）
  ```
  A 1.0 B D
  B 1.0 C
  C 1.0 A B
  D 1.0 B C
  ```

* 输出格式:
  要求分两步完成。第一步输出网页总数N：
  ```
  4
  ```
  第二步输出网页的链接关系和最终的排名值：
  ```
  A 0.21436248817266176 B D
  B 0.3633209225962085 C
  C 0.40833002013844744 A B
  D 0.1302651623462253 B C
  ```

> 注意：
> 计算网页排名的阶段请将排名值解析为 `double` 类型变量进行计算。
> 输出结果的小数位数无需处理
