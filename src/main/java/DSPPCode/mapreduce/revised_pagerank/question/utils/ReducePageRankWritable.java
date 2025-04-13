package DSPPCode.mapreduce.revised_pagerank.question.utils;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//map输出键值对为【网页名称，标识 贡献值或网页信息】，因此可定义该数据类型
public class ReducePageRankWritable implements Writable {

  /// 保存贡献值或网页信息
  private String data;
  // 标识data保存的是贡献值还是网页信息
  private String tag;

  // 用于标识的常量
  public static final String PAGE_INFO = "1";
  public static final String PR_L = "2";

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(tag);
    dataOutput.writeUTF(data);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    tag = dataInput.readUTF();
    data = dataInput.readUTF();
  }

  public String getData() {
    return data;
  }

  public void setData(String data) {
    this.data = data;
  }

  public String getTag() {
    return tag;
  }

  public void setTag(String tag) {
    this.tag = tag;
  }
}
// import org.apache.hadoop.io.*;
//
// import java.io.DataInput;
// import java.io.DataOutput;
// import java.io.IOException;
//
// public class ReducePageRankWritable implements Writable {
//
//   public static final byte PR_L = 0;        // PageRank 贡献值
//   public static final byte PAGE_INFO = 1;   // 网页结构信息
//
//   private ByteWritable tag = new ByteWritable();
//   private DoubleWritable prValue = new DoubleWritable();
//   private Text pageInfo = new Text();
//
//   public ReducePageRankWritable() {}
//
//   public void setTag(byte t) {
//     this.tag.set(t);
//   }
//
//   public byte getTag() {
//     return this.tag.get();
//   }
//
//   public void setPrValue(double pr) {
//     this.prValue.set(pr);
//   }
//
//   public double getPrValue() {
//     return this.prValue.get();
//   }
//
//   public void setPageInfo(String info) {
//     this.pageInfo.set(info);
//   }
//
//   public String getPageInfo() {
//     return this.pageInfo.toString();
//   }
//
//   @Override
//   public void write(DataOutput out) throws IOException {
//     tag.write(out);
//     if (tag.get() == PR_L) {
//       prValue.write(out);
//     } else {
//       pageInfo.write(out);
//     }
//   }
//
//   @Override
//   public void readFields(DataInput in) throws IOException {
//     tag.readFields(in);
//     if (tag.get() == PR_L) {
//       prValue.readFields(in);
//     } else {
//       pageInfo.readFields(in);
//     }
//   }
//
//   @Override
//   public String toString() {
//     if (tag.get() == PR_L) {
//       return "PR_L: " + prValue.get();
//     } else {
//       return "PAGE_INFO: " + pageInfo.toString();
//     }
//   }
// }
