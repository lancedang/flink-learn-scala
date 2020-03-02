package com.dangdang.flinkscala.mysql

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.jdbc.{JDBCInputFormat, JDBCOutputFormat}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.types.Row

import scala.collection.mutable.ArrayBuffer
import org.apache.flink.api.scala._

/**
 * Author: 柏云鹏
 * Date: 3/2/20.
 */
object FlinkMysqlDemo {
    private val JDBC_URL: String = "jdbc:mysql://localhost:3306/flink_demo"
    private val JDBC_USER: String = "root"
    private val JDBC_PASSWORD: String = "abc123456"
    private val JDBC_DRIVER_CLASS: String = "com.mysql.jdbc.Driver"

    def main(args: Array[String]): Unit = {

      val env = ExecutionEnvironment.getExecutionEnvironment

      val inputMysql = testJDBCRead(env)
      //  inputMysql.print()
      inputMysql.map(s => (s.getField(0),s.getField(1),s.getField(2),s.getField(3))).print()

      testJDBCWrite(env)


    }


    def testJDBCRead(env: ExecutionEnvironment): DataSet[Row] = {
      val inputMysql = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
        // 数据库连接驱动名称
        .setDrivername(JDBC_DRIVER_CLASS)
        // 数据库连接驱动名称
        .setDBUrl(JDBC_URL)
        // 数据库连接用户名
        .setUsername(JDBC_USER)
        // 数据库连接密码
        .setPassword(JDBC_PASSWORD)
        // 数据库连接查询SQL
        .setQuery("select id, name, password, age from student")
        // 字段类型,顺序个个数必须与SQL保持一致
        .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO))
        .finish()
      )
      inputMysql
    }

    def testJDBCWrite(env: ExecutionEnvironment): Unit = {
      // 生成测试数据，由于插入数据需要是Row格式，提前设置为Row
      val arr = new ArrayBuffer[Row]()
      val row1 = new Row(4)
      row1.setField(0, 10)
      row1.setField(1, "zhangfei")
      row1.setField(2, "123")
      row1.setField(3, 10)

      arr.+=(row1)

      // 将集合数据转成DataSet
      val data = env.fromCollection(arr)
      // 使用JDBCOutputFormat，将数据写入到Mysql
      data.output(JDBCOutputFormat.buildJDBCOutputFormat()
        // 数据库连接驱动名称
        .setDrivername(JDBC_DRIVER_CLASS)
        // 数据库连接驱动名称
        .setDBUrl(JDBC_URL)
        // 数据库连接用户名
        .setUsername(JDBC_USER)
        // 数据库连接密码
        .setPassword(JDBC_PASSWORD)
        // 数据库插入SQL
        .setQuery("insert into student (id, name,password,age) values(?,?,?,?)")
        .finish())

      // 触发执行
      env.execute("Test JDBC  Output")
    }

}
