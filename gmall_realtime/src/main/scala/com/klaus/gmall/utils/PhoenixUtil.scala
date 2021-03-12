package com.klaus.gmall.utils

import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, Statement}

import com.alibaba.fastjson.JSONObject

import scala.collection.mutable.ListBuffer


object PhoenixUtil {

  def main(args: Array[String]): Unit = {
//    val list:  List[ JSONObject] = queryList("select user_id,if_consumed from  USER_STATE2020")
//    println(list)
    test("s=\"111+11*20\"")
  }
  def test(s:String): Unit ={
    val strings = s.split("=")(1).toCharArray.toList
    val str = strings.filter(char => {
      char != ' ' && char != '"'
    })
  //  111+11*20+20/10-9
    var num :List[Long]=List()
    var fuhao :List[Char]=List()
    var stt:String=""
    var flag:Int = -1
    for(i <- 0 to str.length-1){
      if(str(i)=='+'||str(i)=='-'||str(i)=='*'||str(i)=='/'){

        fuhao = fuhao :+ str(i)
        for(j <- flag+1 until i){
          stt = stt+str(j).toString

        }
        flag=i
        num = num :+ stt.toLong
        stt = ""
      }


    }
    for(i<- flag+1 until str.length){
      stt = stt + str(i).toString

    }
    num = num :+ stt.toLong

    111+11*20-20/10*3-9
    var sum : Long = 0
    for(i<- 0 to fuhao.length-1){
      if(fuhao(i)=='*'){
        if(i==0){num.updated(0,num(0)*num(1));num.drop(1);fuhao.drop(0)}
        else if(fuhao(i-1)=='+'||fuhao(i-1)=='-'){num.updated(0,num(0)*num(1));num.drop(1);fuhao.drop(0)}
      }
      if(fuhao(i)=='/'){
        sum=num(i)/num(i+1)
      }



    }

   num.foreach(a=>print(a+"  "))
   fuhao.foreach(print(_))
  }

  def   queryList(sql:String):List[JSONObject]={
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    val resultList: ListBuffer[JSONObject] = new  ListBuffer[ JSONObject]()
    val conn: Connection = DriverManager.getConnection("jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181")
    val stat: Statement = conn.createStatement
    println(sql)
    val rs: ResultSet = stat.executeQuery(sql )
    val md: ResultSetMetaData = rs.getMetaData
    while (  rs.next ) {
      val rowData = new JSONObject();
      for (i  <-1 to md.getColumnCount  ) {
        rowData.put(md.getColumnName(i), rs.getObject(i))
      }
      resultList+=rowData
    }

    stat.close()
    conn.close()
    resultList.toList
  }
}
