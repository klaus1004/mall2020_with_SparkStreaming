package com.klaus.gmall.utils

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, Index}

//save Daily Active User by redis to elasticsearch
object MyEsUtil {

  var factory : JestClientFactory=null

  def getClient:JestClient ={
    if(factory==null)build();
    factory.getObject

  }

  def  build(): Unit ={
    factory=new JestClientFactory
    val properties = PropertiesUtil.load("config.properties")
    val serverUri = properties.getProperty("ES.serverUri")
    val isMultiThreaded = properties.getProperty("Es.multiThreaded")
    val maxTotalConnection = properties.getProperty("ES.maxTotalConnection")
    val connTimeout = properties.getProperty("ES.connTimeout")
    val readTimeout = properties.getProperty("ES.readTimeout")
    factory.setHttpClientConfig(new HttpClientConfig.Builder(serverUri)
      .multiThreaded(isMultiThreaded.toBoolean)
      .maxTotalConnection(maxTotalConnection.toInt)
      .connTimeout(connTimeout.toInt).readTimeout(readTimeout.toInt).build())

  }
  //批量提交doc到Es
  def bulkDoc(sourceList:List[Any],indexName:String):Unit={
    val jest = getClient
    val bulkBuilder = new Bulk.Builder
    for(source <- sourceList){
      val index = new Index.Builder(source).index(indexName).`type`("_doc").build()
      bulkBuilder.addAction(index)
    }
    val bulk = bulkBuilder.build()
    val result = jest.execute(bulk)
    val items = result.getItems
    println("成功保存到ES"+items.size()+"条数据")
    jest.close()
  }
  def addDoc(source:Any,indexName:String):Unit={
    val jest = getClient
    val index = new Index.Builder(source).index(indexName).`type`("_doc").build()
    val message = jest.execute(index).getErrorMessage
    if(message!=null){
      println(message)
    }
    jest.close()
  }
case class Movie(name:String,id:Long)

//  def main(args: Array[String]): Unit = {
//    addDoc(Movie("教父2",100),"testindex")
//  }
}
