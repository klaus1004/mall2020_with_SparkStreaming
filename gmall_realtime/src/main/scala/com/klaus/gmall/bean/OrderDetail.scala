package com.klaus.gmall.bean

case class OrderDetail(id:Long,
                       order_id:Long,
                       sku_id:Long,
                       order_price:Double,
                       sku_num:Long,
                       sku_name:String,
                       creat_time:String,

                       var spu_id: Long=0L,
                       var tm_id: Long=0L,
                       var category3_id: Long=0L,
                       var spu_name: String=null,
                       var tm_name: String=null,
                       var category3_name: String=null
                      )
