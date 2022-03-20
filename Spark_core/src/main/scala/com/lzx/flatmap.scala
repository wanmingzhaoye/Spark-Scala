package com.lzx

object flatmap {
  def main(args: Array[String]): Unit = {

    val array: Array[List[Any]] = Array(List((1, 2), 3), List((3, 4), 5))

    array.map(x=>x).foreach(println)
    println("===========")
    array.flatten.foreach(println)
    println("===========")
    array.flatMap(x=>x).foreach(println)
    println("===========")

    //    //[List[((Long, Long), Int)]]
    //    //f
    //    List((1,2), 3)
    //    List((3,4), 5)
    //    ===========
    //    (1,2)
    //    3
    //    (3,4)
    //    5
    //    ===========
    //    (1,2)
    //    3
    //    (3,4)
    //    5
    //    ===========
  }

}
