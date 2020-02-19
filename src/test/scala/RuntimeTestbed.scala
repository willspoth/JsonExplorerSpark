import scala.collection.mutable.ListBuffer

object RuntimeTestbed {

  val listLB:ListBuffer[ListBuffer[Any]] = ListBuffer[ListBuffer[Any]]()
  val listL:ListBuffer[List[Any]] = ListBuffer[List[Any]]()

  def main(args: Array[String]): Unit = {
//    val startTime = System.currentTimeMillis()
//
//    val listL:ListBuffer[List[Any]] = ListBuffer[List[Any]]()
//    (1 to 5).foldLeft(List[Any]()){case (l,e) => {
//      val l2 = l ++ List[Any](e)
//      listL.append(l2)
//      itL(l2,30)
//      l2
//    }}
//
//    val endTime = System.currentTimeMillis()
//    println("Runtime List: " + (endTime - startTime).toString)

//    //12375

    val startTimeLB = System.currentTimeMillis()

    (1 to 1000).foldLeft(ListBuffer[Any]()){case (l,e) => {
      val l2: ListBuffer[Any] = l :+ e
      listLB.append(l2)
      itLB(l2,3)
      l2
    }}

    val endTimeLB = System.currentTimeMillis()
    println("Runtime ListBuffer: " + (endTimeLB - startTimeLB).toString)

//    val startTimeRec = System.currentTimeMillis()
//    val l: ListBuffer[Any] = ListBuffer[Any]()
//    app(l,0,10000)
//    val endTimeRec = System.currentTimeMillis()
//    println("Runtime ListBuffer: " + (endTimeRec - startTimeRec).toString)
  }

  def itLB(l: ListBuffer[Any],c: Int): Unit = {
    (1 to c).foldLeft(l){case (l1,e) => {
      val l2: ListBuffer[Any] = l1 :+ e
      listLB.append(l2)
      l2
    }}
  }

  def itL(l: List[Any],c: Int): Unit = {
    (1 to c).foldLeft(l){case (l1,e) => {
      val l2 = l1 ++ List[Any](e)
      listL.append(l2)
      l2
    }}
  }


  //  def app(l:ListBuffer[Any],e: Int, d: Int): Unit = {
//    l.append(e)
//    if(e<d)
//      app(l,e+1,d)
//    l.dropRight(1)
//  }

}
