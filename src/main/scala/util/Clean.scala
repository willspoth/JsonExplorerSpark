package util

import java.io._

object Clean {
  // these are special parsers for our data just to get things running, will replace with better solution for recall tests

  def github(): Unit = {
    val file = new File("C:\\Users\\Will\\Desktop\\JsonData\\github1m.json")
    val bw = new BufferedWriter(new FileWriter(file))
    for( a <- 1 to 100){
      bw.write(scala.io.Source.fromFile("C:\\Users\\Will\\Desktop\\JsonData\\github\\github"+a.toString+".json").mkString+'\n')
      println(a)
    }
    bw.close()
    ???
  }


  def clean(inputName: String, rowLimit: Int = 0): Unit = {
    val in = new File(inputName)
    val br = new BufferedReader(new FileReader(in))
    val out = new File(inputName+"out")
    val bw = new BufferedWriter(new FileWriter(out))
    var rowCount: Int = 0
    var wrongCount: Int = 0
    var line: String = null
    var break: Boolean = false
    while(((rowCount < rowLimit) || (rowLimit == 0)) && ((line = br.readLine())!= null) && !break){
      if(line != null && line.size > 1 && (line.charAt(0).equals('{') && line.charAt(line.size-1).equals('}'))) {
        bw.write(line + '\n')
        rowCount += 1
        if(rowCount % 100000 == 0)
          println(rowCount.toString + " rows so far" + wrongCount.toString + " wrong lines found")
      } else {
        wrongCount += 1
        if(wrongCount%100 == 0) {
          break = true
          System.err.println(wrongCount.toString + " wrong lines found")
        }
      }
    }
    println(rowCount)
    br.close()
    bw.close()
    ???
  }
}
