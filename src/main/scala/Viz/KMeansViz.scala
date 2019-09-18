//package Viz
//
//import java.awt.{Color, Dimension, GridLayout}
//
//import javax.swing.JFrame
//
//
//object myColors {
//  def Red: Color = new Color(158,16,48)
//  def Orange: Color = new Color(228,115,1)
//  def Yellow: Color = new Color(200,150,50)
//  def Green: Color = new Color(0,75,0)
//  def Blue: Color = new Color(0,50,100)
//  def Purple: Color = new Color(100,0,100)
//  def Coral: Color = new Color(255,100,100)
//  def flatGreen: Color = new Color(70,150,70)
//  def flatBlue: Color = new Color(70,100,200)
//  def Olive: Color = new Color(120,110,90)
//}
//
//object KMeansViz {
//  def viz(orig: Array[Array[Double]], kmeans: Array[Array[Double]], bimax: Array[Array[Double]]): Unit = {
//    val frame: JFrame = new JFrame("KMeans")
//    frame.setPreferredSize(new Dimension(3000, 1500))
//    frame.setLayout(new GridLayout(1,3))
//
//
//    val origCMap: Array[Color] = Array(Color.WHITE, // 0
//      myColors.Red // 1
//    )
//
//    val githubCMap: Array[Color] = Array(Color.WHITE, // 0
//      myColors.Red, // 1
//      myColors.Orange, // 2
//      myColors.Yellow, // 3
//      myColors.Green, // 4
//      myColors.Blue, // 5
//      myColors.Purple, // 6
//      myColors.Coral, // 7
//      myColors.flatGreen, // 8
//      myColors.flatBlue, // 9
//      myColors.Olive // 10
//    )
//
//    val yelpCMap: Array[Color] = Array(Color.WHITE, // 0
//      myColors.Red, // 1
//      myColors.Orange, // 2
//      myColors.Yellow, // 3
//      myColors.Green, // 4
//      Color.CYAN, // 5
//      myColors.Purple // 6
//    )
//
//
//
//    val p1 = smile.plot.heatmap(orig)
//    val p2 = smile.plot.heatmap(kmeans)
//    val p3 = smile.plot.heatmap(bimax)
//
//    p1.close
//    p2.close
//    p3.close
//
//    p1.canvas.setTitle("Original")
//    p2.canvas.setTitle("KMeans")
//    p3.canvas.setTitle("BiMax")
//
//    frame.getContentPane.add(p1.canvas)
//    frame.getContentPane.add(p2.canvas)
//    frame.getContentPane.add(p3.canvas)
//
//    p1.canvas.reset()
//    p2.canvas.reset()
//    p3.canvas.reset()
//
//    frame.pack()
//    frame.setVisible(true)
//  }
//}
