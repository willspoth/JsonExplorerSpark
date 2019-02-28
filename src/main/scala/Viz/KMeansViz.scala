package Viz

import java.awt.{Dimension, GridLayout}

import javax.swing.JFrame

object KMeansViz {
  def viz(orig: Array[Array[Double]], kmeans: Array[Array[Double]], bimax: Array[Array[Double]]): Unit = {
    val frame: JFrame = new JFrame("KMeans")
    frame.setPreferredSize(new Dimension(3000, 1500))
    frame.setLayout(new GridLayout(1,2))

    val p1 = smile.plot.heatmap(orig)
    val p2 = smile.plot.heatmap(kmeans)
    val p3 = smile.plot.heatmap(bimax)

    p1.close
    p2.close
    p3.close

    p1.canvas.setTitle("Original")
    p2.canvas.setTitle("KMeans")
    p3.canvas.setTitle("BiMax")

    frame.getContentPane.add(p1.canvas)
    frame.getContentPane.add(p2.canvas)
    frame.getContentPane.add(p3.canvas)

    p1.canvas.reset()
    p2.canvas.reset()
    p3.canvas.reset()

    frame.pack()
    frame.setVisible(true)
  }
}
