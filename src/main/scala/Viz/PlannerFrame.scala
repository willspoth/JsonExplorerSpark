package Viz

import java.awt.event.ActionEvent
import java.awt.{Color, Dimension, FlowLayout, Graphics, GridBagLayout, GridLayout}

import Explorer.{JsonExtractionRoot, _}
import JsonExplorer.SparkMain
import Optimizer.{ConvertOperatorTree, Planner}
import javax.swing._

class PlannerFrame(root:JsonExtractionRoot, useUI: Boolean, infer: Boolean = true) extends JFrame {

  var plannerDone = false

  // compute entropy and reassemble tree for optimizations
  val kse_intervals = Planner.buildOperatorTree(root) // updates root by reference

  // finds the largest interval, this will be the starting kse
  val kse_threshold: Double = if(infer) Planner.inferKSE(kse_intervals.sortBy(_._2)) else kse_intervals.map(_._2).max + 1.0
  Planner.setNaiveTypes(root)

  val (data,labels) = kse_intervals.flatMap(x => {
    root.AllAttributes.get(x._1).get.naiveType.getType() match {
      case JE_Array | JE_Empty_Array => List(Tuple2(List(x._2, root.AllAttributes.get(x._1).get.typeEntropy.get).toArray,0))
      case JE_Object | JE_Empty_Object | JE_Var_Object => List(Tuple2(List(x._2, root.AllAttributes.get(x._1).get.typeEntropy.get).toArray,1))
      case JE_Obj_Array => List(Tuple2(List(x._2, root.AllAttributes.get(x._1).get.typeEntropy.get).toArray,2))
      case JE_Var_Object => List(Tuple2(List(x._2, root.AllAttributes.get(x._1).get.typeEntropy.get).toArray,3))
      case JE_Basic => List()
      case _ => List()
    }
  }).toArray.unzip

  val operatorConverter: ConvertOperatorTree = new Optimizer.ConvertOperatorTree (root)
  operatorConverter.Rewrite (kse_threshold) // put in loop and visualize


  useUI match {

    case true =>
      val legend = Array ('*', 'o', 's', '+')
      val colors = Array (Color.BLACK, Color.BLUE, Color.GREEN, Color.RED)

      val entropyChart = smile.plot.plot (data, labels, legend, colors)
      entropyChart.close
      entropyChart.canvas.setAxisLabel (0, "Key-Space Entropy")
      entropyChart.canvas.setAxisLabel (1, "Type Entropy")


      val graphsPanel = new JPanel ()
      graphsPanel.setLayout (new GridLayout (1, 2) )


      val treePanel: Viz.DrawTree = new Viz.DrawTree (operatorConverter)

      this.setSize (2000, 1500)

      //this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)
      //this.setLayout (new FlowLayout () )
      this.setVisible (true)

      graphsPanel.add (treePanel)
      graphsPanel.add (entropyChart.canvas)
      graphsPanel.setPreferredSize(graphsPanel.getPreferredSize)

      this.getContentPane.add(graphsPanel)

      val kseInput = new JTextField (kse_threshold.toString)
      kseInput.addActionListener (new tryNewKSE (operatorConverter, kseInput, graphsPanel) )

      val doneButton = new JButton ("Done")
      doneButton.addActionListener (new done (operatorConverter) )

      val buttonsPanel = new JPanel ()
      buttonsPanel.setLayout (new GridLayout (1, 2) )

      buttonsPanel.add (kseInput)
      buttonsPanel.add (doneButton)
      buttonsPanel.setPreferredSize (buttonsPanel.getPreferredSize)

      //this.getContentPane.add (buttonsPanel)

      while (! plannerDone) {
        val donothing = true
      }
      this.setVisible (false)
      this.dispose ()

    case false =>
      operatorConverter.Keep()
  }

  override def paint(g: Graphics): Unit = {
    super.paint(g)
    //graphsPanel.setPreferredSize(new Dimension(getWidth(),math.floor(getHeight()*.9).toInt))
    //buttonsPanel.setPreferredSize(new Dimension(getWidth(),math.max(math.floor(getHeight()*.1).toInt,100)))
  }

  class done(operatorConverter: ConvertOperatorTree) extends AbstractAction {
    override def actionPerformed(e: ActionEvent): Unit = {
      operatorConverter.Keep()
      plannerDone = true
    }
  }

}

class tryNewKSE(operatorConverter: ConvertOperatorTree, textField: JTextField, graphPanel: JPanel) extends AbstractAction {
  override def actionPerformed(e: ActionEvent): Unit = {
    operatorConverter.Rewrite(textField.getText().toDouble)
    graphPanel.repaint()
  }
}
