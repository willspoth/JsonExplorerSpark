package Viz

import java.awt.{Color, FontMetrics, Graphics}

import Explorer._
import Optimizer.ConvertOperatorTree
import javax.swing.JPanel

import scala.collection.mutable.ListBuffer

class DrawTree(operatorConverter: ConvertOperatorTree) extends JPanel {

  var (tree,maxDepth) = Types.buildNodeTree(operatorConverter.allAttributes)
  var attributes = operatorConverter.allAttributes

  val heightOffset: Double = .5
  val widthOffset: Double = .5

  val nodes = ListBuffer[treeNode]()
  val edges = ListBuffer[treeEdge]()

  case class treeNode(name: String, x: Int, y: Int, range: (Double,Double), JEType: JsonExplorerType)
  case class treeEdge(px: Int, py: Int, cx: Int, cy: Int)

  def drawNode(name: scala.collection.mutable.ListBuffer[Any], n: node, widthRange: (Double,Double), currentDepth: Int, yInterval: Double): (Int,Int) = {
    val parentX: Double = widthRange._1+((widthRange._2-widthRange._1)/2) // centered
    val parentY: Double = (currentDepth*yInterval)+(yInterval/2) // centered
    if(name.isEmpty)
      nodes += new treeNode("root",parentX.intValue(),parentY.intValue(), widthRange, JE_Object)
    else
      nodes += new treeNode(name.last.toString,parentX.intValue(),parentY.intValue(), widthRange, attributes.get(name).get.naiveType)
    if(n.isEmpty) { // is a Leaf
      return (parentX.intValue(),parentY.intValue())
    }
    val childXInterval: Double = (widthRange._2-widthRange._1)/n.size
    n.foldLeft(widthRange._1){case(low,(childName,grandChildren)) => {
      val cN = name:+childName
      val cA = attributes.get(cN).get.naiveType
      cA.getType() match {
        case JE_Array => low// don't display arrays
        case _ =>
          val (childX,childY) = drawNode(cN,grandChildren.get,(low,low+childXInterval),currentDepth+1, yInterval)
          edges += new treeEdge(parentX.intValue(),parentY.intValue(),childX,childY)
          low+childXInterval
      }

    }}
    return (parentX.intValue(),parentY.intValue())
  }

  override def paint(g: Graphics) { // draw the nodes and edges
    //(tree,maxDepth) = Types.buildNodeTree(operatorConverter.allAttributes)
    attributes = operatorConverter.allAttributes

    nodes.clear()
    edges.clear()

    val absoluteHeight: Int = getHeight()
    val absoluteWidth: Int = getWidth()

    //val heightRange: (Double,Double) = (absoluteHeight*heightOffset,absoluteHeight-absoluteHeight*heightOffset)
    //val widthRange: (Double,Double) = (absoluteWidth*widthOffset,absoluteWidth-absoluteWidth*widthOffset)
    val yRange: (Double,Double) = (0.0,absoluteHeight)
    val xRange: (Double,Double) = (0.0,absoluteWidth)

    drawNode(ListBuffer[Any](),tree,xRange,0,(absoluteHeight/maxDepth))

    val f: FontMetrics = g.getFontMetrics()

    val nodeHeight: Int = math.max(
      ((yRange._2-yRange._1)/(maxDepth*10)).intValue(),
      f.getHeight()
    )
    /*val nodeWidth: Int = math.max(
      (widthRange._2-widthRange._1)bredthSize.max,
      nodes.map(x => f.stringWidth(x.name)).max
    )*/


    g.setColor(Color.black)
    edges.foreach(e => g.drawLine(e.cx,e.cy,e.px,e.py))

    nodes.foreach(n => {
      val nodeWidth: Int = math.max(
        //(n.range._1+((n.range._2-n.range._1)/5)).intValue(),
        nodeHeight,
        nodes.map(x => f.stringWidth(x.name)).max
      )

      n.JEType match {
        case JE_Array => g.setColor(Color.red)
        case JE_Object => g.setColor(Color.blue)
        case JE_Var_Object => g.setColor(Color.cyan)
        case JE_Obj_Array => g.setColor(Color.ORANGE)
        case JE_Basic => g.setColor(Color.white)
        case _ => g.setColor(Color.YELLOW)
      }


      g.fillOval(n.x-nodeWidth/2, n.y-nodeHeight/2,
        nodeWidth, nodeHeight)
      g.setColor(Color.black)
      g.drawOval(n.x-nodeWidth/2, n.y-nodeHeight/2,
        nodeWidth, nodeHeight)

      g.drawString(n.name, n.x-f.stringWidth(n.name)/2,
        n.y+f.getHeight()/2)
    })

  }

  def buildNodeTree(attributes: scala.collection.mutable.HashMap[scala.collection.mutable.ListBuffer[Any],AnyRef]): (node,Int) = {
    val depth: Int = attributes.foldLeft(0){case (maxDepth,n) => Math.max(n._1.size,maxDepth)}+1

    val tree = new node()
    attributes.foreach{case(name,attribute) => {
      // now need to make operator tree
      name.foldLeft(tree){case(tree,n) => {

        tree.get(n) match {
          case Some(opNode) =>
            opNode match {
              case Some(nod) =>
                tree.get(n).get.get
              case None =>
                tree.put(n,Some(new node()))
                tree.get(n).get.get
            }
          case None =>
            tree.put(n,Some(new node()))
            tree.get(n).get.get
        }
      }}

    }}
    return (tree,depth)
  }
}
