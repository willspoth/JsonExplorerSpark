import javax.swing._
import Explorer.{Attribute, Types}


object TreeTest{

  def main(args: Array[String]): Unit = {
    val frame: JFrame = new JFrame()

    val schemas = scala.collection.mutable.HashMap[scala.collection.mutable.ListBuffer[Any],Attribute]()
    schemas.put(scala.collection.mutable.ListBuffer[Any]("Test"),null)
    schemas.put(scala.collection.mutable.ListBuffer[Any]("Test","Child"),null)
    schemas.put(scala.collection.mutable.ListBuffer[Any]("Test","Other"),null)
    schemas.put(scala.collection.mutable.ListBuffer[Any]("Test2","Child"),null)
    schemas.put(scala.collection.mutable.ListBuffer[Any]("Test2","Other"),null)

    val (tree,depth) = Types.buildNodeTree(schemas)

    val panel: Viz.DrawTree = new Viz.DrawTree(tree,depth, null)

    frame.setSize(2000,1500)

    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)
    frame.setVisible(true)

    frame.getContentPane.add(panel)
  }

}