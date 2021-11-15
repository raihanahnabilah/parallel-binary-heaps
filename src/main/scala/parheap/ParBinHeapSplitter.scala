package parheap

import scala.collection.parallel.IterableSplitter
import scala.reflect.ClassTag

/**
  * @author Ilya Sergey
  */
/**
  * A splitter for parallel binary heaps
  *
  * @param heap           A min-heapified array
  * @param heapSize       The size of the heap
  * @param rootPos        The root of the current sub-heap
  * @param remainingGuess An upper-bound on the size of the current sub-heap
  * @tparam T The type of elements contained within the heap
  */
class ParBinHeapSplitter[T: ClassTag](private val heap: Array[T],
                                      private val heapSize: Int,
                                      private val rootPos: Int,
                                      private val remainingGuess: Int) 
  extends IterableSplitter[T] {

  // Current position in the sub-heap to process
  private var pos: Int = rootPos

  private var currHeight = 0
  private var totalNodes = BigInt(2).pow(currHeight).toInt
  private var currNode = 1
  private var currLeft = rootPos
  private var onlyElement = 0

  // Finding the index of the left child
  def leftChild(pos: Int): Int = {
    2*(pos + 1) - 1
  }

  // Finding the index of the right child
  def rightChild(pos: Int): Int = {
    2*(pos+1)
  }

  /**
    * Creating more splitters
    * This better be an O(1) operation!
    */
  override def split: Seq[ParBinHeapSplitter[T]] = {
    if (remaining > 1){
      var result = Seq[ParBinHeapSplitter[T]]()
      result = result :+ new ParBinHeapSplitter[T](heap, heapSize, rootPos, 1)
      if (leftChild(pos) < heapSize){
        result = result :+ new ParBinHeapSplitter[T](heap, heapSize, leftChild(pos), remaining/2)
      }
      if (rightChild(pos) < heapSize){
        result = result :+ new ParBinHeapSplitter[T](heap, heapSize, rightChild(pos), remaining/2)
      }
      result
    } else {
      Seq(this)
    }
  }

  /** Creating a copy of the splitter
    * This should also work in a constant time! 
    */
  override def dup: ParBinHeapSplitter[T] = {
    new ParBinHeapSplitter[T](heap, heapSize, rootPos, remainingGuess)
  }

  /**
    * The number of remaining elements in the sub-heap to process
    *
    * This only needs to be an upper-bound, not an exact figure.
    * Take advantage of this here and use remainingGuess parameter, 
    * because calculating the exact number of elements in 
    * the sub-tree would be expensive.
    */
  override def remaining: Int = {
    remainingGuess
  }

  /**
    * Checks, if this subheap has more elements to process 
    */
  override def hasNext = {
    if (remaining != 1){
      pos < heapSize
    } else {
      onlyElement < remaining
    }
  }

  /**
    * The next element in the "sub-heap" to process 
    */
  override def next(): T = {
    // At the end of the node in a level
    if (currNode >= totalNodes){
      val i = heap(pos)
      currHeight += 1
      totalNodes = BigInt(2).pow(currHeight).toInt
      currNode = 1
      pos = leftChild(currLeft)
      currLeft = pos
      if (remaining == 1){
        onlyElement += 1
      }
      i
    }
    // Still on the same level
    else {
      val i = heap(pos)
      currNode += 1
      pos += 1
      i
    }
  }
}
