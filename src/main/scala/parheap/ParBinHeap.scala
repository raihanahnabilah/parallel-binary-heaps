package parheap

import parheap.ArrayUtil.swap

import scala.collection.immutable.Seq
import scala.collection.parallel.immutable.ParIterable
import scala.reflect.ClassTag

/**
  * Implementation of a parallel binary min-heap.
  *
  * The implicit parameter `Ordering[T]` allows to use it for standard data types,
  * such as Strings and Ints, which already have ordering defined on them
  */
class ParBinHeap[T: Ordering: ClassTag](private val maxSize: Int)
                    extends ParIterable[T] {

  // This is necessary to enable implicit `Ordering` on `T`s
  // Do not remove this import!
  import Ordering.Implicits._

  // Elements of the heap stored along with their priorities (Int)
  private var heap: Array[T] = new Array[T](maxSize)
  private var heapSize : Int = 0

  // Create a new heap from a given array
  def this(array: Array[T]) = {
    this(array.length)
    heap = array.clone
    heapSize = array.length
    // Turn an array to a heap
    for (pos <- size / 2 to 0 by -1) {
      minHeapify(pos)
    }
  }

  // Getter function to get the current heap
  // for testing purposes
  def getHeap(): Array[T] = {
    heap
  }

  //////////////////////////////////////////////////////////////////////
  // Internal Heap Methods
  //////////////////////////////////////////////////////////////////////

  // Return the position of the parent for the node currently at pos
  private def parent(pos: Int): (Int, T) = {
    if (pos == 0){
      (0, heap(0))
    } else{
      val j = (pos+1)/2 - 1
      (j, heap(j))
    }
  }

  // Return the position of the left child for the node currently at pos
  private def leftChild(pos: Int): Option[(Int, T)] = {
    val j = 2*(pos + 1) - 1
    if (j < heapSize){
      Some(j, heap(j))
    } else {
      None
    }
  }

  // Return the position of the right child for the node currently at pos
  private def rightChild(pos: Int): Option[(Int, T)] = {
    val j = 2*(pos + 1)
    if (j < heapSize){
      Some(j, heap(j))
    } else {
      None
    }
  }

  // true if the passed node is a leaf node 
  private def isLeaf(pos: Int): Boolean = {
    if (pos >= (size / 2) && pos <= size) {
      return true
    }
    false
  }

  // Function to heapify the node at pos 
  private def minHeapify(pos: Int): Unit = {
    if (!isLeaf(pos)){
      var smallest = pos
      if (leftChild(pos) != None){
        val left = leftChild(pos).get._1
        val heapLeft = leftChild(pos).get._2
        if (left < heapSize && heapLeft < heap(smallest)){
          smallest = left
        }
      }
      if (rightChild(pos) != None){
        val right = rightChild(pos).get._1
        val heapRight = rightChild(pos).get._2
        if (right < heapSize && heapRight < heap(smallest)){
          smallest = right
        }
      }
      if (smallest != pos){
        swap(heap, smallest, pos)
        minHeapify(smallest)
      }
    }
  }

  //////////////////////////////////////////////////////////////////////
  // Public heap Methods
  //////////////////////////////////////////////////////////////////////

  /**
    * Inserts element to the heap based on its priority 
    */
  def insert(elem: T): Unit = {
    if (heapSize < maxSize){
      heap(heapSize) = elem
      var curr = heapSize
      heapSize = heapSize + 1
      while (heap(curr) < heap(parent(curr)._1)){
        swap(heap, curr, parent(curr)._1)
        curr = parent(curr)._1
        if (curr == 0){
          return
        }
      }
    }
  }


  /**
    * remove and return the minimum  
    */
  def removeMin: T = {
    val minVal = heap(0)
    heap(0) = heap(heapSize-1)
    heapSize = heapSize-1
    minHeapify(0)
    minVal
  }

  //////////////////////////////////////////////////////////////////////
  // Parallel Collection Methods
  //////////////////////////////////////////////////////////////////////

  /**
    * Returns a sequence of elements currently in the binary heap 
    */
  override def seq: Seq[T] = heap.slice(0, heapSize).toIndexedSeq
  /**
    * Returns a splitter for the binary heap.
    *
    * Use your knowledge of the heap structure to make the best splits.
    *
    */
  override def splitter: ParBinHeapSplitter[T] = {
    new ParBinHeapSplitter[T](heap, heapSize, 0, heapSize)
  }

  /**
    * Returns the number of elements currently in the heap 
    */
  override def size: Int = heapSize
}
  
