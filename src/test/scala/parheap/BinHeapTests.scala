package parheap

import org.scalatest.{FunSpec, Matchers}

import scala.reflect.ClassTag

/**
  * @author Ilya Sergey
  */
class BinHeapTests extends FunSpec with Matchers {

  describe(s"Parallel heap implementation") {

    val MAX_SIZE = 100000
    val arr = ArrayUtil.generateRandomArrayOfInts(MAX_SIZE)

    it("should correctly  return `min`") {
      val heap = new ParBinHeap[Int](arr)
      assert(heap.removeMin == arr.min)
    }

    it("should not lose elements") {
      val heap = new ParBinHeap[Int](arr)
      assert(ArrayUtil.checkSameElements(heap.getHeap().sorted, arr.sorted))
    }


    it("should return elements in the increasing order") {
      val heap = new ParBinHeap[Int](arr)
      var resultArray = Array[Int]()
      for (i <- 0 until MAX_SIZE){
        resultArray = resultArray :+ heap.removeMin
      }
      assert(ArrayUtil.checkSorted(resultArray))
    }
    
    // Do not modify this test!
    it("should correctly return `max` when run with a reversed ordering") {
      // Do not remove this one, otherwise you'll have troubles when implementing the rest of the test!
      implicit val intTag = new ClassTag[Int] {
        override def runtimeClass = Int.getClass
      }

      val arr = ArrayUtil.generateRandomArrayOfInts(100000)
      val heap = new ParBinHeap[Int](arr)((x: Int, y: Int) => y - x, intTag)
      assert(heap.removeMin == arr.max)
    }


  }


}
