package parheap

import org.scalatest.{FunSpec, Matchers}

import scala.util.{Failure, Success, Try}

/**
  * @author Ilya Sergey
  */
class ParBinHeapSplitterTests extends FunSpec with Matchers {

  private val ARR_SIZE = 10000000

  // Ensure we have some repeats
  private val arr = ArrayUtil.generateRandomArrayOfBoundedInts(ARR_SIZE, ARR_SIZE / 10) 
  private val heap = new ParBinHeap[Int](arr)

  describe(s"Parallel heap implementation") {

    it("should correctly `count`") {
      val seqResult = arr.count(_ == 3)
      val parResult = heap.count(_ == 3)
      assert(seqResult == parResult)
    }

    it("should correctly find `max`") {
      val seqResult = arr.max
      val parResult = heap.max
      assert(seqResult == parResult)
    }

    it("should correctly implement custom operations based on splitting") {
      // Aggregate
      val aggSeqResult = arr.aggregate[List[Int]](Nil)((acc, num) => if(num == 3) num :: acc else acc, (acc1, acc2) => acc1 ++ acc2)
      val aggParResult = heap.aggregate[List[Int]](Nil)((acc, num) => if(num == 3) num :: acc else acc, (acc1, acc2) => acc1 ++ acc2)
      assert(aggParResult == aggSeqResult)

      // FoldLeft
      val foldSeqResult = arr.foldLeft(0)((acc,num) => if(num==3) num + acc else acc)
      val foldParResult = heap.foldLeft(0)((acc, num) => if(num == 3) num + acc else acc)
      assert(foldSeqResult == foldParResult)
    }

    /**
      * This test checks that your splitter respects the heap structure. 
      * Specifically, it verifies that all splitters produce sub-heaps where 
      * the first element (from the left) is the minimum element of the sub-heap.
      *
      * Do not modify this test!
      */
    it("should preserve the structure of the heap") {

      val splitMin = heap.aggregate[Try[Option[Int]]](Success(None))(
        (acc, n) => {
          acc match {
            case Success(Some(min)) =>
              if (n < min) Failure(new IllegalStateException(s"Encountered minimum element in non-head position n=$n, min=$min"))
              else Success(Some(min))
            case Success(None) =>
              Success(Some(n))
            case Failure(f) =>
              Failure(f)
          }
        },
        (acc1, acc2) => {
          for {
            acc1_s <- acc1
            acc2_s <- acc2
          } yield {

            for {
              acc1_sn <- acc1_s
              acc2_sn <- acc2_s
            } yield {
              Seq(acc1_sn, acc2_sn).min
            }

          }
        }
      )

      splitMin match {
        case Success(Some(min)) =>
          assert(min == arr.min)
        case Success(None) =>
          assert(false, "minimum should not be None")
        case Failure(f) =>
          assert(false, s"encountered exception: $f")

      }

    }

  }

}