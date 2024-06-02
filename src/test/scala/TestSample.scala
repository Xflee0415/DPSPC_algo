import distribute.DPSPC
import org.junit.Test

class TestSample {
  val inPath: String = "D:\\work\\data\\test\\test_letter_sample\\letter_with_label.txt"
  val outPath: String = "D:\\work\\data\\test\\test_letter_sample\\test_sample_20\\result_20_"
  val dim: Int = 16
//  val delim: String = ","
  val numPart: Int = 20
  val sampleSizes: Array[Int] = Array(1, 2, 3, 4, 5,
    10, 15, 20, 25, 30,
    35, 40 ,45, 50, 55,
    60, 65, 70, 75, 80,
    85, 90 ,95, 100)
  val pct: Double = 0.02
//  val projVecNum: Int = 5
  val windowSize: Int = 15
  val theta: Double = 3e-5
  val numOfRep: Int = 10

  def oneExec(sampleSize: Int): Unit = {
    for (i <- 1 to 10) {
      println(s"sample size: $sampleSize, try $i:")
      val outPath_i = outPath + sampleSize
      val dpspc = new DPSPC(inPath, outPath_i, dim, numPart, sampleSize, pct, windowSize, theta, numOfRep)
      dpspc.runDPSPC()
      dpspc.saveResult(i)
    }
  }

  @Test
  def testSample(): Unit = {
    for (sampleSize <- 1 to 20) {
      oneExec(sampleSize)
    }
  }

}
