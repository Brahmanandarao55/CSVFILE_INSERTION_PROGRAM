package csv_insertion

import org.mockito.Mockito.spy
import org.scalatest.{FlatSpec, Matchers}

class UnitTest extends FlatSpec with Matchers{

  it should "hi frnds" in{
      val x = spy(edited)
    val result = x.check()
    result should be ("Updated files")
  }



}
