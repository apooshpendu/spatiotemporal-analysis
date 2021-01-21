package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {

  	if (queryRectangle == null || queryRectangle.isEmpty() || pointString == null || pointString.isEmpty())
		return false

	val x_p= BigDecimal(pointString.split(",")(0))
    val y_p= BigDecimal(pointString.split(",")(1))
    
    val x1= BigDecimal(queryRectangle.split(",")(0))
    val y1= BigDecimal(queryRectangle.split(",")(1))
    val x2= BigDecimal(queryRectangle.split(",")(2))
    val y2= BigDecimal(queryRectangle.split(",")(3))

	if (x_p >= x1 && x_p <= x2 && y_p >= y1 && y_p <= y2)
		return true
	else if (x_p >= x2 && x_p <= x1 && y_p >= y2 && y_p <= y1)
		return true
	else
		return false
    
  }

}
