package com.lzx.streaming


import java.io.InputStreamReader
import java.util.Properties

object PropertiesUtil {
    val prop: Properties = new Properties
    def load(propertiesName:String):Properties={


      prop.load(new InputStreamReader(
          Thread.currentThread.
          getContextClassLoader.
          getResourceAsStream(propertiesName),"UTF-8")
      )
      prop
    }

}
