package config

import com.typesafe.config.configFactory

object Settings {
  private val config = configFactory.load()

  object WebLogGen {
    private val weblogGen = config.getConfig("clickstream")

    lazy val records = weblogGen.getInt("records")
    lazy val timeMultiplier = weblogGen.getInt("time_multiplier")
    lazy val pages = weblogGen.getInt("pages")
    lazy val visitors = weblogGen.getInt("visitors")
    lazy val filePath = weblogGen.getString("file_path")
    lazy val destPath = weblogGen.getString("dest_path")
    lazy val numberOfFiles = weblogGen.getString("number_of_files")
  }
}