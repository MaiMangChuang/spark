

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Hdfs, Path}

object DataRow {
  val BASE_URL ="hdfs://192.168.133.129:9000"
  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val localPath =new Path("traffic.csv")
    val hdfsPath =new Path("/user/root")

    val hdfs=FileSystem.get(URI.create(BASE_URL),conf)
     hdfs.copyFromLocalFile(localPath,hdfsPath)
    println(localPath.getName)
  }
}
