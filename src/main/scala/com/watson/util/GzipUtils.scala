package com.watson.util

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

/**
  * GzipUtils
  *
  * Created by watson on 2018/9/5.
  */
object GzipUtils {

  /**
    * 压缩
    */
  def compress(input: Array[Byte]): Array[Byte] = {
    val bos = new ByteArrayOutputStream(input.length)
    val gzip = new GZIPOutputStream(bos)
    gzip.write(input)
    gzip.finish()
    gzip.close()
    val compressed = bos.toByteArray
    bos.close()
    compressed
  }

  /**
    * 解压
    */
  def decompress(compressed: Array[Byte]): String = {
    val bis = new ByteArrayInputStream(compressed)
    val gzip = new GZIPInputStream(bis)
    val buf = new Array[Byte](1024)
    var num = gzip.read(buf, 0, buf.length)
    val bos = new ByteArrayOutputStream
    while (num != -1) {
      bos.write(buf, 0, num)
      num = gzip.read(buf, 0, buf.length)
    }
    gzip.close()
    bis.close()
    val ret = bos.toString
    bos.flush()
    bos.close()
    ret
  }
}
