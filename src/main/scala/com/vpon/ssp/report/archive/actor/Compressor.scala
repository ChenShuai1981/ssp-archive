package com.vpon.ssp.report.archive.actor

import java.io.ByteArrayOutputStream
import java.util.zip.GZIPOutputStream

import org.anarres.lzo.{LzoOutputStream, LzoLibrary, LzoAlgorithm}
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream


trait ICompressor {
  def compressData(inputData: Array[Byte]): Array[Byte]
}

object GZIPCompressor extends ICompressor {

  def compressData(inputData: Array[Byte]): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val zos = new GZIPOutputStream(baos)
    zos.write(inputData)
    zos.finish()
    zos.flush()
    val outputData = baos.toByteArray
    zos.close()
    baos.flush()
    baos.close()
    outputData
  }

}

object BZIP2Compressor extends ICompressor {

  def compressData(inputData: Array[Byte]): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val zos = new BZip2CompressorOutputStream(baos)
    zos.write(inputData)
    zos.finish()
    zos.flush()
    val outputData = baos.toByteArray
    zos.close()
    baos.flush()
    baos.close()
    outputData
  }

}

object LZOPCompressor extends ICompressor {

  val algorithm = LzoAlgorithm.LZO1X
  val compressor = LzoLibrary.getInstance().newCompressor(algorithm, null)

  def compressData(inputData: Array[Byte]): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val zos = new LzoOutputStream(baos, compressor, 256)
    zos.write(inputData)
    zos.flush()
    val outputData = baos.toByteArray
    zos.close()
    baos.flush()
    baos.close()
    outputData
  }

}
