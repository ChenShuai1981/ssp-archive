package com.vpon.ssp.report.archive.actor

import java.io.ByteArrayOutputStream
import java.util.zip.GZIPOutputStream

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

