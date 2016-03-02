package com.vpon.ssp.report.dedup.generator

package protocol {

import java.io.{ByteArrayOutputStream, DataOutputStream}

import scala.annotation.implicitNotFound

@implicitNotFound(msg = "Could not find a Writes for ${T}")
trait Writes[T] {

  def writes(value: T): Array[Byte]
}

class DataOutputStreamWrites[T](writeValue: (DataOutputStream, T) => Unit) extends Writes[T] {

  def writes(value: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream
    val dos = new DataOutputStream(bos)
    writeValue(dos, value)
    dos.flush()
    val byteArray = bos.toByteArray
    bos.close()
    byteArray
  }
}

object Defaults {

  implicit object WritesString extends Writes[String] {
    def writes(value: String): Array[Byte] = value.getBytes("UTF-8")
  }

  //  implicit object WritesLong extends DataOutputStreamWrites[Long](_.writeLong(_))
  //
  //  implicit object WritesInt extends DataOutputStreamWrites[Int](_.writeInt(_))
  //
  //  implicit object WritesShort extends DataOutputStreamWrites[Short](_.writeShort(_))

}

}

package crypto {

import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec

import org.apache.commons.codec.binary.Base64

import com.vpon.ssp.report.dedup.generator.protocol.Writes

trait Encryption {
  def encrypt(dataBytes: Array[Byte], secret: String): Array[Byte]

  def decrypt(codeBytes: Array[Byte], secret: String): Array[Byte]

  def base64DesEncrypt(dataBytes: Array[Byte], secret: String): String

  def base64DesEncrypt(str: String, secret: String): String

  def base64DesDecrypt(str: String, secret: String): Array[Byte]

  def encrypt[T: Writes](data: T, secret: String): Array[Byte] = encrypt(implicitly[Writes[T]].writes(data), secret)
}

class JavaCryptoEncryption(algorithmName: String) extends Encryption {

  def encrypt(bytes: Array[Byte], secret: String): Array[Byte] = {
    val secretKey = new SecretKeySpec(secret.getBytes("UTF-8"), algorithmName)
    val encipher = Cipher.getInstance(algorithmName)
    encipher.init(Cipher.ENCRYPT_MODE, secretKey)
    encipher.doFinal(bytes)
  }

  def decrypt(bytes: Array[Byte], secret: String): Array[Byte] = {
    val secretKey = new SecretKeySpec(secret.getBytes("UTF-8"), algorithmName)
    val encipher = Cipher.getInstance(algorithmName)
    encipher.init(Cipher.DECRYPT_MODE, secretKey)
    encipher.doFinal(bytes)
  }

  def base64DesEncrypt(dataBytes: Array[Byte], secret: String): String = {
    Base64.encodeBase64String(encrypt(dataBytes, secret))
  }

  def base64DesEncrypt(str: String, secret: String): String = {
    Base64.encodeBase64String(encrypt(str.getBytes, secret))
  }

  def base64DesDecrypt(str: String, secret: String): Array[Byte] = {
    decrypt(Base64.decodeBase64(str), secret)
  }
}

object DES extends JavaCryptoEncryption("DES")

object AES extends JavaCryptoEncryption("AES")

}
