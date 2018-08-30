package fs2

import java.nio.charset._
import java.nio.CharBuffer
import java.nio.ByteBuffer
import java.io.UnsupportedEncodingException

/** Provides utilities for working with streams of text (e.g., encoding byte streams to strings). */
object text {
  private val utf8Charset = StandardCharsets.UTF_8
  private lazy val fallbackCharset = try { Charset.forName("Windows-1252") } catch {
    case _: UnsupportedEncodingException => StandardCharsets.US_ASCII
  }

  private def getCoderError(codeResult: CoderResult): Throwable =
    try {
      codeResult.throwException
      throw new NoSuchElementException("throwable of successful encode/decode operation")
    } catch {
      case scala.util.control.NonFatal(t) => t
    }

  /*
  private def emitError[F[_]](res: CoderResult)(
      implicit ev: ApplicativeError[F, Throwable]): Pull[F, Nothing, Nothing] =
    Pull.raiseError(getCoderError(res))
   */

  private def unexpectedError[F[_]](t: Throwable): Pull[F, Nothing, Nothing] =
    Pull.done.map(_ => throw t)

  private def unexpectedCoderResult[F[_]](res: CoderResult): Pull[F, Nothing, Nothing] =
    unexpectedError(getCoderError(res))

  /** Attempts to detect a standard charset from BOM and first byte heuristics
    * roughly as follows:
    * If a BOM of UTF 32, that
    * else if a BOM of UTF-16 or UTF-8, that
    * else if the start of the byte range is a valid UTF-8 character, UTF-8
    * else if  Windows-1252 is supported, that
    * else 7 bit ASCII
    * Note that that means that an UTF-16 LE string that starts with a null
    * character is mistakenly interpreted as the UTF-32 LE BOM
    */
  def detectEncoding[F[_]](
      source: Stream[F, Byte]): Pull[F, Nothing, (Charset, Stream[F, Byte])] = {

    def isUTF8(bytes: List[Byte]): Boolean = bytes match {
      case Nil => false
      case head :: tail => {
        val cont = continuationBytes(head)
        if (cont == -1) false
        else tail.take(cont).forall(_ < -64)
      }
    }

    source.pull.unconsN(4, true).map {
      case None => (fallbackCharset, source)
      case Some((bytes, tail)) =>
        bytes.toList match {
          case -17 :: -69 :: -65 :: x    => (utf8Charset, Stream(x: _*) ++ tail)
          case 0 :: 0 :: -2 :: -1 :: Nil => (Charset.forName("UTF-32BE"), tail)
          case -1 :: -2 :: 0 :: 0 :: Nil => (Charset.forName("UTF-32LE"), tail)
          case -1 :: -2 :: x             => (StandardCharsets.UTF_16LE, Stream(x: _*) ++ tail)
          case -2 :: -1 :: x             => (StandardCharsets.UTF_16BE, Stream(x: _*) ++ tail)
          case l if (isUTF8(l))          => (utf8Charset, Stream(l: _*) ++ tail)
          case l                         => (fallbackCharset, Stream(l: _*) ++ tail)
        }
    }
  }

  def decodeAutodetect[F[_]](bufferSize: Int): Pipe[F, Byte, String] =
    in =>
      detectEncoding(in)
        .flatMap {
          case (charset, tail) => {
            val decodePipe =
              decode[F](charset, bufferSize, CodingErrorAction.REPLACE, CodingErrorAction.REPLACE)
            Pull.output1(decodePipe(tail))
          }
        }
        .stream
        .flatten

  def decode[F[_]](charset: Charset,
                   bufferSize: Int,
                   onMalformedInput: CodingErrorAction,
                   onUnmappableCharacter: CodingErrorAction): Pipe[F, Byte, String] = {

    def writeCharBuffer(buffer: CharBuffer): Pull[F, String, CharBuffer] = {
      buffer.flip
      val str = new java.lang.StringBuilder(buffer).toString
      val outp = if (str != "") Pull.output1(str) else Pull.pure(buffer)
      buffer.clear
      outp.as(buffer)
    }

    def flush(decoder: CharsetDecoder, outputBuffer: CharBuffer): Pull[F, String, Unit] = {
      val res = decoder.flush(outputBuffer)
      val outp = writeCharBuffer(outputBuffer)
      if (res.isError) outp >> unexpectedCoderResult(res)
      else if (res.isOverflow) {
        outputBuffer.clear
        outp.flatMap(o => flush(decoder, o))
      } else outp.as(())
    }

    /** returns a buffer that contains all bytes available in
      * to, followed by all bytes available in from.
      * this may modify either undelying buffer for
      * possibly questionable claims of performance.
      */
    def refillBuffer(from: ByteBuffer, to: ByteBuffer): ByteBuffer =
      if (to.remaining == 0) from
      else {
        val requiredCapacity = from.remaining + to.remaining
        if (true || to.isReadOnly || requiredCapacity > to.capacity) {
          //can't refill buffer. copy both into a new buffer
          val fresh = ByteBuffer.allocate(from.remaining + to.remaining)
          fresh.put(to)
          fresh.put(from)
          fresh.rewind
          fresh
        } else {
          if (to.capacity - to.limit < to.remaining) {
            to.compact
          }
          to.put(from)
        }
      }

    def getInputStep(input: Stream[F, Byte],
                     decoder: CharsetDecoder,
                     inputBuffer: ByteBuffer,
                     outputBuffer: CharBuffer): Pull[F, String, Unit] =
      input.pull.uncons.flatMap {
        case None => {
          val finalDecode = decoder.decode(inputBuffer, outputBuffer, true)
          val written = writeCharBuffer(outputBuffer)
          if (finalDecode.isOverflow) {
            written.flatMap(out => getInputStep(input, decoder, inputBuffer, out))
          } else if (finalDecode.isUnderflow) {
            written.flatMap(o => flush(decoder, o))
          } else if (finalDecode.isError) written >> unexpectedCoderResult(finalDecode)
          else
            written >> unexpectedError(new Exception(s"unexpected decoder state $finalDecode"))
        }
        case Some((chunk, tail)) => {
          val freshInput = refillBuffer(chunk.toByteBuffer, inputBuffer)
          decodeStep(tail, decoder, freshInput, outputBuffer)
        }
      }

    def decodeStep(input: Stream[F, Byte],
                   decoder: CharsetDecoder,
                   inputBuffer: ByteBuffer,
                   outputBuffer: CharBuffer): Pull[F, String, Unit] = {
      val decodeResult = decoder.decode(inputBuffer, outputBuffer, false)
      if (decodeResult.isError) {
        writeCharBuffer(outputBuffer) >> unexpectedCoderResult(decodeResult)
      } else if (decodeResult.isOverflow)
        writeCharBuffer(outputBuffer).flatMap(out => decodeStep(input, decoder, inputBuffer, out))
      else if (decodeResult.isUnderflow) getInputStep(input, decoder, inputBuffer, outputBuffer)
      else
        writeCharBuffer(outputBuffer) >> unexpectedError(
          new Exception(s"unexpected decoder state $decodeResult"))
    }

    def newDecoder() = {
      val dec = charset.newDecoder
      dec.onMalformedInput(onMalformedInput)
      dec.onUnmappableCharacter(onUnmappableCharacter)
      dec
    }

    in =>
      getInputStep(in, newDecoder(), ByteBuffer.allocate(0), CharBuffer.allocate(bufferSize)).stream

  }

  def encode[F[_]](charset: Charset,
                   bufferSize: Int,
                   onMalformedInput: CodingErrorAction,
                   onUnmappableCharacter: CodingErrorAction): Pipe[F, String, Byte] = {

    def refillInputBuffer(from: Chunk[String], to: CharBuffer): (Chunk[String], CharBuffer) = {
      val target =
        if (to.isReadOnly) {
          val b = CharBuffer.allocate(to.capacity)
          b.put(to)
          b
        } else {
          to.compact
          to
        }
      //bench against variation of whole strings only (at least one)
      def rec(from: Chunk[String], to: CharBuffer): (Chunk[String], CharBuffer) =
        if (to.remaining == 0) (from, to)
        else {
          from.head match {
            case None => (from, to)
            case Some(str) => {
              if (str.length <= to.remaining) {
                to.put(str)
                rec(from.drop(1), to)
              } else {
                val (front, back) = str.splitAt(to.remaining)
                to.put(front)
                val arr = from.toArray
                arr(0) = back
                (Chunk.array(arr), to)
              }
            }
          }
        }

      val (ch, buf) = rec(from, target)
      buf.flip
      (ch, buf)
    }

    /** Writes out the byte buffer to the pull, and returns a new bytebuffer
      * ready for reading
      */
    def writeByteBuffer(buffer: ByteBuffer): Pull[F, Byte, ByteBuffer] = {
      buffer.flip
      if (buffer.remaining == 0) Pull.pure(buffer)
      else {
        val array =
          if (buffer.hasArray && buffer.position == 0 && buffer.limit == buffer.capacity)
            buffer.array
          else {
            val arr = Array.ofDim[Byte](buffer.remaining)
            buffer.get(arr)
            arr
          }
        val chunk = Chunk.Bytes(array)
        Pull.output(chunk).as(ByteBuffer.allocate(buffer.capacity))
      }
    }

    def flush(encoder: CharsetEncoder, outputBuffer: ByteBuffer): Pull[F, Byte, Unit] = {
      val res = encoder.flush(outputBuffer)
      val outp = writeByteBuffer(outputBuffer)
      if (res.isError) outp >> unexpectedCoderResult(res)
      else if (res.isOverflow) {
        outputBuffer.clear
        outp.flatMap(o => flush(encoder, o))
      } else outp.as(())
    }

    def getInputStep(input: Stream[F, String],
                     encoder: CharsetEncoder,
                     inputBuffer: CharBuffer,
                     outputBuffer: ByteBuffer): Pull[F, Byte, Unit] =
      input.pull.uncons.flatMap {
        case None => {
          val finalEncode = encoder.encode(inputBuffer, outputBuffer, true)
          val written = writeByteBuffer(outputBuffer)
          if (finalEncode.isOverflow) {
            written.flatMap(out => getInputStep(input, encoder, inputBuffer, out))
          } else if (finalEncode.isUnderflow) {
            written.flatMap(o => flush(encoder, o))
          } else if (finalEncode.isError) {
            //error case reachability depends on CodingErrorActions.
            //REPLACE and IGNORE are fine, REPORT makes this reachable
            //which means I should have an ApplicativeError here and use Pull.raiseError
            written >> unexpectedCoderResult(finalEncode)
          }
          else //should be unreachable
            written >> unexpectedError(new Exception(s"unexpected encoder state $finalEncode"))
        }
        case Some((chunk, tail)) => {
          val (chunkRemainder, freshInput) = refillInputBuffer(chunk, inputBuffer)
          val ttail = if (chunkRemainder.isEmpty) tail else (Stream.chunk(chunk) ++ tail)
          encodeStep(ttail, encoder, freshInput, outputBuffer)
        }
      }

    def encodeStep(input: Stream[F, String],
                   encoder: CharsetEncoder,
                   inputBuffer: CharBuffer,
                   outputBuffer: ByteBuffer): Pull[F, Byte, Unit] = {
      val encodeResult = encoder.encode(inputBuffer, outputBuffer, false)
      if (encodeResult.isError) { //error case reachability depends on CodingErrorActions
        writeByteBuffer(outputBuffer) >> unexpectedCoderResult(encodeResult)
      } else if (encodeResult.isOverflow)
        writeByteBuffer(outputBuffer).flatMap(out => encodeStep(input, encoder, inputBuffer, out))
      else if (encodeResult.isUnderflow)
        getInputStep(input, encoder, inputBuffer, outputBuffer)
      else //should be unreachable
        writeByteBuffer(outputBuffer) >> unexpectedError(
          new Exception(s"unexpected encoder state $encodeResult"))
    }

    def newEncoder(): CharsetEncoder = {
      val enc = charset.newEncoder
      enc.onMalformedInput(onMalformedInput)
      enc.onUnmappableCharacter(onUnmappableCharacter)
      enc
    }

    def inputBuffer = {
      val b = CharBuffer.allocate(bufferSize)
      b.limit(b.capacity)
      b.position(b.limit)
      b
    }

    in =>
      getInputStep(in, newEncoder(), inputBuffer, ByteBuffer.allocate(bufferSize)).stream
  }

  def utf8Decode[F[_]]: Pipe[F, Byte, String] =
    decode(utf8Charset, 128, CodingErrorAction.REPLACE, CodingErrorAction.REPLACE)

  def utf8Encode[F[_]]: Pipe[F, String, Byte] = 
    encode(utf8Charset, 128, CodingErrorAction.REPLACE, CodingErrorAction.REPLACE)

  /** Converts UTF-8 encoded byte stream to a stream of `String`. */
  def utf8DecodeManual[F[_]]: Pipe[F, Byte, String] =
    _.chunks.through(utf8DecodeC)

  /*
   * Returns the number of continuation bytes if `b` is an ASCII byte or a
   * leading byte of a multi-byte sequence, and -1 otherwise.
   */
  private def continuationBytes(b: Byte): Int =
    if ((b & 0x80) == 0x00) 0 // ASCII byte
    else if ((b & 0xE0) == 0xC0) 1 // leading byte of a 2 byte seq
    else if ((b & 0xF0) == 0xE0) 2 // leading byte of a 3 byte seq
    else if ((b & 0xF8) == 0xF0) 3 // leading byte of a 4 byte seq
    else -1 // continuation byte or garbage

  /** Converts UTF-8 encoded `Chunk[Byte]` inputs to `String`. */
  def utf8DecodeC[F[_]]: Pipe[F, Chunk[Byte], String] = {

    /*
     * Returns the length of an incomplete multi-byte sequence at the end of
     * `bs`. If `bs` ends with an ASCII byte or a complete multi-byte sequence,
     * 0 is returned.
     */
    def lastIncompleteBytes(bs: Array[Byte]): Int = {
      val lastThree = bs.drop(0.max(bs.size - 3)).toArray.reverseIterator
      lastThree
        .map(continuationBytes)
        .zipWithIndex
        .find {
          case (c, _) => c >= 0
        }
        .map {
          case (c, i) => if (c == i) 0 else i + 1
        }
        .getOrElse(0)
    }

    def processSingleChunk(outputAndBuffer: (List[String], Chunk[Byte]),
                           nextBytes: Chunk[Byte]): (List[String], Chunk[Byte]) = {
      val (output, buffer) = outputAndBuffer
      val allBytes = Array.concat(buffer.toArray, nextBytes.toArray)
      val splitAt = allBytes.size - lastIncompleteBytes(allBytes)

      if (splitAt == allBytes.size)
        (new String(allBytes.toArray, utf8Charset) :: output, Chunk.empty)
      else if (splitAt == 0)
        (output, Chunk.bytes(allBytes))
      else
        (new String(allBytes.take(splitAt).toArray, utf8Charset) :: output,
         Chunk.bytes(allBytes.drop(splitAt)))
    }

    def doPull(
        buf: Chunk[Byte],
        s: Stream[Pure, Chunk[Byte]]): Pull[Pure, String, Option[Stream[Pure, Chunk[Byte]]]] =
      s.pull.uncons.flatMap {
        case Some((byteChunks, tail)) =>
          val (output, nextBuffer) =
            byteChunks.toList.foldLeft((Nil: List[String], buf))(processSingleChunk)
          Pull.output(Chunk.seq(output.reverse)) >> doPull(nextBuffer, tail)
        case None if !buf.isEmpty =>
          Pull.output1(new String(buf.toArray, utf8Charset)) >> Pull.pure(None)
        case None =>
          Pull.pure(None)
      }

    (in: Stream[Pure, Chunk[Byte]]) =>
      doPull(Chunk.empty, in).stream
  }

  /** Encodes a stream of `String` in to a stream of bytes using the UTF-8 charset. */
  def utf8EncodeManual[F[_]]: Pipe[F, String, Byte] =
    _.flatMap(s => Stream.chunk(Chunk.bytes(s.getBytes(utf8Charset))))

  /** Encodes a stream of `String` in to a stream of `Chunk[Byte]` using the UTF-8 charset. */
  def utf8EncodeC[F[_]]: Pipe[F, String, Chunk[Byte]] =
    _.map(s => Chunk.bytes(s.getBytes(utf8Charset)))

  /** Transforms a stream of `String` such that each emitted `String` is a line from the input. */
  def lines[F[_]]: Pipe[F, String, String] = {

    def linesFromString(string: String): (Vector[String], String) = {
      var i = 0
      var start = 0
      var out = Vector.empty[String]
      while (i < string.size) {
        string(i) match {
          case '\n' =>
            out = out :+ string.substring(start, i)
            start = i + 1
          case '\r' =>
            if (i + 1 < string.size && string(i + 1) == '\n') {
              out = out :+ string.substring(start, i)
              start = i + 2
              i += 1
            }
          case c =>
            ()
        }
        i += 1
      }
      val carry = string.substring(start, string.size)
      (out, carry)
    }

    def extractLines(buffer: Vector[String],
                     chunk: Chunk[String],
                     pendingLineFeed: Boolean): (Chunk[String], Vector[String], Boolean) = {
      @annotation.tailrec
      def loop(remainingInput: Vector[String],
               buffer: Vector[String],
               output: Vector[String],
               pendingLineFeed: Boolean): (Chunk[String], Vector[String], Boolean) =
        if (remainingInput.isEmpty) {
          (Chunk.indexedSeq(output), buffer, pendingLineFeed)
        } else {
          val next = remainingInput.head
          if (pendingLineFeed) {
            if (next.headOption == Some('\n')) {
              val out = (buffer.init :+ buffer.last.init).mkString
              loop(next.tail +: remainingInput.tail, Vector.empty, output :+ out, false)
            } else {
              loop(remainingInput, buffer, output, false)
            }
          } else {
            val (out, carry) = linesFromString(next)
            val pendingLF =
              if (carry.nonEmpty) carry.last == '\r' else pendingLineFeed
            loop(remainingInput.tail,
                 if (out.isEmpty) buffer :+ carry else Vector(carry),
                 if (out.isEmpty) output
                 else output ++ ((buffer :+ out.head).mkString +: out.tail),
                 pendingLF)
          }
        }
      loop(chunk.toVector, buffer, Vector.empty, pendingLineFeed)
    }

    def go(buffer: Vector[String],
           pendingLineFeed: Boolean,
           s: Stream[F, String]): Pull[F, String, Option[Unit]] =
      s.pull.uncons.flatMap {
        case Some((chunk, s)) =>
          val (toOutput, newBuffer, newPendingLineFeed) =
            extractLines(buffer, chunk, pendingLineFeed)
          Pull.output(toOutput) >> go(newBuffer, newPendingLineFeed, s)
        case None if buffer.nonEmpty =>
          Pull.output1(buffer.mkString) >> Pull.pure(None)
        case None => Pull.pure(None)
      }

    s =>
      go(Vector.empty, false, s).stream
  }
}
