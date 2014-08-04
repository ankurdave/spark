/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.storage

import java.io.{BufferedOutputStream, FileOutputStream, File, OutputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import org.apache.spark.Logging
import org.apache.spark.serializer.{SerializationStream, Serializer}

/**
 * An interface for writing JVM objects to some underlying storage. This interface allows
 * appending data to an existing block, and can guarantee atomicity in the case of faults
 * as it allows the caller to revert partial writes.
 *
 * This interface does not support concurrent writes.
 */
private[spark] abstract class BlockObjectWriter(val blockId: BlockId) {

  def open(): BlockObjectWriter

  def close()

  def isOpen: Boolean

  /**
   * Flush the partial writes and commit them as a single atomic block. Return the
   * number of bytes written for this commit.
   */
  def commit(): Long

  /**
   * Reverts writes that haven't been flushed yet. Callers should invoke this function
   * when there are runtime exceptions.
   */
  def revertPartialWrites()

  /**
   * Writes an object.
   */
  def write(value: Any)

  /**
   * Returns the file segment of committed data that this Writer has written.
   */
  def fileSegment(): FileSegment

  /**
   * Cumulative time spent performing blocking writes, in ns.
   */
  def timeWriting(): Long

  /**
   * Number of bytes written so far
   */
  def bytesWritten: Long
}

/** BlockObjectWriter which writes directly to a file on disk. Appends to the given file. */
private[spark] class DiskBlockObjectWriter(
    blockId: BlockId,
    file: File,
    serializer: Serializer,
    bufferSize: Int,
    compressStream: OutputStream => OutputStream,
    syncWrites: Boolean)
  extends BlockObjectWriter(blockId)
  with Logging
{

  /** Intercepts write calls and tracks total time spent writing. Not thread safe. */
  private class TimeTrackingOutputStream(out: OutputStream) extends OutputStream {
    def timeWriting = _timeWriting
    private var _timeWriting = 0L

    private def callWithTiming(f: => Unit) = {
      val start = System.nanoTime()
      f
      _timeWriting += (System.nanoTime() - start)
    }

    def write(i: Int): Unit = callWithTiming(out.write(i))
    override def write(b: Array[Byte]) = callWithTiming(out.write(b))
    override def write(b: Array[Byte], off: Int, len: Int) = callWithTiming(out.write(b, off, len))
    override def close() = out.close()
    override def flush() = out.flush()
  }

  /** The file channel, used for repositioning / truncating the file. */
  private var channel: FileChannel = null
  private var bs: OutputStream = null
  private var fos: FileOutputStream = null
  private var ts: TimeTrackingOutputStream = null
  private var objOut: SerializationStream = null
  private val initialPosition = file.length()
  private var lastValidPosition = initialPosition
  private var initialized = false
  private var _timeWriting = 0L

  override def open(): BlockObjectWriter = {
    fos = new FileOutputStream(file, true)
    ts = new TimeTrackingOutputStream(fos)
    channel = fos.getChannel()
    lastValidPosition = initialPosition
    bs = compressStream(new BufferedOutputStream(ts, bufferSize))
    objOut = serializer.newInstance().serializeStream(bs)
    initialized = true
    this
  }

  override def close() {
    if (initialized) {
      if (syncWrites) {
        // Force outstanding writes to disk and track how long it takes
        objOut.flush()
        val start = System.nanoTime()
        fos.getFD.sync()
        _timeWriting += System.nanoTime() - start
      }
      objOut.close()

      _timeWriting += ts.timeWriting

      channel = null
      bs = null
      fos = null
      ts = null
      objOut = null
      initialized = false
    }
  }

  override def isOpen: Boolean = objOut != null

  override def commit(): Long = {
    if (initialized) {
      // NOTE: Because Kryo doesn't flush the underlying stream we explicitly flush both the
      //       serializer stream and the lower level stream.
      objOut.flush()
      bs.flush()
      val prevPos = lastValidPosition
      lastValidPosition = channel.position()
      lastValidPosition - prevPos
    } else {
      // lastValidPosition is zero if stream is uninitialized
      lastValidPosition
    }
  }

  override def revertPartialWrites() {
    if (initialized) {
      // Discard current writes. We do this by flushing the outstanding writes and
      // truncate the file to the last valid position.
      objOut.flush()
      bs.flush()
      channel.truncate(lastValidPosition)
    }
  }

  override def write(value: Any) {
    if (!initialized) {
      open()
    }
    objOut.writeObject(value)
  }

  override def fileSegment(): FileSegment = {
    new FileSegment(file, initialPosition, bytesWritten)
  }

  // Only valid if called after close()
  override def timeWriting() = _timeWriting

  // Only valid if called after commit()
  override def bytesWritten: Long = {
    lastValidPosition - initialPosition
  }
}

private[spark] class MemoryBlockObjectWriter(
    blockManager: BlockManager,
    blockId: BlockId,
    serializer: Serializer,
    compressStream: OutputStream => OutputStream)
  extends BlockObjectWriter(blockId)
  with Logging {

  private var s: SerializationStream = null // the top-level stream that we write to
  private var bs: OutputStream = null // need access to the next stream down to flush it for Kryo
  private var baos: ByteArrayOutputStream = null // the bottom-level stream - need access to it get the underlying byte array
  private var initialized = false
  private var _bytesWritten: Long = 0

  override def open(): BlockObjectWriter = {
    baos = new ByteArrayOutputStream()
    bs = compressStream(baos)
    s = serializer.newInstance().serializeStream(bs)
    initialized = true
    this
  }

  override def close() {
    if (initialized) {
      s.close()
      baos = null
      bs = null
      s = null
      initialized = false
    }
  }

  override def isOpen: Boolean = s != null

  // Don't call this more than once!
  override def commit(): Long = {
    if (initialized) {
      // NOTE: Because Kryo doesn't flush the underlying stream we explicitly flush both the
      //       serializer stream and the lower level stream.
      s.flush()
      bs.flush()
      val bytes = baos.toByteArray

      // Remove the block if it already exists from a previous commit
      blockManager.removeBlock(blockId, tellMaster = false)

      blockManager.putBytes(
        blockId, ByteBuffer.wrap(bytes), StorageLevel.MEMORY_AND_DISK_SER, tellMaster = false)
      _bytesWritten += bytes.length
      bytes.length
    } else {
      0
    }
  }

  override def revertPartialWrites() {
    if (initialized) {
      s.flush()
      baos.reset()
      _bytesWritten = 0
    }
  }

  override def write(value: Any) {
    if (!initialized) {
      open()
    }
    s.writeObject(value)
  }

  override def fileSegment(): FileSegment = {
    new FileSegment(null, 0, _bytesWritten)
  }

  override def timeWriting() = 0

  // Only valid if called after commit()
  override def bytesWritten: Long = {
    _bytesWritten
  }
}
