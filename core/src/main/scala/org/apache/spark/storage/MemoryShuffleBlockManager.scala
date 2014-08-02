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

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._

import org.apache.spark.Logging
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.{MetadataCleaner, MetadataCleanerType, TimeStampedHashMap}
import org.apache.spark.util.collection.{PrimitiveKeyOpenHashMap, PrimitiveVector}
import org.apache.spark.shuffle.sort.SortShuffleManager

/**
 * Manages assigning disk-based block writers to shuffle tasks. Each shuffle task gets
 * one file per reducer (this set of files is called a ShuffleFileGroup).
 *
 * As an optimization to reduce the number of physical shuffle files produced, multiple shuffle
 * blocks are aggregated into the same file. There is one "combined shuffle file" per reducer
 * per concurrently executing shuffle task. As soon as a task finishes writing to its shuffle
 * files, it releases them for another task.
 * Regarding the implementation of this feature, shuffle files are identified by a 3-tuple:
 *   - shuffleId: The unique id given to the entire shuffle stage.
 *   - bucketId: The id of the output partition (i.e., reducer id)
 *   - fileId: The unique id identifying a group of "combined shuffle files." Only one task at a
 *       time owns a particular fileId, and this id is returned to a pool when the task finishes.
 * Each shuffle file is then mapped to a FileSegment, which is a 3-tuple (file, offset, length)
 * that specifies where in a given file the actual block data is located.
 *
 * Shuffle file metadata is stored in a space-efficient manner. Rather than simply mapping
 * ShuffleBlockIds directly to FileSegments, each ShuffleFileGroup maintains a list of offsets for
 * each block stored in each file. In order to find the location of a shuffle block, we search the
 * files within a ShuffleFileGroups associated with the block's reducer.
 */
// TODO: Factor this into a separate class for each ShuffleManager implementation
private[spark]
class MemoryShuffleBlockManager(blockManager: BlockManager) extends ShuffleBlockManager with Logging {
  def conf = blockManager.conf

  // Are we using sort-based shuffle?
  val sortBasedShuffle =
    conf.get("spark.shuffle.manager", "") == classOf[SortShuffleManager].getName

  /**
   * Contains all the state related to a particular shuffle.
   */
  private class ShuffleState(val numBuckets: Int) {
    /**
     * The BlockIds of all map tasks completed on this Executor for this shuffle.
     */
    val completedMapTasks = new ConcurrentLinkedQueue[ShuffleBlockId]()
  }

  private val shuffleStates = new TimeStampedHashMap[ShuffleId, ShuffleState]

  private val metadataCleaner =
    new MetadataCleaner(MetadataCleanerType.SHUFFLE_BLOCK_MANAGER, this.cleanup, conf)

  /**
   * Register a completed map without getting a ShuffleWriterGroup. Used by sort-based shuffle
   * because it just writes a single file by itself.
   */
  override def addCompletedMap(blockId: ShuffleBlockId, numBuckets: Int): Unit = {
    shuffleStates.putIfAbsent(blockId.shuffleId, new ShuffleState(numBuckets))
    val shuffleState = shuffleStates(blockId.shuffleId)
    shuffleState.completedMapTasks.add(blockId)
  }

  override def forMapTask(shuffleId: Int, mapId: Int, numBuckets: Int, serializer: Serializer) = {
    new ShuffleWriterGroup {
      shuffleStates.putIfAbsent(shuffleId, new ShuffleState(numBuckets))
      private val shuffleState = shuffleStates(shuffleId)
      private val blockIds = Array.tabulate[ShuffleBlockId](numBuckets) { bucketId =>
        ShuffleBlockId(shuffleId, mapId, bucketId)
      }

      override val writers: Array[BlockObjectWriter] = blockIds.map { blockId =>
        // TODO: delete the block from memory if it already exists because of previous failures
        blockManager.getMemoryWriter(blockId, serializer)
      }

      override def releaseWriters(success: Boolean) {
        for (blockId <- blockIds) shuffleState.completedMapTasks.add(blockId, numBuckets)
      }

      private def getUnusedFileGroup(): ShuffleFileGroup = {
        val fileGroup = shuffleState.unusedFileGroups.poll()
        if (fileGroup != null) fileGroup else newFileGroup()
      }

      private def newFileGroup(): ShuffleFileGroup = {
        val fileId = shuffleState.nextFileId.getAndIncrement()
        val files = Array.tabulate[File](numBuckets) { bucketId =>
          val filename = physicalFileName(shuffleId, bucketId, fileId)
          blockManager.diskBlockManager.getFile(filename)
        }
        val fileGroup = new ShuffleFileGroup(fileId, shuffleId, files)
        shuffleState.allFileGroups.add(fileGroup)
        fileGroup
      }

      private def recycleFileGroup(group: ShuffleFileGroup) {
        shuffleState.unusedFileGroups.add(group)
      }
    }
  }

  /**
   * Returns the physical file segment in which the given BlockId is located.
   * This function should only be called if shuffle file consolidation is enabled, as it is
   * an error condition if we don't find the expected block.
   */
  def getBlockLocation(id: ShuffleBlockId): FileSegment = {
    // Search all file groups associated with this shuffle.
    val shuffleState = shuffleStates(id.shuffleId)
    for (fileGroup <- shuffleState.allFileGroups) {
      val segment = fileGroup.getFileSegmentFor(id.mapId, id.reduceId)
      if (segment.isDefined) { return segment.get }
    }
    throw new IllegalStateException("Failed to find shuffle block: " + id)
  }

  /** Remove all the blocks / files and metadata related to a particular shuffle. */
  def removeShuffle(shuffleId: ShuffleId): Boolean = {
    // Do not change the ordering of this, if shuffleStates should be removed only
    // after the corresponding shuffle blocks have been removed
    val cleaned = removeShuffleBlocks(shuffleId)
    shuffleStates.remove(shuffleId)
    cleaned
  }

  /** Remove all the blocks / files related to a particular shuffle. */
  private def removeShuffleBlocks(shuffleId: ShuffleId): Boolean = {
    shuffleStates.get(shuffleId) match {
      case Some(state) =>
        if (sortBasedShuffle) {
          // There's a single block ID for each map, plus an index file for it
          for (mapId <- state.completedMapTasks) {
            val blockId = new ShuffleBlockId(shuffleId, mapId, 0)
            blockManager.diskBlockManager.getFile(blockId).delete()
            blockManager.diskBlockManager.getFile(blockId.name + ".index").delete()
          }
        } else if (consolidateShuffleFiles) {
          for (fileGroup <- state.allFileGroups; file <- fileGroup.files) {
            file.delete()
          }
        } else {
          for (mapId <- state.completedMapTasks; reduceId <- 0 until state.numBuckets) {
            val blockId = new ShuffleBlockId(shuffleId, mapId, reduceId)
            blockManager.diskBlockManager.getFile(blockId).delete() // TODO: handle in-memory case
          }
        }
        logInfo("Deleted all files for shuffle " + shuffleId)
        true
      case None =>
        logInfo("Could not find files for shuffle " + shuffleId + " for deleting")
        false
    }
  }

  private def physicalFileName(shuffleId: Int, bucketId: Int, fileId: Int) = {
    "merged_shuffle_%d_%d_%d".format(shuffleId, bucketId, fileId)
  }

  private def cleanup(cleanupTime: Long) {
    shuffleStates.clearOldValues(cleanupTime, (shuffleId, state) => removeShuffleBlocks(shuffleId))
  }

  def stop() {
    metadataCleaner.cancel()
  }
}

private[spark]
object ShuffleBlockManager {
  /**
   * A group of shuffle files, one per reducer.
   * A particular mapper will be assigned a single ShuffleFileGroup to write its output to.
   */
  private class ShuffleFileGroup(val shuffleId: Int, val fileId: Int, val files: Array[File]) {
    /**
     * Stores the absolute index of each mapId in the files of this group. For instance,
     * if mapId 5 is the first block in each file, mapIdToIndex(5) = 0.
     */
    private val mapIdToIndex = new PrimitiveKeyOpenHashMap[Int, Int]()

    /**
     * Stores consecutive offsets of blocks into each reducer file, ordered by position in the file.
     * This ordering allows us to compute block lengths by examining the following block offset.
     * Note: mapIdToIndex(mapId) returns the index of the mapper into the vector for every
     * reducer.
     */
    private val blockOffsetsByReducer = Array.fill[PrimitiveVector[Long]](files.length) {
      new PrimitiveVector[Long]()
    }

    def numBlocks = mapIdToIndex.size

    def apply(bucketId: Int) = files(bucketId)

    def recordMapOutput(mapId: Int, offsets: Array[Long]) {
      mapIdToIndex(mapId) = numBlocks
      for (i <- 0 until offsets.length) {
        blockOffsetsByReducer(i) += offsets(i)
      }
    }

    /** Returns the FileSegment associated with the given map task, or None if no entry exists. */
    def getFileSegmentFor(mapId: Int, reducerId: Int): Option[FileSegment] = {
      val file = files(reducerId)
      val blockOffsets = blockOffsetsByReducer(reducerId)
      val index = mapIdToIndex.getOrElse(mapId, -1)
      if (index >= 0) {
        val offset = blockOffsets(index)
        val length =
          if (index + 1 < numBlocks) {
            blockOffsets(index + 1) - offset
          } else {
            file.length() - offset
          }
        assert(length >= 0)
        Some(new FileSegment(file, offset, length))
      } else {
        None
      }
    }
  }
}
