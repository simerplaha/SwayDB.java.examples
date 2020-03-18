package configurations;

import scala.concurrent.ExecutionContext;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;
import swaydb.Compression;
import swaydb.Pair;
import swaydb.configs.level.SingleThreadFactory;
import swaydb.data.accelerate.Accelerator;
import swaydb.data.accelerate.LevelZeroMeter;
import swaydb.data.compaction.CompactionExecutionContext;
import swaydb.data.compaction.LevelMeter;
import swaydb.data.compaction.Throttle;
import swaydb.data.compression.LZ4Compressor;
import swaydb.data.compression.LZ4Decompressor;
import swaydb.data.compression.LZ4Instance;
import swaydb.data.config.*;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static swaydb.java.StorageUnits.gb;
import static swaydb.java.StorageUnits.mb;

/**
 * This API requires improvement to suit Java. Currently
 * the following is created from Scala's API. See similar in
 * Scala - https://github.com/simerplaha/SwayDB.scala.examples/blob/master/src/test/scala/configuringlevels/ConfiguringLevels.scala
 */
public class ConfiguringLevels {

  public static void main(String[] args) {

    ExecutionContext myTestSingleThreadExecutionContext =
      new ExecutionContext() {
        ExecutorService threadPool = Executors.newSingleThreadExecutor((ThreadFactory) SingleThreadFactory.create(true));

        @Override
        public void reportFailure(Throwable cause) {
          System.err.println("REPORT FAILURE!" + cause.getMessage());
        }

        @Override
        public void execute(Runnable runnable) {
          threadPool.execute(runnable);
        }
      };

    //sample configuration for a memory level
    MemoryLevelConfig memoryLevel =
      MemoryLevelConfig
        .builder()
        .withMinSegmentSize(mb(4)) //4.mb
        .withMaxKeyValuesPerSegment(100000)
        .withCopyForward(false)
        .withDeleteSegmentsEventually(true)
        //todo provide easier way to access Shared from Java
        .withCompactionExecutionContext(CompactionExecutionContext.shared())
        .withThrottle(
          (LevelMeter levelMeter) -> {
            if (levelMeter.levelSize() > gb(1)) {
              return new Throttle(Duration.Zero(), 10);
            } else {
              return new Throttle(Duration.Zero(), 0);
            }
          }
        )
        .build();

    //sample configuration for a persistent memory level
    PersistentLevelConfig persistentLevel =
      PersistentLevelConfig
        .builder()
        .withDir(Paths.get("level2"))
        //todo provide simpler API for Java
        .withOtherDirs(Arrays.asList(new Dir(Paths.get("level2-1"), 1), new Dir(Paths.get("level2-3"), 1)))
        .withMmapAppendix(true)
        .withAppendixFlushCheckpointSize(mb(4)) //4.mb
        .withSortedKeyIndex(
          SortedKeyIndex
            .builder()
            .withPrefixCompression(new PrefixCompression.Disable(true))
            .withEnablePositionIndex(true)
            .withIoStrategy(ioAction -> new IOStrategy.SynchronisedIO(true))
            .withCompressions(info -> Collections.emptyList())
            .build()
        )
        .withRandomKeyIndex(
          RandomKeyIndex
            .builder()
            .withMaxProbe(1)
            .withMinimumNumberOfKeys(5)
            .withMinimumNumberOfHits(2)
            .withIndexFormat(IndexFormat.Reference$.MODULE$)
            .withAllocateSpace(RandomKeyIndex.RequiredSpace::requiredSpace)
            .withIoStrategy(ioAction -> new IOStrategy.SynchronisedIO(true))
            .withCompression(info -> Collections.emptyList())
            .build()
        )
        .withBinarySearchIndex(
          BinarySearchIndex.fullIndexBuilder()
            .withMinimumNumberOfKeys(10)
            .withIoStrategy(ioAction -> new IOStrategy.SynchronisedIO(true))
            .withIndexFormat(IndexFormat.copyKey())
            .withSearchSortedIndexDirectly(true)
            .withCompression(info -> Collections.emptyList())
            .build()
        )
        .withMightContainKeyIndex(
          MightContainIndex.builder()
            .withFalsePositiveRate(0.01)
            .withUpdateMaxProbe(optimalMaxProbe -> 1)
            .withMinimumNumberOfKeys(10)
            .withIoStrategy(ioAction -> new IOStrategy.SynchronisedIO(true))
            .withCompression(info -> Collections.emptyList())
            .build()
        )
        .withValuesConfig(
          ValuesConfig.builder()
            .withCompressDuplicateValues(true)
            .withCompressDuplicateRangeValues(true)
            .withIoStrategy(ioAction -> new IOStrategy.SynchronisedIO(true))
            .withCompression(info -> Collections.emptyList())
            .build()
        )
        .withSegmentConfig(
          SegmentConfig.builder()
            .withCacheSegmentBlocksOnCreate(true)
            .withDeleteSegmentsEventually(true)
            .withPushForward(true)
            .withMmap(MMAP.disabled())
            .withMinSegmentSize(mb(4))
            .withMaxKeyValuesPerSegment(100000)
            .withIoStrategy(
              ioStrategy -> {
                if (ioStrategy.isOpenResource()) {
                  return new IOStrategy.SynchronisedIO(true);
                } else if (ioStrategy.isReadDataOverview()) {
                  return new IOStrategy.SynchronisedIO(true);
                } else {
                  IOAction.DataAction action = (IOAction.DataAction) ioStrategy;
                  return new IOStrategy.SynchronisedIO(action.isCompressed());
                }
              }
            )
            .withCompression(
              info ->
                Arrays.asList(
                  Compression.lz4Pair(
                    new Pair(LZ4Instance.fastestJavaInstance(), new LZ4Compressor.Fast(20.0)),
                    new Pair(LZ4Instance.fastestJavaInstance(), LZ4Decompressor.fastDecompressor())
                  ),
                  new Compression.Snappy(20.0),
                  Compression.noneCompression()
                )
            )
            .build()
        )
        .withCompactionExecutionContext(CompactionExecutionContext.shared())
        .withThrottle(
          (LevelMeter levelMeter) -> {
            FiniteDuration delay = new FiniteDuration(5 - levelMeter.segmentsCount(), TimeUnit.SECONDS);
            int batch = Math.min(levelMeter.segmentsCount(), 5);
            return new Throttle(delay, batch);
          }
        )
        .build();

    //create custom level hierarchy.
    SwayDBPersistentConfig config =
      ConfigWizard
        .withPersistentLevel0() //LEVEL 0
        .withDir(Paths.get("myMap"))
        .withMapSize(mb(4)) //4.mb
        .withMmap(true)
        .withRecoveryMode(RecoveryMode.reportFailure())
        .withCompactionExecutionContext(new CompactionExecutionContext.Create(myTestSingleThreadExecutionContext))
        .withAcceleration(Accelerator::cruise)
        .withThrottle(
          (LevelZeroMeter meter) -> {
            int mapsCount = meter.mapsCount();
            if (mapsCount > 3) {
              return Duration.Zero();
            } else if (mapsCount > 2) {
              return new FiniteDuration(1, TimeUnit.SECONDS);
            } else {
              return new FiniteDuration(30, TimeUnit.SECONDS);
            }
          }
        )
        .build()
        .withMemoryLevel1(memoryLevel) //LEVEL 1
        .withPersistentLevel(persistentLevel) //level2
        .withTrashLevel(); //level3
  }
}
