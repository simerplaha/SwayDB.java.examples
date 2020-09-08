package configurations;

import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;
import swaydb.Compression;
import swaydb.Pair;
import swaydb.data.accelerate.Accelerator;
import swaydb.data.accelerate.LevelZeroMeter;
import swaydb.data.compaction.CompactionExecutionContext;
import swaydb.data.compaction.LevelMeter;
import swaydb.data.compaction.Throttle;
import swaydb.data.compression.LZ4Compressor;
import swaydb.data.compression.LZ4Decompressor;
import swaydb.data.compression.LZ4Instance;
import swaydb.data.config.*;
import swaydb.data.util.OperatingSystem;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static swaydb.java.StorageUnits.gb;
import static swaydb.java.StorageUnits.mb;

/**
 * The following demo all configurations by configuring a 4 Leveled database.
 * <p>
 * - Level0 - In-Memory
 * - Level1 - In-Memory
 * - Level2 - Persistent
 * - Level3 - Trash
 * <p>
 * For detailed info on each config see documentation: http://swaydb.io/configurations/?language=java/
 */
public class ConfiguringLevels {

  public static void main(String[] args) {

    //sample configuration for a memory level
    MemoryLevelConfig memoryLevel =
      MemoryLevelConfig
        .builder()
        .minSegmentSize(mb(4)) //4.mb
        .maxKeyValuesPerSegment(100000)
        .copyForward(false)
        .deleteSegmentsEventually(true)
        .compactionExecutionContext(CompactionExecutionContext.shared())
        .throttle(
          (LevelMeter levelMeter) -> {
            if (levelMeter.levelSize() > gb(1)) {
              return new Throttle(Duration.Zero(), 10);
            } else {
              return new Throttle(Duration.Zero(), 0);
            }
          }
        );

    //sample configuration for a persistent memory level
    PersistentLevelConfig persistentLevel =
      PersistentLevelConfig
        .builder()
        .dir(Paths.get("level2"))
        .otherDirs(Arrays.asList(new Dir(Paths.get("level2-1"), 1), new Dir(Paths.get("level2-3"), 1)))
        .mmapAppendix(MMAP.enabled(OperatingSystem.isWindows(), ForceSave.disabled()))
        .appendixFlushCheckpointSize(mb(4)) //4.mb
        .sortedKeyIndex(
          SortedKeyIndex
            .builder()
            .prefixCompression(new PrefixCompression.Disable(true))
            .enablePositionIndex(true)
            .blockIOStrategy(ioAction -> new IOStrategy.SynchronisedIO(true))
            .compressions(info -> Collections.emptyList())
        )
        .randomKeyIndex(
          RandomKeyIndex
            .builder()
            .maxProbe(1)
            .minimumNumberOfKeys(5)
            .minimumNumberOfHits(2)
            .indexFormat(IndexFormat.Reference$.MODULE$)
            .allocateSpace(RandomKeyIndex.RequiredSpace::requiredSpace)
            .blockIOStrategy(ioAction -> new IOStrategy.SynchronisedIO(true))
            .compression(info -> Collections.emptyList())
        )
        .binarySearchIndex(
          BinarySearchIndex.fullIndexBuilder()
            .minimumNumberOfKeys(10)
            .searchSortedIndexDirectly(true)
            .indexFormat(IndexFormat.copyKey())
            .blockIOStrategy(ioAction -> new IOStrategy.SynchronisedIO(true))
            .compression(info -> Collections.emptyList())
        )
        .mightContainKeyIndex(
          MightContainIndex.builder()
            .falsePositiveRate(0.01)
            .updateMaxProbe(optimalMaxProbe -> 1)
            .minimumNumberOfKeys(10)
            .blockIOStrategy(ioAction -> new IOStrategy.SynchronisedIO(true))
            .compression(info -> Collections.emptyList())
        )
        .valuesConfig(
          ValuesConfig.builder()
            .compressDuplicateValues(true)
            .compressDuplicateRangeValues(true)
            .blockIOStrategy(ioAction -> new IOStrategy.SynchronisedIO(true))
            .compression(info -> Collections.emptyList())
        )
        .segmentConfig(
          SegmentConfig.builder()
            .cacheSegmentBlocksOnCreate(true)
            .deleteSegmentsEventually(true)
            .pushForward(true)
            .mmap(MMAP.disabled(ForceSave.disabled()))
            .minSegmentSize(mb(4))
            .maxKeyValuesPerSegment(100000)
            .fileOpenIOStrategy(new IOStrategy.SynchronisedIO(true))
            .blockIOStrategy(
              ioStrategy -> {
                if (ioStrategy.isReadDataOverview()) {
                  return new IOStrategy.SynchronisedIO(true);
                } else {
                  return new IOStrategy.SynchronisedIO(ioStrategy.isCompressed());
                }
              }
            )
            .compression(
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
        )
        .compactionExecutionContext(CompactionExecutionContext.shared())
        .throttle(
          (LevelMeter levelMeter) -> {
            FiniteDuration delay = new FiniteDuration(5 - levelMeter.segmentsCount(), TimeUnit.SECONDS);
            int batch = Math.min(levelMeter.segmentsCount(), 5);
            return new Throttle(delay, batch);
          }
        );

    //create custom level hierarchy.
    SwayDBPersistentConfig config =
      ConfigWizard
        .withPersistentLevel0() //LEVEL 0
        .dir(Paths.get("myMap"))
        .mapSize(mb(4)) //4.mb
        .mmap(MMAP.enabled(OperatingSystem.isWindows(), ForceSave.disabled()))
        .recoveryMode(RecoveryMode.reportFailure())
        .compactionExecutionContext(CompactionExecutionContext.create(Executors.newSingleThreadExecutor()))
        .acceleration(Accelerator::cruise)
        .throttle(
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
        .withMemoryLevel1(memoryLevel) //LEVEL 1
        .withPersistentLevel(persistentLevel) //level2
        .withTrashLevel(); //level3
  }
}
