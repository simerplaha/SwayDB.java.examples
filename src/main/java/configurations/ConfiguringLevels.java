package configurations;

import scala.concurrent.ExecutionContext;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;
import scala.jdk.javaapi.CollectionConverters;
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

    SwayDBPersistentConfig config =
      ConfigWizard
        .addPersistentLevel0( //level0
          Paths.get("myMap"),
          4000000, //4.mb
          true,
          RecoveryMode.reportFailure(),
          new CompactionExecutionContext.Create(myTestSingleThreadExecutionContext),
          Accelerator::cruise,
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
        .addMemoryLevel1( //level1
          4000000, //4.mb
          100000,
          false,
          true,
          //todo provide easier way to access Shared from Java
          CompactionExecutionContext.Shared$.MODULE$,
          (LevelMeter levelMeter) -> {
            if (levelMeter.levelSize() > 1000000000) {
              return new Throttle(Duration.Zero(), 10);
            } else {
              return new Throttle(Duration.Zero(), 0);
            }
          }
        )
        .addPersistentLevel( //level2
          Paths.get("level2"),
          //todo provide simpler API for Java
          CollectionConverters.asScala(Arrays.asList(new Dir(Paths.get("level2-1"), 1), new Dir(Paths.get("level2-3"), 1))).toList(),
          true,
          4000000, //4.mb
          SortedKeyIndex.enableJava(
            new PrefixCompression.Disable(true),
            true,
            ioAction -> new IOStrategy.SynchronisedIO(true),
            info -> Collections.emptyList()
          ),
          RandomKeyIndex.enableJava(
            1,
            5,
            2,
            IndexFormat.Reference$.MODULE$,
            RandomKeyIndex.RequiredSpace::requiredSpace,
            ioAction -> new IOStrategy.SynchronisedIO(true),
            info -> Collections.emptyList()
          ),
          BinarySearchIndex.fullIndexJava(
            10,
            ioAction -> new IOStrategy.SynchronisedIO(true),
            IndexFormat.CopyKey$.MODULE$,
            true,
            info -> Collections.emptyList()
          ),
          MightContainIndex.enableJava(
            0.01,
            optimalMaxProbe -> 1,
            10,
            ioAction -> new IOStrategy.SynchronisedIO(true),
            info -> Collections.emptyList()
          ),
          ValuesConfig.createJava(
            true,
            true,
            ioAction -> new IOStrategy.SynchronisedIO(true),
            info -> Collections.emptyList()
          ),
          SegmentConfig.createJava(
            true,
            true,
            true,
            MMAP.disabled(),
            4000000,
            100000,
            ioStrategy -> {
              if (ioStrategy.isOpenResource()) {
                return new IOStrategy.SynchronisedIO(true);
              } else if (ioStrategy.isReadDataOverview()) {
                return new IOStrategy.SynchronisedIO(true);
              } else {
                IOAction.DataAction action = (IOAction.DataAction) ioStrategy;
                return new IOStrategy.SynchronisedIO(action.isCompressed());
              }
            },
            info ->
              Arrays.asList(
                Compression.lz4Pair(
                  new Pair(LZ4Instance.fastestJavaInstance(), new LZ4Compressor.Fast(20.0)),
                  new Pair(LZ4Instance.fastestJavaInstance(), LZ4Decompressor.fastDecompressor())
                ),
                new Compression.Snappy(20.0),
                Compression.noneCompression()
              )
          ),
          CompactionExecutionContext.Shared$.MODULE$,
          (LevelMeter levelMeter) -> {
            FiniteDuration delay = new FiniteDuration(5 - levelMeter.segmentsCount(), TimeUnit.SECONDS);
            int batch = Math.min(levelMeter.segmentsCount(), 5);
            return new Throttle(delay, batch);
          }
        )
        .addTrashLevel(); //level3
  }
}
