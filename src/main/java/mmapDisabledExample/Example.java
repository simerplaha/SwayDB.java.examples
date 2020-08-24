package mmapDisabledExample;

import org.junit.jupiter.api.Test;
import swaydb.data.config.MMAP;
import swaydb.data.util.OperatingSystem;
import swaydb.java.Map;
import swaydb.java.persistent.MapConfig;
import swaydb.persistent.DefaultConfigs;

import java.nio.file.Paths;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static swaydb.java.serializers.Default.intSerializer;
import static swaydb.java.serializers.Default.stringSerializer;

class Example {

  /**
   * An example that demos how to disable MMAP for all files in a persistent instance.
   */
  @Test
  void mmapDisabled() {
    Map<Integer, String, Void> map =
      MapConfig
        .functionsOff(Paths.get("target/myDatabase"), intSerializer(), stringSerializer())
        .setMmapAppendix(MMAP.enabled(OperatingSystem.isWindows()))
        .setMmapMaps(MMAP.disabled())
        .setSegmentConfig(DefaultConfigs.segmentConfig(true).copyWithMmap(MMAP.disabled()))
        .get();

    map.put(42, "forty two");
    assertEquals(map.get(42), Optional.of("forty two"));

    map.delete();
  }
}