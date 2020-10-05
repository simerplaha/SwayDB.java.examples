package configurations;


import org.junit.jupiter.api.Test;
import swaydb.data.config.ForceSave;
import swaydb.data.config.MMAP;
import swaydb.data.util.OperatingSystem;
import swaydb.java.Map;
import swaydb.java.persistent.PersistentMap;

import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static swaydb.java.serializers.Default.intSerializer;
import static swaydb.java.serializers.Default.stringSerializer;

public class ConfiguringMMAPAndForceSaveTest {

  /**
   * For Scala version of this see
   * https://github.com/simerplaha/SwayDB.scala.examples/blob/b2c06e39c6db0f674a17aaa138f409d74256ea4c/src/test/scala/configuringlevels/ConfiguringMMAPAndForceSave.scala
   */
  @Test
  void example_configuration_for_MMAP_and_ForceSave() {

    MMAP.On mmapEnabled =
      MMAP.on(
        //delete after MMAP are cleaned only on Windows.
        OperatingSystem.isWindows(),
        //enable force safe to run before cleaning MMAP files.
        ForceSave.beforeClean(
          false, //disabled applying forceSave before copying MMAP files
          false, //disable forceSave for read-only MMAP files
          true //log time take to execute force-save.
        )
      );

    //create map and apply the above MMAP setting to all files - maps, appendices and segments.
    Map<Integer, String, Void> map =
      PersistentMap
        .functionsOff(Paths.get("target/mmap_force_save_test"), intSerializer(), stringSerializer())
        .setMmapMaps(mmapEnabled)
        .setMmapAppendix(mmapEnabled)
        .setSegmentConfig(swaydb.persistent.DefaultConfigs.segmentConfig(true).copyWithMmap(mmapEnabled))
        .get();

    map.put(1, "one");
    assertEquals(map.get(1).get(), "one");
    map.delete();

    /**
     * The following demos other [[ForceSave]] configurations
     * which can also be used above.
     */

    //disabled force save
    ForceSave forceSaveDisabled =
      ForceSave.off();

    //enables forceSave before clean
    ForceSave forceSaveBeforeClean =
      ForceSave.beforeClean(false, false, true);

    //forceSave before copying files
    ForceSave forceSaveBeforeCopy =
      ForceSave.beforeCopy(false, true);

    //forceSave before closing files
    ForceSave forceSaveBeforeClose =
      ForceSave.beforeClose(false, false, true);

  }

}
