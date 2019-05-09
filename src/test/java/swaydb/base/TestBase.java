/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */
package swaydb.base;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

@SuppressWarnings({"checkstyle:JavadocMethod", "checkstyle:JavadocType"})
public class TestBase {

    protected static void deleteDirectoryWalkTree(Path path) {
        if (!path.toFile().exists()) {
            return;
        }
        FileVisitor<Path> visitor = new SimpleFileVisitor<Path>() {

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                if (exc != null) {
                    return FileVisitResult.TERMINATE;
                }
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        };
        try {
            Files.walkFileTree(path, visitor);
        } catch (IOException ignored) {
            // ignored
        }
    }

    protected static void deleteDirectoryWalkTreeStartsWith(String startName) throws IOException {
        Files.walk(Paths.get("."))
              .filter(file -> file.toFile().isDirectory())
              .filter(s -> s.getNameCount() == 2)
              .filter(s -> s.getName(1).toString().startsWith(startName))
              .map(Path::getFileName)
              .sorted()
              .forEach(TestBase::deleteDirectoryWalkTree);
    }

}
