/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pl.net.was;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.testing.LocalQueryRunner;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public final class BenchmarkQueryRunner
{
    private BenchmarkQueryRunner() {}

    public static LocalQueryRunner createLocalQueryRunner(File tempDir)
    {
        Session session = testSessionBuilder()
                .setCatalog("faker")
                .setSchema("default")
                .build();

        LocalQueryRunner localQueryRunner = LocalQueryRunner.create(session);

        localQueryRunner.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.of());

        localQueryRunner.installPlugin(new FakerPlugin());

        localQueryRunner.createCatalog(
                "faker",
                "faker",
                Map.of());

        localQueryRunner.execute("CREATE TABLE orders AS SELECT * FROM tpch.sf1.orders WITH NO DATA");
        localQueryRunner.execute("CREATE TABLE lineitem AS SELECT * FROM tpch.sf1.lineitem WITH NO DATA");
        return localQueryRunner;
    }

    public static void main(String[] args)
            throws IOException
    {
        String outputDirectory = requireNonNull(System.getProperty("outputDirectory"), "Must specify -DoutputDirectory=...");
        File tempDir = Files.createTempDirectory(null).toFile();
        try (LocalQueryRunner localQueryRunner = createLocalQueryRunner(tempDir)) {
            new FakerBenchmarkSuite(localQueryRunner, outputDirectory).runAllBenchmarks();
        }
        finally {
            deleteRecursively(tempDir.toPath(), ALLOW_INSECURE);
        }
    }
}
