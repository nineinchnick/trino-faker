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

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import io.airlift.log.Logger;
import io.trino.benchmark.AbstractBenchmark;
import io.trino.benchmark.BenchmarkResultHook;
import io.trino.benchmark.BenchmarkSuite;
import io.trino.benchmark.JsonAvgBenchmarkResultWriter;
import io.trino.benchmark.JsonBenchmarkResultWriter;
import io.trino.benchmark.OdsBenchmarkResultWriter;
import io.trino.benchmark.SimpleLineBenchmarkResultWriter;
import io.trino.testing.LocalQueryRunner;
import pl.net.was.benchmark.BaselineRandomPrimitiveType;
import pl.net.was.benchmark.RandomPrimitiveType;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FakerBenchmarkSuite
{
    private static final Logger LOGGER = Logger.get(BenchmarkSuite.class);

    private final LocalQueryRunner localQueryRunner;
    private final String outputDirectory;

    public FakerBenchmarkSuite(LocalQueryRunner localQueryRunner, String outputDirectory)
    {
        this.localQueryRunner = localQueryRunner;
        this.outputDirectory = outputDirectory;
    }

    public static List<AbstractBenchmark> createBenchmarks(LocalQueryRunner localQueryRunner)
    {
        return ImmutableList.of(
                new BaselineRandomPrimitiveType(localQueryRunner),
                new RandomPrimitiveType(localQueryRunner));
    }

    public void runAllBenchmarks()
            throws IOException
    {
        List<AbstractBenchmark> benchmarks = createBenchmarks(localQueryRunner);

        LOGGER.info("=== Pre-running all benchmarks for JVM warmup ===");
        for (AbstractBenchmark benchmark : benchmarks) {
            benchmark.runBenchmark();
        }

        LOGGER.info("=== Actually running benchmarks for metrics ===");
        for (AbstractBenchmark benchmark : benchmarks) {
            try (OutputStream jsonOut = new FileOutputStream(createOutputFile(format("%s/json/%s.json", outputDirectory, benchmark.getBenchmarkName())));
                    OutputStream jsonAvgOut = new FileOutputStream(createOutputFile(format("%s/json-avg/%s.json", outputDirectory, benchmark.getBenchmarkName())));
                    OutputStream csvOut = new FileOutputStream(createOutputFile(format("%s/csv/%s.csv", outputDirectory, benchmark.getBenchmarkName())));
                    OutputStream odsOut = new FileOutputStream(createOutputFile(format("%s/ods/%s.json", outputDirectory, benchmark.getBenchmarkName())))) {
                benchmark.runBenchmark(
                        new ForwardingBenchmarkResultWriter(
                                ImmutableList.of(
                                        new JsonBenchmarkResultWriter(jsonOut),
                                        new JsonAvgBenchmarkResultWriter(jsonAvgOut),
                                        new SimpleLineBenchmarkResultWriter(csvOut),
                                        new OdsBenchmarkResultWriter("trino.benchmark." + benchmark.getBenchmarkName(), odsOut))));
            }
        }
    }

    private static File createOutputFile(String fileName)
            throws IOException
    {
        File outputFile = new File(fileName);
        Files.createParentDirs(outputFile);
        return outputFile;
    }

    private static class ForwardingBenchmarkResultWriter
            implements BenchmarkResultHook
    {
        private final List<BenchmarkResultHook> benchmarkResultHooks;

        private ForwardingBenchmarkResultWriter(List<BenchmarkResultHook> benchmarkResultHooks)
        {
            requireNonNull(benchmarkResultHooks, "benchmarkResultHooks is null");
            this.benchmarkResultHooks = ImmutableList.copyOf(benchmarkResultHooks);
        }

        @Override
        public BenchmarkResultHook addResults(Map<String, Long> results)
        {
            requireNonNull(results, "results is null");
            for (BenchmarkResultHook benchmarkResultHook : benchmarkResultHooks) {
                benchmarkResultHook.addResults(results);
            }
            return this;
        }

        @Override
        public void finished()
        {
            for (BenchmarkResultHook benchmarkResultHook : benchmarkResultHooks) {
                benchmarkResultHook.finished();
            }
        }
    }
}
