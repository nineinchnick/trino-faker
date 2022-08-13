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

import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.ptf.AbstractConnectorTableFunction;
import io.trino.spi.ptf.Argument;
import io.trino.spi.ptf.Descriptor;
import io.trino.spi.ptf.ScalarArgument;
import io.trino.spi.ptf.ScalarArgumentSpecification;
import io.trino.spi.ptf.TableFunctionAnalysis;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.ptf.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.stream.Collectors.toList;

public class SequenceFunction
        extends AbstractConnectorTableFunction
{
    public SequenceFunction()
    {
        super(
                "default",
                "sequence",
                List.of(
                        ScalarArgumentSpecification.builder()
                                .name("START")
                                .type(BIGINT)
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name("STOP")
                                .type(BIGINT)
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name("STEP")
                                .type(BIGINT)
                                .defaultValue(1L)
                                .build()),
                GENERIC_TABLE);
    }

    @Override
    public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
    {
        long start = (long) ((ScalarArgument) arguments.get("START")).getValue();
        long stop = (long) ((ScalarArgument) arguments.get("STOP")).getValue();
        long step = (long) ((ScalarArgument) arguments.get("STEP")).getValue();

        if (start < 1) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "start must be positive");
        }
        if (stop < 1) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "stop must be positive");
        }
        if (step < 1) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "step must be positive");
        }

        // determine the returned row type
        List<Descriptor.Field> fields = Stream.of("seq")
                .map(name -> new Descriptor.Field(name, Optional.of(BIGINT)))
                .collect(toList());

        Descriptor returnedType = new Descriptor(fields);

        return TableFunctionAnalysis.builder()
                .returnedType(returnedType)
                .handle(new SequenceTableHandle(start, stop, step))
                .build();
    }
}
