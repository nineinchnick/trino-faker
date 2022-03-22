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

import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import net.datafaker.Faker;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.StandardTypes.VARCHAR;

public final class FakerFunctions
{
    private final Faker faker;

    public FakerFunctions()
    {
        faker = new Faker();
    }

    @ScalarFunction
    @Description("Generate a random string based on the Faker expression")
    @SqlType(VARCHAR)
    public Slice randomExpression(@SqlType(VARCHAR) Slice expression)
    {
        return utf8Slice(faker.expression(expression.toStringUtf8()));
    }
}
