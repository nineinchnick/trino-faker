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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

import java.util.Objects;
import java.util.Optional;

public class FakerColumnHandle
        implements ColumnHandle
{
    private final int columnIndex;
    private final String name;
    private final Type type;
    private final double nullProbability;
    private final String generator;

    @JsonCreator
    public FakerColumnHandle(
            @JsonProperty("columnIndex") int columnIndex,
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type,
            @JsonProperty("nullProbability") double nullProbability,
            @JsonProperty("generator") String generator)
    {
        this.columnIndex = columnIndex;
        this.name = name;
        this.type = type;
        this.nullProbability = nullProbability;
        this.generator = generator;
    }

    @JsonProperty("columnIndex")
    public int getColumnIndex()
    {
        return columnIndex;
    }

    @JsonProperty("name")
    public String getName()
    {
        return name;
    }

    @JsonProperty("type")
    public Type getType()
    {
        return type;
    }

    @JsonProperty("nullProbability")
    public double getNullProbability()
    {
        return nullProbability;
    }

    @JsonProperty("generator")
    public Optional<String> getGenerator()
    {
        return Optional.ofNullable(generator);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnIndex);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FakerColumnHandle that = (FakerColumnHandle) o;
        return Objects.equals(columnIndex, that.columnIndex);
    }

    @Override
    public String toString()
    {
        return name + ":" + type;
    }
}
