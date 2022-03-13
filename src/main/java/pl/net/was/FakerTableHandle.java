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
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;

import java.util.Objects;

public class FakerTableHandle
        implements ConnectorTableHandle
{
    private final long id;
    private final SchemaTableName schemaTableName;

    @JsonCreator
    public FakerTableHandle(
            @JsonProperty("id") long id,
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName)
    {
        this.id = id;
        this.schemaTableName = schemaTableName;
    }

    @JsonProperty
    public long getId()
    {
        return id;
    }

    @JsonProperty("schemaTableName")
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
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
        FakerTableHandle that = (FakerTableHandle) o;
        return id == that.id &&
                schemaTableName.equals(that.schemaTableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, schemaTableName);
    }

    public String toString()
    {
        return schemaTableName.getTableName();
    }
}
