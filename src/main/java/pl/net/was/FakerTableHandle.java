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
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.Objects;
import java.util.Optional;

public class FakerTableHandle
        implements ConnectorTableHandle, Cloneable
{
    private final Long id;
    private final SchemaTableName schemaTableName;
    private TupleDomain<ColumnHandle> constraint;
    private long limit;

    @JsonCreator
    public FakerTableHandle(
            @JsonProperty("id") Long id,
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("limit") long limit)
    {
        this.id = id;
        this.schemaTableName = schemaTableName;
        this.constraint = constraint;
        this.limit = limit;
    }

    @JsonProperty
    public Optional<Long> getId()
    {
        return Optional.ofNullable(id);
    }

    @JsonProperty("schemaTableName")
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty("constraint")
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonProperty("limit")
    public long getLimit()
    {
        return limit;
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
                schemaTableName.equals(that.schemaTableName) &&
                constraint.equals(that.constraint) &&
                limit == that.limit;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, schemaTableName, constraint, limit);
    }

    public String toString()
    {
        return schemaTableName.getTableName();
    }

    @Override
    public FakerTableHandle clone()
    {
        try {
            return (FakerTableHandle) super.clone();
        }
        catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    public FakerTableHandle cloneWithConstraint(TupleDomain<ColumnHandle> constraint)
    {
        FakerTableHandle tableHandle = this.clone();
        tableHandle.constraint = constraint;
        return tableHandle;
    }

    public FakerTableHandle cloneWithLimit(long limit)
    {
        FakerTableHandle tableHandle = this.clone();
        tableHandle.limit = limit;
        return tableHandle;
    }
}
