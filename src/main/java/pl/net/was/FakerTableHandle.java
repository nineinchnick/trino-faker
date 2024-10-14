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

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import static java.util.Objects.requireNonNull;

public record FakerTableHandle(Long id, SchemaTableName schemaTableName, TupleDomain<ColumnHandle> constraint, long limit)
        implements ConnectorTableHandle
{
    public FakerTableHandle
    {
        requireNonNull(id, "id is null");
        requireNonNull(schemaTableName, "schemaTableName is null");
        requireNonNull(constraint, "constraint is null");
    }

    public FakerTableHandle withConstraint(TupleDomain<ColumnHandle> constraint)
    {
        return new FakerTableHandle(id, schemaTableName, constraint, limit);
    }

    public FakerTableHandle withLimit(long limit)
    {
        return new FakerTableHandle(id, schemaTableName, constraint, limit);
    }
}
