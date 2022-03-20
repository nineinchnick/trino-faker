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
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ColumnInfo
        implements Cloneable
{
    private final ColumnHandle handle;
    private final String name;
    private final Type type;
    private Map<String, Object> properties;
    private Optional<String> comment;
    private final ColumnMetadata metadata;

    public static final String NULL_PROBABILITY_PROPERTY = "null_probability";
    public static final String GENERATOR_PROPERTY = "generator";

    public ColumnInfo(ColumnHandle handle, String name, Type type, Map<String, Object> properties, Optional<String> comment)
    {
        this(handle, ColumnMetadata.builder()
                .setName(name)
                .setType(type)
                .setProperties(properties)
                .setComment(comment)
                .build());
    }

    public ColumnInfo(ColumnHandle handle, ColumnMetadata metadata)
    {
        this.handle = requireNonNull(handle, "handle is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.name = metadata.getName();
        this.type = metadata.getType();
        this.properties = metadata.getProperties();
        this.comment = Optional.ofNullable(metadata.getComment());
    }

    public ColumnHandle getHandle()
    {
        return handle;
    }

    public String getName()
    {
        return name;
    }

    public ColumnMetadata getMetadata()
    {
        return metadata;
    }

    @Override
    public String toString()
    {
        return name + "::" + type;
    }

    @Override
    public ColumnInfo clone()
    {
        try {
            return (ColumnInfo) super.clone();
        }
        catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    public ColumnInfo cloneWithProperties(Map<String, Object> properties)
    {
        ColumnInfo tableInfo = this.clone();
        tableInfo.properties = properties;
        return tableInfo;
    }

    public ColumnInfo cloneWithComment(Optional<String> comment)
    {
        ColumnInfo tableInfo = this.clone();
        tableInfo.comment = comment;
        return tableInfo;
    }
}
