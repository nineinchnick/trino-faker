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
import io.trino.spi.ptf.ConnectorTableFunctionHandle;

import java.util.Objects;

import static java.lang.String.format;

public class SequenceTableHandle
        implements ConnectorTableFunctionHandle
{
    private final long start;
    private final long stop;
    private final long step;

    @JsonCreator
    public SequenceTableHandle(
            @JsonProperty("start") long start,
            @JsonProperty("stop") long stop,
            @JsonProperty("step") long step)
    {
        this.start = start;
        this.stop = stop;
        this.step = step;
    }

    @JsonProperty
    public long getStart()
    {
        return start;
    }

    @JsonProperty
    public long getStop()
    {
        return stop;
    }

    @JsonProperty
    public long getStep()
    {
        return step;
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
        SequenceTableHandle that = (SequenceTableHandle) o;
        return start == that.start && stop == that.stop && step == that.step;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(start, stop, step);
    }

    public String toString()
    {
        return format("Sequence(%d, %d, %d)", start, stop, step);
    }
}
