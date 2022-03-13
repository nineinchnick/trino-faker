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
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;

import javax.inject.Inject;

import java.util.List;

import static java.util.stream.Collectors.toList;

public class FakerSplitManager
        implements ConnectorSplitManager
{
    private final NodeManager nodeManager;

    @Inject
    public FakerSplitManager(NodeManager nodeManager)
    {
        this.nodeManager = nodeManager;
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            ConnectorSplitManager.SplitSchedulingStrategy splitSchedulingStrategy,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        List<HostAddress> addresses = nodeManager.getRequiredWorkerNodes().stream()
                .map(Node::getHostAndPort)
                .collect(toList());

        return new FixedSplitSource(ImmutableList.of(
                new FakerConnectorSplit((FakerTableHandle) table, addresses)));
    }
}
