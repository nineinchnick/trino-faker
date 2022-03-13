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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

public class TestFakerQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return FakerQueryRunner.createQueryRunner();
    }

    @Test
    public void showTables()
    {
        assertQuery("SHOW SCHEMAS FROM faker", "VALUES 'default', 'information_schema'");
        assertUpdate("CREATE TABLE faker.default.test (id INTEGER, name VARCHAR)");
        assertTableColumnNames("faker.default.test", "id", "name");
    }

    @Test
    public void selectFromTable()
    {
        assertUpdate("CREATE TABLE faker.default.names (id INTEGER, name VARCHAR)");
        assertQuery("SELECT count(distinct id) AS ids, count(distinct name) as names FROM names",
                "VALUES (1, 1)");
        assertUpdate("DROP TABLE faker.default.names");
    }
}
