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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;

public class FakerConfig
{
    private String someSetting = "default-value";

    public String getSomeSetting()
    {
        return someSetting;
    }

    @Config("some_setting")
    public FakerConfig setSomeSetting(String value)
    {
        this.someSetting = value;
        return this;
    }
}
