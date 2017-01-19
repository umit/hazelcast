/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.config;

import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ConfigXmlGeneratorTest {

    @Test
    public void testReplicatedMapConfigGenerator() {
        Config config = new Config();
        ReplicatedMapConfig replicatedMapConfig = new ReplicatedMapConfig();
        replicatedMapConfig.setName("replicated-map-name");
        replicatedMapConfig.setStatisticsEnabled(false);
        replicatedMapConfig.setConcurrencyLevel(128);
        replicatedMapConfig.addEntryListenerConfig(new EntryListenerConfig("com.hazelcast.entrylistener", false, false));
        config.addReplicatedMapConfig(replicatedMapConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        ReplicatedMapConfig xmlReplicatedMapConfig = xmlConfig.getReplicatedMapConfig("replicated-map-name");
        assertEquals("replicated-map-name", xmlReplicatedMapConfig.getName());
        assertEquals(false, xmlReplicatedMapConfig.isStatisticsEnabled());
        assertEquals(128, xmlReplicatedMapConfig.getConcurrencyLevel());
        assertEquals("com.hazelcast.entrylistener", xmlReplicatedMapConfig.getListenerConfigs().get(0).getClassName());
    }

    @Test
    public void testCacheQuorumRef() {
        Config config = new Config();
        CacheSimpleConfig cacheConfig = new CacheSimpleConfig();
        cacheConfig.setName("testCache");
        cacheConfig.setQuorumName("testQuorum");
        config.addCacheConfig(cacheConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        CacheSimpleConfig xmlCacheConfig = xmlConfig.getCacheConfig("testCache");
        assertEquals("testQuorum", xmlCacheConfig.getQuorumName());
    }

    @Test
    public void testCacheMergePolicy() {
        Config config = new Config();
        CacheSimpleConfig cacheConfig = new CacheSimpleConfig();
        cacheConfig.setName("testCache");
        cacheConfig.setMergePolicy("testMergePolicy");
        config.addCacheConfig(cacheConfig);
        ConfigXmlGenerator configXmlGenerator = new ConfigXmlGenerator();
        String xml = configXmlGenerator.generate(config);
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        Config xmlConfig = configBuilder.build();

        CacheSimpleConfig xmlCacheConfig = xmlConfig.getCacheConfig("testCache");
        assertEquals("testMergePolicy", xmlCacheConfig.getMergePolicy());
    }

    @Test
    public void testNativeMemory() {
        NativeMemoryConfig nativeMemoryConfig = new NativeMemoryConfig();
        nativeMemoryConfig.setEnabled(true);
        nativeMemoryConfig.setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.STANDARD);
        nativeMemoryConfig.setMetadataSpacePercentage((float) 12.5);
        nativeMemoryConfig.setMinBlockSize(50);
        nativeMemoryConfig.setPageSize(100);
        nativeMemoryConfig.setSize(new MemorySize(20, MemoryUnit.MEGABYTES));

        Config config = new Config()
                .setNativeMemoryConfig(nativeMemoryConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        NativeMemoryConfig xmlNativeMemoryConfig = xmlConfig.getNativeMemoryConfig();
        assertEquals(true, xmlNativeMemoryConfig.isEnabled());
        assertEquals(NativeMemoryConfig.MemoryAllocatorType.STANDARD, nativeMemoryConfig.getAllocatorType());
        assertEquals(12.5, nativeMemoryConfig.getMetadataSpacePercentage(),0.0001);
        assertEquals(50, nativeMemoryConfig.getMinBlockSize());
        assertEquals(100, nativeMemoryConfig.getPageSize());
        assertEquals(new MemorySize(20, MemoryUnit.MEGABYTES).getUnit(), nativeMemoryConfig.getSize().getUnit());
        assertEquals(new MemorySize(20, MemoryUnit.MEGABYTES).getValue(), nativeMemoryConfig.getSize().getValue());
    }

    @Test
    public void testMapAttributesConfig() {
        Config config = new Config();
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName("carMap");

        MapAttributeConfig attrConfig = new MapAttributeConfig();
        attrConfig.setName("power");
        attrConfig.setExtractor("com.car.PowerExtractor");
        mapConfig.addMapAttributeConfig(attrConfig);
        config.addMapConfig(mapConfig);

        Config xmlConfig = getNewConfigViaXMLGenerator(config);

        MapAttributeConfig xmlAttrConfig = xmlConfig.getMapConfig("carMap").getMapAttributeConfigs().get(0);
        assertEquals(attrConfig.getName(), xmlAttrConfig.getName());
        assertEquals(attrConfig.getExtractor(), xmlAttrConfig.getExtractor());
    }

    private static Config getNewConfigViaXMLGenerator(Config config) {
        ConfigXmlGenerator configXmlGenerator = new ConfigXmlGenerator();
        String xml = configXmlGenerator.generate(config);

        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        return configBuilder.build();
    }
}
