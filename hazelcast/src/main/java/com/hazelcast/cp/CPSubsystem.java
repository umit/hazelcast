/*
 *  Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp;

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.spi.annotation.Beta;

/**
 * TODO: Javadoc Pending...
 *
 */
@Beta
public interface CPSubsystem {

    /**
     * TODO
     * @param name
     * @return
     */
    IAtomicLong getAtomicLong(String name);

    /**
     * TODO
     *
     * @param name
     * @return
     */
    <E> IAtomicReference<E> getAtomicReference(String name);

    /**
     * TODO
     * @param key
     * @return
     */
    FencedLock getFencedLock(String key);

    /**
     * TODO
     *
     * @param name
     * @return
     */
    ICountDownLatch getCountDownLatch(String name);

    /**
     * TODO
     * @param name
     * @return
     */
    ISemaphore getSemaphore(String name);

    /**
     * TODO
     * @return
     */
    CPSubsystemManagementService getCPSubsystemManagementService();

    /**
     * TODO
     * @return
     */
    SessionManagementService getSessionManagementService();

}
