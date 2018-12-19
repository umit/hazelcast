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

import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.ICompletableFuture;

import java.util.Collection;

/**
 * The public API used for managing CP sessions.
 */
public interface SessionManagementService {

    /**
     * Returns a non-null collection of sessions that are currently active in the given CP group.
     */
    Collection<Session> getAllSessions(CPGroupId groupId);

    /**
     * If owner of a session crashes, its session is not terminated immediately.
     * Instead, the session will be closed after {@link CPSubsystemConfig#setSessionTimeToLiveSeconds(int)} sessions.
     * If it is known for sure that the session owner is gone and will not come back,
     * this method can be used for closing the session and releasing its resources immediately.
     */
    ICompletableFuture<Boolean> forceCloseSession(CPGroupId groupId, long sessionId);

}
