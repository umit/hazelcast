package com.hazelcast.spi;

import java.util.concurrent.TimeUnit;

/**
 * TODO: Javadoc Pending...
 *
 */
public interface GracefulShutdownAwareService {

    boolean onShutdown(long timeout, TimeUnit unit);

}
