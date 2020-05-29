/*
 * Copyright 2019 (c) Odnoklassniki
 *
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

package ru.mail.polis.service;

import one.nio.http.HttpClient;
import one.nio.net.ConnectionString;
import one.nio.pool.PoolException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.function.Executable;
import org.junit.platform.commons.util.ExceptionUtils;
import ru.mail.polis.Files;
import ru.mail.polis.TestBase;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.DAOFactory;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Basic init/deinit test for {@link Service} implementation.
 *
 * @author Vadim Tsesko
 */
class StartStopTest extends TestBase {
    private static final long TIMEOUT_MS = TimeUnit.SECONDS.toMillis(1);
    private static final Duration TIMEOUT = Duration.ofMillis(TIMEOUT_MS * 2);

    private int port;
    private File data;
    private DAO dao;
    private Service kvService;
    private HttpClient client;

    @BeforeEach
    void beforeEach() throws IOException {
        data = Files.createTempDirectory();
        dao = DAOFactory.create(data);
        port = randomPort();
        kvService = ServiceFactory.create(port, dao, Collections.singleton(endpoint(port)));
        reset();
    }

    @AfterEach
    void afterEach() throws IOException {
        client.close();
        kvService.stop();
        dao.close();
        Files.recursiveDelete(data);
    }

    private int status() throws Exception {
        return client.get("/v0/status").getStatus();
    }

    private void reset() {
        if (client != null) {
            client.close();
        }
        client = new HttpClient(new ConnectionString("http://localhost:" + port));
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    void create() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            assertThrows(PoolException.class, this::status);
        });
    }

    @Test
    @EnabledOnOs(OS.WINDOWS)
    void createOnWindows() throws InterruptedException {
        assertNotFinishesIn(this::status);
    }

    @Test
    void start() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            kvService.start();
            Thread.sleep(TimeUnit.SECONDS.toMillis(1));

            assertEquals(200, status());
        });
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    void stop() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            makeLifecycle();

            // Should not respond after stop
            assertThrows(PoolException.class, this::status);
        });
    }

    @Test
    @EnabledOnOs(OS.WINDOWS)
    void stopOnWindows() throws InterruptedException {
        assertNotFinishesIn(() -> {
            makeLifecycle();

            // Should not respond after stop
            status();
        });
    }

    private void makeLifecycle() throws Exception {
        kvService.start();
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));

        assertEquals(200, status());

        kvService.stop();
        reset();
    }

    private static void assertNotFinishesIn(final Executable executable)
    throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(() -> {
            try {
                executable.execute();
                latch.countDown();
            } catch (Throwable throwable) {
                throwable.printStackTrace();
                ExceptionUtils.throwAsUncheckedException(throwable);
            }
        }).start();
        boolean completedBeforeTimeout = latch.await(StartStopTest.TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        if (completedBeforeTimeout) {
            fail("Executable has completed before timeout while should not have been");
        }
    }
}
