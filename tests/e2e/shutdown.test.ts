import { expect } from "chai";
import "mocha";
import { execFileSync } from "child_process";

import AmqpCacoon, {
  ConfirmChannel,
  ConsumeMessage,
  IAmqpCacoonConfig,
} from "../../src";

const E2E_HOST = process.env.AMQP_E2E_HOST || "127.0.0.1";
const E2E_PORT = Number(process.env.AMQP_E2E_PORT || "5673");
const E2E_USERNAME = process.env.AMQP_E2E_USERNAME || "guest";
const E2E_PASSWORD = process.env.AMQP_E2E_PASSWORD || "guest";
const CONNECTION_TIMEOUT_MS = 15000;
const SHUTDOWN_WAIT_ASSERTION_MS = 250;
const MESSAGE_WAIT_TIMEOUT_MS = 10000;
const COMPOSE_FILE = "docker-compose.e2e.yml";

function delay(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function createDeferred() {
  let resolve!: () => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<void>((innerResolve, innerReject) => {
    resolve = innerResolve;
    reject = innerReject;
  });
  return { promise, resolve, reject };
}

async function waitForCondition(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs: number,
  errorMessage: string,
) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    if (await predicate()) {
      return;
    }
    await delay(100);
  }

  throw new Error(errorMessage);
}

function createQueueName() {
  return `amqp-cacoon-e2e-shutdown-${Date.now()}-${Math.random()
    .toString(16)
    .slice(2)}`;
}

function createCleanupQueueName() {
  return `amqp-cacoon-e2e-cleanup-${Date.now()}-${Math.random()
    .toString(16)
    .slice(2)}`;
}

function createConfig(queue: string): IAmqpCacoonConfig {
  return {
    protocol: "amqp",
    username: E2E_USERNAME,
    password: E2E_PASSWORD,
    host: E2E_HOST,
    port: E2E_PORT,
    amqp_opts: {
      heartbeatIntervalInSeconds: 5,
      reconnectTimeInSeconds: 5,
      connectionOptions: {},
    },
    providers: {},
    onChannelConnect: async (channel: ConfirmChannel) => {
      await channel.assertQueue(queue, {
        durable: false,
        autoDelete: false,
      });
      await channel.prefetch(1);
    },
  };
}

function createSharedConfig(queue: string): IAmqpCacoonConfig {
  return {
    ...createConfig(queue),
    shareConnection: true,
  };
}

async function waitForRabbitMq(queue: string) {
  const probe = new AmqpCacoon(createConfig(queue));
  const startedAt = Date.now();
  let lastError: unknown;

  try {
    while (Date.now() - startedAt < CONNECTION_TIMEOUT_MS) {
      try {
        const channel = await probe.getPublishChannel();
        await channel.waitForConnect();
        return;
      } catch (error) {
        lastError = error;
        await delay(500);
      }
    }
  } finally {
    await probe.close();
  }

  throw new Error(
    `RabbitMQ was not reachable within ${CONNECTION_TIMEOUT_MS}ms. Last error: ${String(lastError)}`,
  );
}

function stopRabbitMq() {
  execFileSync("docker", ["compose", "-f", COMPOSE_FILE, "stop", "rabbitmq"], {
    cwd: process.cwd(),
    stdio: "pipe",
  });
}

function startRabbitMq() {
  execFileSync("docker", ["compose", "-f", COMPOSE_FILE, "up", "-d", "rabbitmq"], {
    cwd: process.cwd(),
    stdio: "pipe",
  });
}

function removeRabbitMq() {
  execFileSync("docker", ["compose", "-f", COMPOSE_FILE, "down", "-v"], {
    cwd: process.cwd(),
    stdio: "pipe",
  });
}

describe("Amqp Cacoon E2E", function () {
  this.timeout(30000);

  let queueName: string;
  let cleanupQueueName: string;
  let consumer: AmqpCacoon;
  let publisher: AmqpCacoon;

  before(async () => {
    startRabbitMq();
    await waitForRabbitMq(createQueueName());
  });

  beforeEach(async () => {
    queueName = createQueueName();
    cleanupQueueName = createCleanupQueueName();
    await waitForRabbitMq(queueName);
    consumer = new AmqpCacoon(createConfig(queueName));
    publisher = new AmqpCacoon(createConfig(queueName));

    await (await consumer.getConsumerChannel()).waitForConnect();
    await (await publisher.getPublishChannel()).waitForConnect();
  });

  afterEach(async () => {
    startRabbitMq();
    await waitForRabbitMq(queueName);

    try {
      const channel = await publisher.getPublishChannel();
      await channel.waitForConnect();
      await channel.deleteQueue(queueName);
      await channel.deleteQueue(cleanupQueueName);
    } catch (error) {
      // Ignore cleanup failures when the connection has already been closed.
    }

    await Promise.allSettled([consumer?.close(), publisher?.close()]);
    globalThis.amqpCacoonConnections = [];
    (AmqpCacoon as any).sharedConnections = {};
  });

  after(async () => {
    removeRabbitMq();
  });

  /**
   * Verifies the core graceful shutdown contract for a normal consumer.
   * We need this because shutdown correctness depends on waiting for in-flight work
   * instead of closing channels underneath a still-running handler.
   *
   * Steps:
   * 1. Register a consumer whose handler blocks until the test releases it.
   * 2. Publish one message and wait until the handler has started.
   * 3. Trigger `gracefullShutdown()`.
   * 4. Assert shutdown does not resolve while the handler is still blocked.
   * 5. Release the handler and assert shutdown then completes.
   */
  it("gracefullShutdown waits for an in-flight consumer to finish before closing", async () => {
    const handlerStarted = createDeferred();
    const allowHandlerToFinish = createDeferred();
    const handlerFinished = createDeferred();
    let shutdownResolved = false;
    let prePublishCalledBeforeHandlerFinished = false;
    let preCloseCalledBeforeHandlerFinished = false;
    let handlerHasFinished = false;

    await consumer.registerConsumer(
      queueName,
      async (channel, msg: ConsumeMessage) => {
        handlerStarted.resolve();
        await allowHandlerToFinish.promise;
        channel.ack(msg);
        handlerHasFinished = true;
        handlerFinished.resolve();
      },
      { noAck: false },
    );

    await publisher.publish(
      "",
      queueName,
      Buffer.from("shutdown-test-message"),
    );
    await handlerStarted.promise;

    const shutdownPromise = consumer
      .gracefullShutdown({
        prePublishCallback: async () => {
          if (!handlerHasFinished) {
            prePublishCalledBeforeHandlerFinished = true;
          }
        },
        preCloseCallback: async () => {
          if (!handlerHasFinished) {
            preCloseCalledBeforeHandlerFinished = true;
          }
        },
      })
      .then(() => {
        shutdownResolved = true;
      });

    await delay(SHUTDOWN_WAIT_ASSERTION_MS);
    expect(
      shutdownResolved,
      "gracefullShutdown resolved before the long-running handler finished",
    ).to.equal(false);
    expect(
      prePublishCalledBeforeHandlerFinished,
      "prePublishCallback ran before the in-flight handler completed",
    ).to.equal(false);
    expect(
      preCloseCalledBeforeHandlerFinished,
      "preCloseCallback ran before the in-flight handler completed",
    ).to.equal(false);

    allowHandlerToFinish.resolve();
    await handlerFinished.promise;
    await shutdownPromise;

    expect(
      prePublishCalledBeforeHandlerFinished,
      "prePublishCallback should not run until the handler has completed",
    ).to.equal(false);
    expect(
      preCloseCalledBeforeHandlerFinished,
      "preCloseCallback should not run until the handler has completed",
    ).to.equal(false);
    expect(shutdownResolved, "gracefullShutdown never resolved").to.equal(true);
  });

  /**
   * Verifies backlog behavior during shutdown when `prefetch: 1` is in use.
   * We need this because the expected operational behavior is that only the
   * in-flight message is processed, while the remaining queued backlog stays on RabbitMQ.
   *
   * Steps:
   * 1. Register a blocking consumer with `prefetch: 1`.
   * 2. Publish ten messages to create a backlog.
   * 3. Wait until only the first message is in-flight.
   * 4. Trigger graceful shutdown while that first handler is blocked.
   * 5. Release the handler, let shutdown complete, and inspect the queue.
   * 6. Assert one message was processed and the other nine remain queued.
   */
  it("leaves queued backlog messages on RabbitMQ when shutdown happens during the first in-flight message", async () => {
    const totalMessages = 10;
    const handlerStarted = createDeferred();
    const allowHandlerToFinish = createDeferred();
    const handlerFinished = createDeferred();
    let processedMessages = 0;

    await consumer.registerConsumer(
      queueName,
      async (channel, msg: ConsumeMessage) => {
        processedMessages++;
        handlerStarted.resolve();
        await allowHandlerToFinish.promise;
        channel.ack(msg);
        handlerFinished.resolve();
      },
      { noAck: false, prefetch: 1 },
    );

    for (let i = 0; i < totalMessages; i++) {
      await publisher.publish(
        "",
        queueName,
        Buffer.from(`shutdown-backlog-message-${i}`),
      );
    }

    await handlerStarted.promise;

    const shutdownPromise = consumer.gracefullShutdown({
      prePublishCallback: async () => Promise.resolve(),
      preCloseCallback: async () => Promise.resolve(),
    });

    await delay(SHUTDOWN_WAIT_ASSERTION_MS);
    expect(
      processedMessages,
      "prefetch=1 should only allow the first message to be in flight before shutdown",
    ).to.equal(1);

    allowHandlerToFinish.resolve();
    await handlerFinished.promise;
    await shutdownPromise;

    const publisherChannel = await publisher.getPublishChannel();
    await publisherChannel.waitForConnect();
    const queueState = await publisherChannel.checkQueue(queueName);

    expect(
      processedMessages,
      "only the first in-flight message should have been processed",
    ).to.equal(1);
    expect(
      queueState.messageCount,
      "the remaining backlog should still be present on the queue after shutdown",
    ).to.equal(totalMessages - 1);
  });

  /**
   * Verifies consumer recovery after a real broker restart.
   * We need this because reconnecting and resubscribing consumers is one of the
   * primary guarantees this wrapper is expected to provide in production.
   *
   * Steps:
   * 1. Register a consumer and prove it receives a message before restart.
   * 2. Stop RabbitMQ to break the underlying connection.
   * 3. Start RabbitMQ again and wait for the broker to become reachable.
   * 4. Publish another message after restart.
   * 5. Assert the original consumer resumes and receives the post-restart message.
   */
  it("reconnects a consumer and resumes message delivery after RabbitMQ restarts", async () => {
    const receivedMessages: Array<string> = [];
    const messageAfterRestart = createDeferred();

    await consumer.registerConsumer(
      queueName,
      async (channel, msg: ConsumeMessage) => {
        const payload = msg.content.toString();
        receivedMessages.push(payload);
        channel.ack(msg);
        if (payload === "after-restart") {
          messageAfterRestart.resolve();
        }
      },
      { noAck: false, prefetch: 1 },
    );

    await publisher.publish("", queueName, Buffer.from("before-restart"));
    await waitForCondition(
      () => receivedMessages.includes("before-restart"),
      MESSAGE_WAIT_TIMEOUT_MS,
      "consumer did not receive the message before broker restart",
    );

    stopRabbitMq();
    await delay(1000);
    startRabbitMq();
    await waitForRabbitMq(queueName);

    const postRestartPublisher = new AmqpCacoon(createConfig(queueName));
    try {
      const channel = await postRestartPublisher.getPublishChannel();
      await channel.waitForConnect();
      await postRestartPublisher.publish("", queueName, Buffer.from("after-restart"));
      await messageAfterRestart.promise;
    } finally {
      await postRestartPublisher.close();
    }

    expect(receivedMessages, "consumer did not resume after reconnect").to.deep.equal([
      "before-restart",
      "after-restart",
    ]);
  });

  /**
   * Verifies publish buffering across a broker outage.
   * We need this because callers rely on queued publishes surviving short outages
   * and being delivered after the connection manager reconnects.
   *
   * Steps:
   * 1. Stop RabbitMQ before publishing.
   * 2. Call `publish()` while the broker is unavailable.
   * 3. Start RabbitMQ again and wait for reconnect.
   * 4. Wait for the queued publish promise to settle successfully.
   * 5. Inspect the queue and assert the message was eventually delivered.
   */
  it("publishes queued messages after RabbitMQ reconnects", async () => {
    stopRabbitMq();
    await delay(1000);

    const queuedPublish = publisher.publish(
      "",
      queueName,
      Buffer.from("published-during-outage"),
    );

    startRabbitMq();
    await waitForRabbitMq(queueName);
    await queuedPublish;

    const verifier = new AmqpCacoon(createConfig(queueName));
    try {
      const channel = await verifier.getPublishChannel();
      await channel.waitForConnect();
      await waitForCondition(
        async () => {
          const queueState = await channel.checkQueue(queueName);
          return queueState.messageCount === 1;
        },
        MESSAGE_WAIT_TIMEOUT_MS,
        "queued publish was not delivered after broker reconnect",
      );

      const queueState = await channel.checkQueue(queueName);
      expect(
        queueState.messageCount,
        "expected the offline publish to be present after reconnect",
      ).to.equal(1);
    } finally {
      await verifier.close();
    }
  });

  /**
   * Verifies the minimal shared-connection mode actually shares one underlying connection.
   * We need this because the feature is only useful if matching instances reuse the same
   * `AmqpConnectionManager` rather than silently creating parallel connections.
   *
   * Steps:
   * 1. Create two matching instances with `shareConnection: true`.
   * 2. Open channels on both instances.
   * 3. Assert both instances point at the same underlying connection object.
   */
  it("reuses the same underlying connection for matching shared instances", async () => {
    const sharedPublisher = new AmqpCacoon(createSharedConfig(queueName));
    const sharedConsumer = new AmqpCacoon(createSharedConfig(queueName));

    try {
      await (await sharedPublisher.getPublishChannel()).waitForConnect();
      await (await sharedConsumer.getConsumerChannel()).waitForConnect();

      expect(
        (sharedPublisher as any).connection,
        "shared instances should reuse the same AmqpConnectionManager",
      ).to.equal((sharedConsumer as any).connection);
    } finally {
      await Promise.allSettled([sharedPublisher.close(), sharedConsumer.close()]);
    }
  });

  /**
   * Verifies shared connection ref-counting during close.
   * We need this because one sharer closing should not tear down the shared connection
   * while another instance is still using it.
   *
   * Steps:
   * 1. Create two matching shared instances.
   * 2. Register a consumer on one shared instance.
   * 3. Close the other shared instance.
   * 4. Publish a message through an independent publisher.
   * 5. Assert the remaining shared consumer still receives and processes the message.
   */
  it("keeps the remaining shared instance usable after another sharer closes", async () => {
    const sharedPublisher = new AmqpCacoon(createSharedConfig(queueName));
    const sharedConsumer = new AmqpCacoon(createSharedConfig(queueName));
    const receivedMessage = createDeferred();

    try {
      await (await sharedPublisher.getPublishChannel()).waitForConnect();
      await (await sharedConsumer.getConsumerChannel()).waitForConnect();

      await sharedConsumer.registerConsumer(
        queueName,
        async (channel, msg: ConsumeMessage) => {
          channel.ack(msg);
          receivedMessage.resolve();
        },
        { noAck: false, prefetch: 1 },
      );

      await sharedPublisher.close();
      await publisher.publish(
        "",
        queueName,
        Buffer.from("message-after-other-sharer-closed"),
      );
      await receivedMessage.promise;
    } finally {
      await Promise.allSettled([sharedPublisher.close(), sharedConsumer.close()]);
    }
  });

  /**
   * Verifies `gracefullShutdownAll()` interacts correctly with shared connections.
   * We need this because shared ownership adds ref-counting and a global shutdown must
   * still wait for in-flight work before releasing the final shared connection.
   *
   * Steps:
   * 1. Create shared publisher and consumer instances.
   * 2. Register a blocking consumer handler on the shared consumer.
   * 3. Publish one message and wait until the handler has started.
   * 4. Trigger `gracefullShutdownAll()`.
   * 5. Assert global shutdown does not resolve while the shared handler is still blocked.
   * 6. Release the handler and assert shutdown completes and shared state is cleaned up.
   */
  it("gracefullShutdownAll waits for in-flight shared consumers before closing the shared connection", async () => {
    const sharedConsumer = new AmqpCacoon(createSharedConfig(queueName));
    const sharedPublisher = new AmqpCacoon(createSharedConfig(queueName));
    const handlerStarted = createDeferred();
    const allowHandlerToFinish = createDeferred();
    const handlerFinished = createDeferred();
    let shutdownResolved = false;

    try {
      await (await sharedConsumer.getConsumerChannel()).waitForConnect();
      await (await sharedPublisher.getPublishChannel()).waitForConnect();

      await sharedConsumer.registerConsumer(
        queueName,
        async (channel, msg: ConsumeMessage) => {
          handlerStarted.resolve();
          await allowHandlerToFinish.promise;
          channel.ack(msg);
          handlerFinished.resolve();
        },
        { noAck: false, prefetch: 1 },
      );

      await publisher.publish("", queueName, Buffer.from("shared-shutdown-message"));
      await handlerStarted.promise;

      const shutdownPromise = AmqpCacoon.gracefullShutdownAll({
        softwareBlockCanceledConsumers: false,
        prePublishCallback: async () => Promise.resolve(),
        preCloseCallback: async () => Promise.resolve(),
      }).then(() => {
        shutdownResolved = true;
      });

      await delay(SHUTDOWN_WAIT_ASSERTION_MS);
      expect(
        shutdownResolved,
        "gracefullShutdownAll resolved before the shared in-flight handler finished",
      ).to.equal(false);

      allowHandlerToFinish.resolve();
      await handlerFinished.promise;
      await shutdownPromise;

      expect(
        shutdownResolved,
        "gracefullShutdownAll should resolve after the shared in-flight handler finishes",
      ).to.equal(true);
      expect(
        (AmqpCacoon as any).sharedConnections,
        "shared connection registry should be empty after shared shutdown completes",
      ).to.deep.equal({});
    } finally {
      await Promise.allSettled([sharedPublisher.close(), sharedConsumer.close()]);
    }
  });

  /**
   * Verifies that `prePublishCallback` can publish cleanup messages while
   * `gracefullShutdownAll()` is shutting down multiple live connections.
   * We need this because one intended shutdown pattern is to cancel consumers,
   * publish final cleanup messages, and only then close publishers/connections.
   *
   * Steps:
   * 1. Create multiple live AMQP instances and ensure their publisher channels are connected.
   * 2. Start a blocking consumer so shutdown has real in-flight work to wait on.
   * 3. Create a separate cleanup queue that is distinct from the consumed queue.
   * 4. Trigger `gracefullShutdownAll()` with a `prePublishCallback` that publishes cleanup messages.
   * 5. Assert shutdown does not finish until the in-flight handler is released.
   * 6. After shutdown completes, inspect the cleanup queue and verify the cleanup publishes were delivered.
   */
  it("gracefullShutdownAll allows prePublishCallback to publish cleanup messages to a separate queue across multiple live connections", async () => {
    const additionalPublisher = new AmqpCacoon(createConfig(queueName));
    const handlerStarted = createDeferred();
    const allowHandlerToFinish = createDeferred();
    const handlerFinished = createDeferred();
    let shutdownResolved = false;

    try {
      await (await publisher.getPublishChannel()).waitForConnect();
      await (await additionalPublisher.getPublishChannel()).waitForConnect();
      await (await publisher.getPublishChannel()).assertQueue(cleanupQueueName, {
        durable: false,
        autoDelete: false,
      });

      await consumer.registerConsumer(
        queueName,
        async (channel, msg: ConsumeMessage) => {
          handlerStarted.resolve();
          await allowHandlerToFinish.promise;
          channel.ack(msg);
          handlerFinished.resolve();
        },
        { noAck: false, prefetch: 1 },
      );

      await publisher.publish("", queueName, Buffer.from("message-before-cleanup-publish"));
      await handlerStarted.promise;

      const shutdownPromise = AmqpCacoon.gracefullShutdownAll({
        softwareBlockCanceledConsumers: true,
        consumerTimeoutWaitingForMessageProcessingMs: 60 * 1000,
        prePublishCallback: async () => {
          await publisher.publish(
            "",
            cleanupQueueName,
            Buffer.from("cleanup-message-1"),
          );
          await additionalPublisher.publish(
            "",
            cleanupQueueName,
            Buffer.from("cleanup-message-2"),
          );
        },
        preCloseCallback: async () => Promise.resolve(),
      }).then(() => {
        shutdownResolved = true;
      });

      await delay(SHUTDOWN_WAIT_ASSERTION_MS);
      expect(
        shutdownResolved,
        "gracefullShutdownAll resolved before the in-flight handler finished",
      ).to.equal(false);

      allowHandlerToFinish.resolve();
      await handlerFinished.promise;
      await shutdownPromise;

      const verifier = new AmqpCacoon(createConfig(queueName));
      try {
        const verifierChannel = await verifier.getPublishChannel();
        await verifierChannel.waitForConnect();
        const queueState = await verifierChannel.checkQueue(cleanupQueueName);

        expect(
          queueState.messageCount,
          "expected both cleanup messages to remain on the cleanup queue after shutdown",
        ).to.equal(2);
      } finally {
        await verifier.close();
      }
    } finally {
      await additionalPublisher.close();
    }
  });

  /**
   * Verifies that `prePublishCallback` can publish from a publisher instance whose
   * publish channel was not opened before shutdown started.
   * We need this because callers should not have to warm a publisher channel during
   * normal runtime just to make a shutdown cleanup publish reliable.
   *
   * Steps:
   * 1. Create a separate cleanup queue.
   * 2. Create a cold publisher instance and do not call `getPublishChannel()` on it.
   * 3. Start a blocking consumer so `gracefullShutdownAll()` has real work to coordinate.
   * 4. Trigger `gracefullShutdownAll()` with a `prePublishCallback` that publishes from the cold publisher.
   * 5. Assert shutdown waits for the in-flight consumer.
   * 6. After shutdown completes, inspect the cleanup queue and verify the cold publisher delivered its cleanup message.
   */
  it("gracefullShutdownAll allows prePublishCallback to publish from a cold publisher", async () => {
    const coldPublisher = new AmqpCacoon(createConfig(queueName));
    const handlerStarted = createDeferred();
    const allowHandlerToFinish = createDeferred();
    const handlerFinished = createDeferred();
    let shutdownResolved = false;

    try {
      const publisherChannel = await publisher.getPublishChannel();
      await publisherChannel.waitForConnect();
      await publisherChannel.assertQueue(cleanupQueueName, {
        durable: false,
        autoDelete: false,
      });

      await consumer.registerConsumer(
        queueName,
        async (channel, msg: ConsumeMessage) => {
          handlerStarted.resolve();
          await allowHandlerToFinish.promise;
          channel.ack(msg);
          handlerFinished.resolve();
        },
        { noAck: false, prefetch: 1 },
      );

      await publisher.publish("", queueName, Buffer.from("message-before-cold-cleanup-publish"));
      await handlerStarted.promise;

      const shutdownPromise = AmqpCacoon.gracefullShutdownAll({
        softwareBlockCanceledConsumers: true,
        consumerTimeoutWaitingForMessageProcessingMs: 60 * 1000,
        prePublishCallback: async () => {
          await coldPublisher.publish(
            "",
            cleanupQueueName,
            Buffer.from("cleanup-message-from-cold-publisher"),
          );
        },
        preCloseCallback: async () => Promise.resolve(),
      }).then(() => {
        shutdownResolved = true;
      });

      await delay(SHUTDOWN_WAIT_ASSERTION_MS);
      expect(
        shutdownResolved,
        "gracefullShutdownAll resolved before the in-flight handler finished",
      ).to.equal(false);

      allowHandlerToFinish.resolve();
      await handlerFinished.promise;
      await shutdownPromise;

      const verifier = new AmqpCacoon(createConfig(queueName));
      try {
        const verifierChannel = await verifier.getPublishChannel();
        await verifierChannel.waitForConnect();
        const queueState = await verifierChannel.checkQueue(cleanupQueueName);

        expect(
          queueState.messageCount,
          "expected the cold publisher cleanup message to be present on the cleanup queue after shutdown",
        ).to.equal(1);
      } finally {
        await verifier.close();
      }
    } finally {
      await coldPublisher.close();
    }
  });

  /**
   * Verifies shutdown can suppress `onChannelConnect` while still allowing a cold
   * cleanup publisher to publish during `prePublishCallback`.
   * We need this because some services want shutdown cleanup publishes to proceed
   * without running topology setup callbacks again during the shutdown window.
   *
   * Steps:
   * 1. Create a queue pair where the cleanup publisher has an `onChannelConnect` callback that would increment a counter.
   * 2. Do not open that cleanup publisher before shutdown so it stays cold.
   * 3. Start a blocking consumer to force real shutdown coordination.
   * 4. Trigger `gracefullShutdownAll()` with `disableOnChannelConnectDuringShutdown: true`.
   * 5. Publish from the cold cleanup publisher inside `prePublishCallback`.
   * 6. Verify the cleanup message is delivered and the cleanup publisher's `onChannelConnect` callback was never called.
   */
  it("gracefullShutdownAll can disable onChannelConnect during shutdown for a cold cleanup publisher", async () => {
    const flaggedQueueName = createQueueName();
    const flaggedCleanupQueueName = createCleanupQueueName();
    const handlerStarted = createDeferred();
    const allowHandlerToFinish = createDeferred();
    const handlerFinished = createDeferred();
    let shutdownResolved = false;
    let cleanupOnChannelConnectCallCount = 0;

    const flaggedConsumer = new AmqpCacoon(createConfig(flaggedQueueName));
    const flaggedPublisher = new AmqpCacoon(createConfig(flaggedQueueName));
    const flaggedColdCleanupPublisher = new AmqpCacoon({
      ...createConfig(flaggedQueueName),
      onChannelConnect: async () => {
        cleanupOnChannelConnectCallCount++;
        return Promise.resolve();
      },
    });

    try {
      const flaggedPublisherChannel = await flaggedPublisher.getPublishChannel();
      await flaggedPublisherChannel.waitForConnect();
      await flaggedPublisherChannel.assertQueue(flaggedQueueName, {
        durable: false,
        autoDelete: false,
      });
      await flaggedPublisherChannel.assertQueue(flaggedCleanupQueueName, {
        durable: false,
        autoDelete: false,
      });

      await flaggedConsumer.registerConsumer(
        flaggedQueueName,
        async (channel, msg: ConsumeMessage) => {
          handlerStarted.resolve();
          await allowHandlerToFinish.promise;
          channel.ack(msg);
          handlerFinished.resolve();
        },
        { noAck: false, prefetch: 1 },
      );

      await flaggedPublisher.publish(
        "",
        flaggedQueueName,
        Buffer.from("message-before-flagged-cold-cleanup-publish"),
      );
      await handlerStarted.promise;

      const shutdownPromise = AmqpCacoon.gracefullShutdownAll({
        softwareBlockCanceledConsumers: true,
        consumerTimeoutWaitingForMessageProcessingMs: 60 * 1000,
        disableOnChannelConnectDuringShutdown: true,
        prePublishCallback: async () => {
          await flaggedColdCleanupPublisher.publish(
            "",
            flaggedCleanupQueueName,
            Buffer.from("cleanup-message-with-disabled-onChannelConnect"),
          );
        },
        preCloseCallback: async () => Promise.resolve(),
      }).then(() => {
        shutdownResolved = true;
      });

      await delay(SHUTDOWN_WAIT_ASSERTION_MS);
      expect(
        shutdownResolved,
        "gracefullShutdownAll resolved before the in-flight handler finished",
      ).to.equal(false);

      allowHandlerToFinish.resolve();
      await handlerFinished.promise;
      await shutdownPromise;

      const verifier = new AmqpCacoon(createConfig(flaggedQueueName));
      try {
        const verifierChannel = await verifier.getPublishChannel();
        await verifierChannel.waitForConnect();
        const queueState = await verifierChannel.checkQueue(flaggedCleanupQueueName);

        expect(
          queueState.messageCount,
          "expected the cleanup message to be present when onChannelConnect is disabled during shutdown",
        ).to.equal(1);
        expect(
          cleanupOnChannelConnectCallCount,
          "cleanup publisher onChannelConnect should be skipped during shutdown when the flag is enabled",
        ).to.equal(0);
      } finally {
        await verifier.close();
      }
    } finally {
      const cleanupPublisherChannel = await flaggedPublisher.getPublishChannel();
      await cleanupPublisherChannel.waitForConnect();
      await cleanupPublisherChannel.deleteQueue(flaggedQueueName);
      await cleanupPublisherChannel.deleteQueue(flaggedCleanupQueueName);

      await Promise.allSettled([
        flaggedConsumer.close(),
        flaggedPublisher.close(),
        flaggedColdCleanupPublisher.close(),
      ]);
      globalThis.amqpCacoonConnections = [];
      (AmqpCacoon as any).sharedConnections = {};
    }
  });

  /**
   * Verifies shutdown behavior with a large number of non-shared instances.
   * We need this because small e2e cases can pass while large parallel shutdown fan-out
   * still exposes timing or coordination problems in real services.
   *
   * Steps:
   * 1. Create enough additional non-shared instances to bring the total to 100.
   * 2. Warm publisher channels on those instances so shutdown has many live connections to close.
   * 3. Create a separate cold publisher and do not open its publish channel before shutdown.
   * 4. Start one blocking consumer so shutdown still has real in-flight work to coordinate.
   * 5. Trigger `gracefullShutdownAll()` with a cleanup publish to a separate queue from the cold publisher.
   * 6. Assert shutdown does not resolve before the in-flight handler finishes.
   * 7. Release the handler and verify shutdown completes and the cleanup message was delivered.
   */
  it("gracefullShutdownAll completes with 100 non-shared instances and a cold cleanup publisher", async function () {
    this.timeout(120000);

    const totalInstances = 100;
    const extraInstanceCount = totalInstances - 2; // account for the default consumer + publisher created in beforeEach
    const extraInstances: AmqpCacoon[] = [];
    const coldPublisher = new AmqpCacoon(createConfig(queueName));
    const handlerStarted = createDeferred();
    const allowHandlerToFinish = createDeferred();
    const handlerFinished = createDeferred();
    let shutdownResolved = false;

    try {
      const publisherChannel = await publisher.getPublishChannel();
      await publisherChannel.waitForConnect();
      await publisherChannel.assertQueue(cleanupQueueName, {
        durable: false,
        autoDelete: false,
      });

      for (let i = 0; i < extraInstanceCount; i++) {
        extraInstances.push(new AmqpCacoon(createConfig(queueName)));
      }

      await Promise.all(
        extraInstances.map(async (instance) => {
          const channel = await instance.getPublishChannel();
          await channel.waitForConnect();
        }),
      );

      await consumer.registerConsumer(
        queueName,
        async (channel, msg: ConsumeMessage) => {
          handlerStarted.resolve();
          await allowHandlerToFinish.promise;
          channel.ack(msg);
          handlerFinished.resolve();
        },
        { noAck: false, prefetch: 1 },
      );

      await publisher.publish("", queueName, Buffer.from("message-before-100-instance-shutdown"));
      await handlerStarted.promise;

      const shutdownPromise = AmqpCacoon.gracefullShutdownAll({
        softwareBlockCanceledConsumers: true,
        consumerTimeoutWaitingForMessageProcessingMs: 60 * 1000,
        prePublishCallback: async () => {
          await coldPublisher.publish(
            "",
            cleanupQueueName,
            Buffer.from("cleanup-message-100-instance-shutdown"),
          );
        },
        preCloseCallback: async () => Promise.resolve(),
      }).then(() => {
        shutdownResolved = true;
      });

      await delay(SHUTDOWN_WAIT_ASSERTION_MS);
      expect(
        shutdownResolved,
        "gracefullShutdownAll resolved before the in-flight handler finished under 100-instance load",
      ).to.equal(false);

      allowHandlerToFinish.resolve();
      await handlerFinished.promise;
      await shutdownPromise;

      const verifier = new AmqpCacoon(createConfig(queueName));
      try {
        const verifierChannel = await verifier.getPublishChannel();
        await verifierChannel.waitForConnect();
        const queueState = await verifierChannel.checkQueue(cleanupQueueName);

        expect(
          queueState.messageCount,
          "expected the cleanup message to be present on the cleanup queue after 100-instance shutdown",
        ).to.equal(1);
      } finally {
        await verifier.close();
      }
    } finally {
      await Promise.allSettled([
        coldPublisher.close(),
        ...extraInstances.map((instance) => instance.close()),
      ]);
    }
  });
});
