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
  let consumer: AmqpCacoon;
  let publisher: AmqpCacoon;

  before(async () => {
    startRabbitMq();
    await waitForRabbitMq(createQueueName());
  });

  beforeEach(async () => {
    queueName = createQueueName();
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
    } catch (error) {
      // Ignore cleanup failures when the connection has already been closed.
    }

    await Promise.allSettled([consumer?.close(), publisher?.close()]);
  });

  after(async () => {
    removeRabbitMq();
  });

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
});
