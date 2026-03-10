import amqp, {
  ChannelWrapper,
  AmqpConnectionManager,
  AmqpConnectionManagerOptions,
} from "amqp-connection-manager";
import {
  ConsumeMessage,
  Channel,
  ConfirmChannel,
  Options,
  Connection,
} from "amqplib";
import { ILogger } from "./types";
import MessageBatchingManager from "./helpers/message_batching_manager";

type ConnectCallback = (channel: ConfirmChannel) => Promise<any>;
type BrokerConnectCallback = (connection: Connection, url: string) => void;
type BrokerDisconnectCallback = (err: Error) => void;

export {
  ConsumeMessage,
  ChannelWrapper,
  Channel,
  ConfirmChannel,
  ConnectCallback,
  BrokerConnectCallback,
  BrokerDisconnectCallback,
  AmqpConnectionManagerOptions,
};

const DEFAULT_MAX_FILES_SIZE_BYTES = 1024 * 1024 * 2; // 2 MB
const DEFAULT_MAX_BUFFER_TIME_MS = 60 * 1000; // 60 seconds

// Used as return value in handler for registerConsumerBatch function
export interface ConsumeBatchMessages {
  batchingOptions: {
    maxSizeBytes?: number;
    maxTimeMs?: number;
  };
  totalSizeInBytes: number;
  messages: Array<ConsumeMessage>;
  ackAll: (allUpTo?: boolean) => void;
  nackAll: (allUpTo?: boolean, requeue?: boolean) => void;
}

// Used for registerConsumer function
export interface ConsumerOptions extends Options.Consume {
  prefetch?: number;
}

// Used for registerConsumerBatch function
export interface ConsumerBatchOptions extends Options.Consume {
  batching?: {
    maxSizeBytes?: number;
    maxTimeMs?: number;
  };
}

export interface IAmqpCacoonConfig {
  protocol?: string;
  username?: string;
  password?: string;
  host?: string;
  port?: number;
  vhost?: string;
  connectionString?: string;
  amqp_opts: object;
  providers: {
    logger?: ILogger;
  };
  shareConnection?: boolean;
  onChannelConnect?: ConnectCallback;
  onBrokerConnect?: BrokerConnectCallback;
  onBrokerDisconnect?: BrokerDisconnectCallback;
  // maxWaitForDrainMs?: number; // How long to wait for a drain event if RabbitMq fills up. Zero to wait forever. Defaults to 60000 ms (1 min)
}

globalThis.amqpCacoonConnections = [];

/**
 * AmqpCacoon
 * This module is used to communicate using the RabbitMQ amqp protocol
 * Usage
 * 1. Instantiate and pass in the configuration using the IAmqpCacoonConfig interface
 * 2. Call publish() function to publish a message
 * 3. To consume messages call registerConsumer() passing in a handler function
 * 4. When consuming the callback registered in the previous step will be called when a message is received
 **/
class AmqpCacoon {
  private static sharedConnections: {
    [key: string]: {
      connection: AmqpConnectionManager;
      refCount: number;
    };
  } = {};
  private pubChannelWrapper: ChannelWrapper | null;
  private subChannelWrapper: ChannelWrapper | null;
  private connection?: AmqpConnectionManager;
  private fullHostName: string;
  private amqp_opts: object;
  private shareConnection: boolean;
  private sharedConnectionKey: string | null;
  private logger?: ILogger;
  // private maxWaitForDrainMs: number;
  private onChannelConnect: ConnectCallback | null;
  private onBrokerConnect: Function | null;
  private onBrokerDisconnect: Function | null;
  private disableOnChannelConnectDuringShutdown: boolean = false;
  private hasInjectedConnectionEvents: boolean = false;
  private readonly brokerConnectHandler: (
    connection: Connection,
    url: string,
  ) => void;
  private readonly brokerDisconnectHandler: (err: Error) => void;
  private isShuttingDownPublisher: boolean = false;
  private isShuttingDownConsumer: boolean = false;
  private hasCanceledConsumer: boolean = false;
  private shouldSoftwareBlockCanceledConsumber: boolean = false;
  private messageStatistics = {
    messagesPublished: 0,
    messagesReceived: 0,
    messagesProcessed: 0,
    messagesBeingProcessed: 0,
  };

  /**
   * constructor
   *
   * Usage
   * This function just sets some class level variables
   *
   * @param config - Contains the amqp connection config and the Logger provder
   **/
  constructor(config: IAmqpCacoonConfig) {
    this.pubChannelWrapper = null;
    this.subChannelWrapper = null;
    this.fullHostName =
      config.connectionString || AmqpCacoon.getFullHostName(config);
    this.amqp_opts = config.amqp_opts;
    this.shareConnection = config.shareConnection || false;
    this.sharedConnectionKey = this.shareConnection ? this.fullHostName : null;
    this.logger = config.providers.logger;
    // this.maxWaitForDrainMs = config.maxWaitForDrainMs || 60000; // Default to 1 min
    this.onChannelConnect = config.onChannelConnect || null;
    this.onBrokerConnect = config.onBrokerConnect || null;
    this.onBrokerDisconnect = config.onBrokerDisconnect || null;
    this.brokerConnectHandler = this.handleBrokerConnect.bind(this);
    this.brokerDisconnectHandler = this.handleBrokerDisconnect.bind(this);

    // Add this instance to the global list of connections
    globalThis.amqpCacoonConnections.push(this);
  }

  /**
   * getFullHostName
   * Just generates the full connection name for amqp based on the passed in config
   * @param config - Contains the amqp connection config
   **/
  private static getFullHostName(config: IAmqpCacoonConfig) {
    var fullHostNameString =
      config.protocol +
      "://" +
      config.username +
      ":" +
      config.password +
      "@" +
      config.host +
      (config.port ? `:${config.port}` : "") +
      (config.vhost ? `/${config.vhost}` : "");

    return fullHostNameString;
  }

  private injectConnectionEvents(connection: AmqpConnectionManager) {
    if (this.hasInjectedConnectionEvents) return;
    // Subscribe to onConnect / onDisconnection functions for debugging
    connection.on("connect", this.brokerConnectHandler);
    connection.on("disconnect", this.brokerDisconnectHandler);
    this.hasInjectedConnectionEvents = true;
  }

  private shouldSkipOnChannelConnectDuringShutdown() {
    return (
      this.disableOnChannelConnectDuringShutdown &&
      (this.isShuttingDownConsumer || this.isShuttingDownPublisher)
    );
  }

  private runOnChannelConnect(channel: ConfirmChannel) {
    if (this.shouldSkipOnChannelConnectDuringShutdown()) {
      this.logger?.info(
        "AMQPCacoon.onChannelConnect: Skipping callback during shutdown",
      );
      return Promise.resolve();
    }

    if (this.onChannelConnect) {
      return this.onChannelConnect(channel);
    } else {
      return Promise.resolve();
    }
  }

  private removeConnectionEvents(connection: AmqpConnectionManager) {
    if (!this.hasInjectedConnectionEvents) return;
    connection.removeListener("connect", this.brokerConnectHandler);
    connection.removeListener("disconnect", this.brokerDisconnectHandler);
    this.hasInjectedConnectionEvents = false;
  }

  private getOrCreateConnection() {
    if (this.connection) {
      return this.connection;
    }

    if (this.shareConnection && this.sharedConnectionKey) {
      const sharedConnection =
        AmqpCacoon.sharedConnections[this.sharedConnectionKey];
      if (sharedConnection) {
        sharedConnection.refCount++;
        this.connection = sharedConnection.connection;
        return this.connection;
      }
    }

    const connection = amqp.connect([this.fullHostName], this.amqp_opts);
    this.connection = connection;

    if (this.shareConnection && this.sharedConnectionKey) {
      AmqpCacoon.sharedConnections[this.sharedConnectionKey] = {
        connection,
        refCount: 1,
      };
    }

    return connection;
  }

  private async releaseConnection() {
    const connection = this.connection;
    if (!connection) return;

    this.removeConnectionEvents(connection);

    if (this.shareConnection && this.sharedConnectionKey) {
      const sharedConnection =
        AmqpCacoon.sharedConnections[this.sharedConnectionKey];
      if (sharedConnection && sharedConnection.connection === connection) {
        sharedConnection.refCount--;
        this.logger?.info("AMQPCacoon.close: Released shared connection", {
          sharedConnectionKey: this.sharedConnectionKey,
          remainingReferences: sharedConnection.refCount,
        });
        if (sharedConnection.refCount <= 0) {
          delete AmqpCacoon.sharedConnections[this.sharedConnectionKey];
          this.connection = undefined;
          this.logger?.info("AMQPCacoon.close: Closing final shared connection", {
            sharedConnectionKey: this.sharedConnectionKey,
          });
          await connection.close();
          return;
        }
        this.connection = undefined;
        return;
      }
    }

    this.connection = undefined;
    this.logger?.info("AMQPCacoon.close: Closing dedicated connection");
    await connection.close();
  }

  /**
   * getPublishChannel
   * This connects to amqp and creates a channel or gets the current channel
   * @return ChannelWrapper
   **/
  async getPublishChannel() {
    try {
      // Return the pubChannel if we are already connected
      if (this.pubChannelWrapper) {
        return this.pubChannelWrapper;
      }
      // Connect if needed
      const connection = this.getOrCreateConnection();
      this.injectConnectionEvents(connection);

      // Open a channel (get reference to ChannelWrapper)
      // Add a setup function that will be called on each connection retry
      // This function is specified in the config
      this.pubChannelWrapper = connection.createChannel({
        setup: (channel: ConfirmChannel) => this.runOnChannelConnect(channel),
      });
    } catch (e) {
      if (this.logger) this.logger.error("AMQPCacoon.connect: Error: ", e);
      throw e;
    }
    // Return the channel
    return this.pubChannelWrapper;
  }

  /**
   * getConsumerChannel
   * This connects to amqp and creates a channel or gets the current channel
   * @return ChannelWrapper
   **/
  async getConsumerChannel() {
    try {
      // Return the subChannel if we are already connected
      if (this.subChannelWrapper) return this.subChannelWrapper;
      // Connect if needed
      const connection = this.getOrCreateConnection();
      this.injectConnectionEvents(connection);

      // Open a channel (get reference to ChannelWrapper)
      // Add a setup function that will be called on each connection retry
      // This function is specified in the config
      this.subChannelWrapper = connection.createChannel({
        setup: (channel: ConfirmChannel) => this.runOnChannelConnect(channel),
      });
    } catch (e) {
      if (this.logger)
        this.logger.error("AMQPCacoon.getConsumerChannel: Error: ", e);
      throw e;
    }
    // Return the channel
    return this.subChannelWrapper;
  }

  /**
   * registerConsumerPrivate
   * registerConsumer and registerConsumerBatch use this function to register consumers
   *
   * @param queue - Name of the queue
   * @param consumerHandler: (channel: ChannelWrapper, msg: object) => Promise<any> - A handler that receives the message
   * @param options : ConsumerOptions - Used to pass in consumer options
   * @return Promise<void>
   **/
  private async registerConsumerPrivate(
    queue: string,
    consumerHandler: (
      channel: ChannelWrapper,
      msg: ConsumeMessage | null,
    ) => Promise<void>,
    options?: ConsumerOptions,
  ) {
    try {
      // Get consumer channel
      const channelWrapper = await this.getConsumerChannel();
      await channelWrapper.consume(
        queue,
        consumerHandler.bind(this, channelWrapper),
        options,
      );
    } catch (e) {
      if (this.logger)
        this.logger.error("AMQPCacoon.registerConsumerPrivate: Error: ", e);
      throw e;
    }
  }

  /**
   * registerConsumer
   * After registering a handler on a queue that handler will
   * get called for messages received on the specified queue
   *
   * @param queue - Name of the queue
   * @param handler: (channel: ChannelWrapper, msg: object) => Promise<any> - A handler that receives the message
   * @param options : ConsumerOptions - Used to pass in consumer options
   * @return Promise<void>
   **/
  async registerConsumer(
    queue: string,
    handler: (channel: ChannelWrapper, msg: ConsumeMessage) => Promise<void>,
    options?: ConsumerOptions,
  ) {
    if (this.isShuttingDownConsumer) {
      throw new Error("AMQPCacoon.registerConsumerBatch: Shutting down");
    }
    return this.registerConsumerPrivate(
      queue,
      async (channel: ChannelWrapper, msg: ConsumeMessage | null) => {
        if (!msg) return; // We know this will always be true but typescript requires this
        if (
          this.hasCanceledConsumer &&
          this.shouldSoftwareBlockCanceledConsumber
        ) {
          this.logger?.info(
            "AMQPCacoon.registerConsumer: Consumer has been canceled but we received a message - ignoring message - May be requeued",
          );
          return;
        }
        try {
          this.messageStatistics.messagesReceived++;
          this.messageStatistics.messagesBeingProcessed++;
          await handler(channel, msg);
          this.messageStatistics.messagesProcessed++;
          this.messageStatistics.messagesBeingProcessed--;
        } catch (e) {
          this.messageStatistics.messagesProcessed++;
          this.messageStatistics.messagesBeingProcessed--;
          throw e;
        }
      },
      options,
    );
  }

  /**
   * registerConsumerBatch
   * This is very similar to registerConsumer except this enables message batching.
   * The following options are configurable
   * 1. batching.maxTimeMs - Max time in milliseconds before we return the batch
   * 2. batching.maxSizeBytes - Max size in bytes before we return
   *
   * @param queue - Name of the queue
   * @param handler: (channel: ChannelWrapper, msg: object) => Promise<any> - A handler that receives the message
   * @param options : ConsumerOptions - Used to pass in consumer options
   * @return Promise<void>
   **/
  async registerConsumerBatch(
    queue: string,
    handler: (
      channel: ChannelWrapper,
      msg: ConsumeBatchMessages,
    ) => Promise<void>,
    options?: ConsumerBatchOptions,
  ) {
    if (this.isShuttingDownConsumer) {
      throw new Error("AMQPCacoon.registerConsumerBatch: Shutting down");
    }
    // Set some default options
    if (!options?.batching) {
      options = Object.assign(
        {},
        {
          batching: {
            maxTimeMs: DEFAULT_MAX_BUFFER_TIME_MS,
            maxSizeBytes: DEFAULT_MAX_FILES_SIZE_BYTES,
          },
        },
        options,
      );
    }

    // Initialize Message batching manager
    let messageBatchingHandler: MessageBatchingManager;
    messageBatchingHandler = new MessageBatchingManager({
      providers: { logger: this.logger },
      maxSizeBytes: options?.batching?.maxSizeBytes,
      maxTimeMs: options?.batching?.maxTimeMs,
      skipNackOnFail: options?.noAck,
    });

    // Register consumer
    return this.registerConsumerPrivate(
      queue,
      async (channel: ChannelWrapper, msg: ConsumeMessage | null) => {
        if (!msg) return; // We know this will always be true but typescript requires this

        if (
          this.hasCanceledConsumer &&
          this.shouldSoftwareBlockCanceledConsumber
        ) {
          this.logger?.info(
            "AMQPCacoon.registerConsumer: Consumer has been canceled but we received a message - ignoring message - May be requeued",
          );
          return;
        }

        try {
          this.messageStatistics.messagesReceived++;
          this.messageStatistics.messagesBeingProcessed++;
          // Handle message batching
          messageBatchingHandler.handleMessageBuffering(channel, msg, handler);
          this.messageStatistics.messagesProcessed++;
          this.messageStatistics.messagesBeingProcessed--;
        } catch (e) {
          this.messageStatistics.messagesProcessed++;
          this.messageStatistics.messagesBeingProcessed--;
          throw e;
        }
      },
      options,
    );
  }

  /**
   * publish
   * publish to an exchange
   *
   * @param exchange - name of the echange
   * @param routingKey - queue or queue routingKey
   * @param msgBuffer - Message buffer
   * @param options - Options for publishing
   * @return Promise<void> - Promise resolves when done sending
   **/
  async publish(
    exchange: any,
    routingKey: any,
    msgBuffer: any,
    options?: Options.Publish,
  ) {
    if (this.isShuttingDownPublisher) {
      throw new Error("AMQPCacoon.publish: Shutting down");
    }
    try {
      // Actually returns a wrapper
      const channel = await this.getPublishChannel(); // Sets up the publisher channel

      this.messageStatistics.messagesPublished++;
      // There's currently a reported bug in node-amqp-connection-manager saying the lib does
      // not handle drain events properly... requires research.
      // See https://github.com/valtech-sd/amqp-cacoon/issues/20
      await channel.publish(exchange, routingKey, msgBuffer, options);
      return;
    } catch (e) {
      if (this.logger) this.logger.error("AMQPCacoon.publish: Error: ", e);
      throw e;
    }
  }

  async close() {
    this.logger?.info("AMQPCacoon.close: START");
    try {
      this.logger?.info("AMQPCacoon.close: Closing publish channel");
      await this.closePublishChannel();
      this.logger?.info("AMQPCacoon.close: Closing consumer channel");
      await this.closeConsumerChannel();
      await this.releaseConnection();
      this.logger?.info("AMQPCacoon.close: END");
    } catch (error) {
      this.logger?.error("AMQPCacoon.close: Error", error);
    }
  }

  async cancelPublisherChanel() {
    this.logger?.info("AMQPCacoon.cancelPublisherChanel: START");
    this.isShuttingDownPublisher = true;
    try {
      const publishChannel = this.pubChannelWrapper;
      if (!publishChannel) {
        this.logger?.info("AMQPCacoon.cancelPublisherChanel: No channel");
        this.logger?.info("AMQPCacoon.cancelPublisherChanel: END");
        return;
      }
      this.logger?.info(
        "AMQPCacoon.cancelPublisherChanel: Cancelling publishers",
      );
      await publishChannel.cancelAll(); // Shouldn't do anything but just in case someone was silly with use of publish channel
      this.logger?.info(
        "AMQPCacoon.cancelPublisherChanel: Cancelled publishers",
      );
      // Wait for all messages to be sent
      this.logger?.info(
        "AMQPCacoon.cancelPublisherChanel: Waiting for all messages to be sent",
      );
      let queueLength = publishChannel.queueLength();
      while (queueLength > 0) {
        await new Promise((resolve) => setTimeout(resolve, 500));
        queueLength = publishChannel.queueLength();
      }
      this.logger?.info("AMQPCacoon.cancelPublisherChanel: All messages sent");
    } catch (error) {
      this.logger?.error("AMQPCacoon.cancelPublisherChanel: Error", error);
    }

    this.logger?.info("AMQPCacoon.cancelPublisherChanel: END");
  }

  async cancelConsumerChanel(
    options: {
      timeoutWaitingForMessageProcessingMs?: number;
      softwareBlockCanceledConsumers?: boolean;
    } = {
      timeoutWaitingForMessageProcessingMs: 10000,
      softwareBlockCanceledConsumers: false,
    },
  ) {
    this.logger?.info("AMQPCacoon.cancelConsumerChanel: START");
    this.isShuttingDownConsumer = true;
    this.hasCanceledConsumer = true;
    try {
      const consumerChannel = this.subChannelWrapper;
      if (!consumerChannel) {
        this.logger?.info("AMQPCacoon.cancelConsumerChanel: No channel");
        this.logger?.info("AMQPCacoon.cancelConsumerChanel: END");
        return;
      }
      this.logger?.info(
        "AMQPCacoon.cancelConsumerChanel: Cancelling consumers",
        { messageStatistics: this.messageStatistics },
      );
      await consumerChannel.cancelAll();

      this.shouldSoftwareBlockCanceledConsumber =
        options.softwareBlockCanceledConsumers || false;
      // Wait for all messages to be processed
      const startWaitTimeMs = Date.now();
      while (
        this.messageStatistics.messagesBeingProcessed > 0 &&
        Date.now() <
          startWaitTimeMs +
            (options.timeoutWaitingForMessageProcessingMs || 10000)
      ) {
        this.logger?.info(
          "AMQPCacoon.cancelConsumerChanel: Waiting for messages to be processed",
          { messageStatistics: this.messageStatistics },
        );
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
      this.logger?.info(
        "AMQPCacoon.cancelConsumerChanel: Cancelled consumers",
        { messageStatistics: this.messageStatistics },
      );
    } catch (error) {
      this.logger?.error("AMQPCacoon.cancelConsumerChanel: Error", error);
    }
    this.logger?.info("AMQPCacoon.cancelConsumerChanel: END");
  }

  async gracefullShutdown(options: {
    prePublishCallback: () => Promise<any>;
    preCloseCallback: () => Promise<any>;
    disableOnChannelConnectDuringShutdown?: boolean;
  }) {
    this.disableOnChannelConnectDuringShutdown =
      options.disableOnChannelConnectDuringShutdown || false;
    await this.cancelConsumerChanel();

    if (options.prePublishCallback) await options.prePublishCallback();

    await this.cancelPublisherChanel();

    if (options.preCloseCallback) await options.preCloseCallback();

    await this.close();
  }

  static async gracefullShutdownAll(options: {
    softwareBlockCanceledConsumers: boolean;
    consumerTimeoutWaitingForMessageProcessingMs?: number;
    prePublishCallback: () => Promise<any>;
    preCloseCallback: () => Promise<any>;
    disableOnChannelConnectDuringShutdown?: boolean;
  }) {
    const consumerCloseProms = [];
    globalThis.amqpCacoonConnections?.[0]?.logger?.info(
      "AMQPCacoon.gracefullShutdownAll: START",
    );
    for (let conn of globalThis.amqpCacoonConnections) {
      conn.disableOnChannelConnectDuringShutdown =
        options.disableOnChannelConnectDuringShutdown || false;
      conn.logger?.info("AMQPCacoon.gracefullShutdownAll: Messages stats: ", {
        messageStatistics: conn.messageStatistics,
      });
      consumerCloseProms.push(
        conn.cancelConsumerChanel({
          softwareBlockCanceledConsumers:
            options.softwareBlockCanceledConsumers,
          timeoutWaitingForMessageProcessingMs:
            options.consumerTimeoutWaitingForMessageProcessingMs,
        }),
      );
    }
    await Promise.all(consumerCloseProms);

    // Cancel publishers
    if (options.prePublishCallback) await options.prePublishCallback();
    const publisherCloseProms = [];
    for (let conn of globalThis.amqpCacoonConnections) {
      publisherCloseProms.push(conn.cancelPublisherChanel());
    }
    await Promise.all(publisherCloseProms);

    // Close connctions
    if (options.preCloseCallback) await options.preCloseCallback();
    const closeProms = [];
    for (let conn of globalThis.amqpCacoonConnections) {
      closeProms.push(conn.close());
    }
    await Promise.all(closeProms);
    globalThis.amqpCacoonConnections?.[0]?.logger?.info(
      "AMQPCacoon.gracefullShutdownAll: END",
    );
  }
  /**
   * closeConsumerChannel
   * Close consume channel
   * @return Promise<void>
   **/
  async closeConsumerChannel() {
    if (!this.subChannelWrapper) return;
    await this.subChannelWrapper.close();
    this.subChannelWrapper = null;
    return;
  }

  /**
   * closePublishChannel
   * Close publish channel
   * @return Promise<void>
   **/
  async closePublishChannel() {
    if (!this.pubChannelWrapper) return;
    await this.pubChannelWrapper.close();
    this.pubChannelWrapper = null;
    return;
  }

  /**
   * handleBrokerConnect
   * Fires onBrokerConnect callback function whenever amqp connection is made
   *
   * @return void
   **/
  private handleBrokerConnect(connection: Connection, url: string) {
    if (this.onBrokerConnect) {
      this.onBrokerConnect(connection, url);
    }
  }

  /**
   * handleBrokerDisconnect
   * Fires onBrokerDisconnect callback function whenever amqp connection is lost
   *
   * @return void
   **/
  private handleBrokerDisconnect(err: Error) {
    if (this.onBrokerDisconnect) {
      this.onBrokerDisconnect(err);
    }
  }
}

export default AmqpCacoon;
