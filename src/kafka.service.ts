import { Inject, Injectable, Logger } from '@nestjs/common';
import {
  Consumer,
  EachMessagePayload,
  Kafka,
  Message,
  Producer,
  RecordMetadata,
} from 'kafkajs';
import { KafkaAdminService } from './kafka-admin.service';

import { KafkaRegistryService } from './kafka-registry.service';
import { KAFKA_CONFIG_TOKEN } from './kafka.constants';
import {
  KafkaConfig,
  KafkaConsumerPayload,
  KafkaSendInput,
  KafkaSendInputMessage,
} from './kafka.interfaces';

interface KafkaLogMessage {
  topic: string;
  partition: number;
  offset: string;
  key: string | undefined;
  timestamp: string;
  message?: string;
}

@Injectable()
export class KafkaService {
  protected readonly logger = new Logger(KafkaService.name);

  public readonly kafka: Kafka;

  protected readonly producer: Producer;

  protected consumers: { consumer: Consumer; groupId: string }[] = [];

  constructor(
    @Inject(KAFKA_CONFIG_TOKEN) config: KafkaConfig,
    public readonly registry: KafkaRegistryService,
    public readonly admin: KafkaAdminService,
  ) {
    this.kafka = new Kafka(config);
    this.producer = this.kafka.producer();
  }

  public async connect(): Promise<void> {
    await this.producer.connect();
    await this.admin.connect();

    this.logger.log('Kafka - producer connected');

    const topics = this.registry.getTopics();
    await this.admin.ensureTopics(topics);

    for (
      const {
        topics,
        config,
        fromBeginning,
        autoCommit,
      } of this.registry.getConsumers()
    ) {
      const consumer = this.kafka.consumer(config);
      this.consumers.push({ consumer, groupId: config.groupId });

      await this.connectConsumer(consumer, config.groupId);
      await this.subscribe(consumer, config.groupId, topics, fromBeginning);
      await this.consume(consumer, config.groupId, autoCommit);
    }
  }

  public async disconnect(): Promise<void> {
    await this.producer.disconnect();
    await Promise.all(
      this.consumers.map((consumer) =>
        this.disconnectConsumer(consumer.consumer, consumer.groupId)
      ),
    );

    this.consumers = [];
  }

  public async send(data: KafkaSendInput): Promise<RecordMetadata[]> {
    await this.admin.ensureTopics(data.topic);

    const messages: Message[] = Array.isArray(data.messages)
      ? data.messages.map((msg: KafkaSendInputMessage) =>
        this.createMessage(msg)
      )
      : [this.createMessage(data.messages)];

    return this.producer.send({ topic: data.topic, messages });
  }

  public async ensureTopics(topic: string | string[]): Promise<void> {
    await this.admin.ensureTopics(topic);
  }

  protected async subscribe(
    consumer: Consumer,
    groupId: string,
    topics: string[],
    fromBeginning?: boolean,
  ): Promise<void> {
    await consumer.subscribe({ topics, fromBeginning });

    this.logger.log(
      'Kafka - consumer %s subscribed to topics: %s',
      groupId,
      topics.join(','),
    );
  }

  protected createMessage(data: KafkaSendInputMessage): Message {
    return {
      ...data,
      value: JSON.stringify(data.value),
    };
  }

  protected async connectConsumer(
    consumer: Consumer,
    groupId: string,
  ): Promise<void> {
    await consumer.connect();

    this.logger.log('Kafka - consumer connected: %s', groupId);
  }

  protected async disconnectConsumer(
    consumer: Consumer,
    groupId: string,
  ): Promise<void> {
    await consumer
      .disconnect()
      .then(() => this.logger.log('Kafka - consumer disconnected: %s', groupId))
      .catch((err) =>
        this.onError(err, `Kafka - error disconnecting consumer: ${groupId}`)
      );
  }

  protected async consume(
    consumer: Consumer,
    groupId: string,
    autoCommit: boolean,
  ): Promise<void> {
    await consumer.run({
      autoCommit,
      eachMessage: (payload: EachMessagePayload) =>
        this.handleMessage(payload, !autoCommit ? consumer : undefined).catch(
          (err) =>
            this.onError(
              err,
              `Kafka - error handling message in consumer ${groupId}`,
            ),
        ),
    });

    this.logger.log('Kafka - kafka consumer started %s', groupId);
  }

  protected async handleMessage(
    payload: EachMessagePayload,
    consumer?: Consumer,
  ): Promise<void> {
    const messageValue = this.parseMessage(payload);
    const ack = (): Promise<void> => this.commitOffset(consumer, payload);

    if (!messageValue) {
      return ack();
    }

    this.logger.debug(
      'Kafka - received message: %o',
      this.formatLogMessage(payload),
    );

    await this.handle({
      ...payload,
      ack,
      message: { ...payload.message, value: messageValue },
    });
  }

  protected async commitOffset(
    consumer: Consumer | undefined,
    payload: EachMessagePayload,
  ): Promise<void> {
    if (!consumer) {
      return;
    }

    await consumer.commitOffsets([
      {
        topic: payload.topic,
        partition: payload.partition,
        offset: (Number(payload.message.offset) + 1).toString(),
      },
    ]);

    this.logger.debug(
      'Kafka - committed partition: %s, offset: %s, topic: %s, ',
      payload.partition,
      payload.message.offset,
      payload.topic,
    );
  }

  protected parseMessage(
    payload: EachMessagePayload,
  ): Record<string, any> | undefined {
    const messageValue = payload.message.value?.toString();
    if (!messageValue) {
      this.logger.warn(
        'Kafka - received invalid/empty message: %o',
        this.formatLogMessage(payload),
      );

      return;
    }

    let parsedMessage: Record<string, any> | undefined;
    try {
      parsedMessage = JSON.parse(messageValue);
    } catch {
      //
    }

    return parsedMessage;
  }

  protected async handle(payload: KafkaConsumerPayload): Promise<void> {
    const handlers = this.registry.getHandlers(payload.topic);
    if (!handlers?.length) {
      return;
    }

    await Promise.all(
      handlers.map((handler) =>
        handler
          .handle(payload)
          .catch((err) => this.onError(err, 'Kafka - error handling message'))
      ),
    );
  }

  protected onError(err: any, message?: string): void {
    if (message) {
      this.logger.error(message);
    }

    this.logger.error(err);
  }

  protected formatLogMessage(payload: EachMessagePayload): KafkaLogMessage {
    const { topic, partition, message } = payload;

    return {
      topic,
      partition,
      offset: message.offset,
      key: message.key?.toString(),
      message: message.value?.toString(),
      timestamp: message.timestamp,
    };
  }
}
