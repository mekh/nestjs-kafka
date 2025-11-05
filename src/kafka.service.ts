import { Inject, Injectable, Logger } from '@nestjs/common';
import {
  EachMessagePayload,
  IHeaders,
  Kafka,
  Message,
  Producer,
  RecordMetadata,
} from 'kafkajs';
import { KafkaAdminService } from './kafka-admin.service';

import { KafkaRegistryService } from './kafka-registry.service';
import { KAFKA_CONFIG_TOKEN } from './kafka.constants';
import { KafkaConsumer } from './kafka.consumer';
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
  public readonly kafka: Kafka;

  protected readonly logger = new Logger(KafkaService.name);

  protected readonly producer: Producer;

  protected consumers: KafkaConsumer[] = [];

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

    for (const consumer of this.registry.getConsumers()) {
      consumer.createConsumer(this.kafka);

      await consumer.connect();
      await consumer.subscribe();
      await this.consume(consumer);

      this.consumers.push(consumer);
    }

    this.logger.log('Kafka - initialization completed');
  }

  public async disconnect(): Promise<void> {
    await this.producer.disconnect();
    await Promise.all(
      this.consumers.map((consumer) => this.disconnectConsumer(consumer)),
    );

    this.consumers = [];
  }

  public async send(data: KafkaSendInput): Promise<RecordMetadata[]> {
    await this.ensureTopics(data.topic);

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

  protected createMessage(data: KafkaSendInputMessage): Message {
    return {
      ...data,
      value: JSON.stringify(data.value),
    };
  }

  protected async disconnectConsumer(consumer: KafkaConsumer): Promise<void> {
    const groupId = consumer.groupId;

    await consumer
      .disconnect()
      .then(() => this.logger.log('Kafka - consumer disconnected: %s', groupId))
      .catch((err) =>
        this.onError(err, `Kafka - error disconnecting consumer: ${groupId}`)
      );
  }

  protected async consume(consumer: KafkaConsumer): Promise<void> {
    const autoCommit = consumer.autoCommit;

    await consumer.run({
      autoCommit,
      eachMessage: (payload: EachMessagePayload) =>
        this.handleMessage(payload, consumer).catch(
          (err) =>
            this.onError(
              err,
              `Kafka - error handling message in consumer ${consumer.groupId}`,
            ),
        ),
    });
  }

  protected async handleMessage(
    payload: EachMessagePayload,
    consumer: KafkaConsumer,
  ): Promise<void> {
    const value = this.parseMessage(payload.message.value);
    const headers = this.parseHeaders(payload.message.headers);
    const key = payload.message.key?.toString();
    const ack = (): Promise<void> => this.commitOffset(payload, consumer);

    this.logger.debug(
      'Kafka - received message: %o',
      this.formatLogMessage(payload),
    );

    await this.handle({
      ...payload,
      ack,
      message: { ...payload.message, key, headers, value },
    });
  }

  protected async commitOffset(
    payload: EachMessagePayload,
    consumer: KafkaConsumer,
  ): Promise<void> {
    if (consumer.autoCommit) {
      return;
    }

    await consumer.commitOffset({
      topic: payload.topic,
      partition: payload.partition,
      offset: (Number(payload.message.offset) + 1).toString(),
    });
  }

  protected parseMessage(
    value: Buffer | null,
  ): Record<string, any> | undefined {
    const str = value?.toString();
    if (!str) {
      return;
    }

    let parsed: Record<string, any> | undefined;
    try {
      parsed = JSON.parse(str);
    } catch {
      //
    }

    return parsed;
  }

  protected parseHeaders(
    headers?: IHeaders,
  ): Record<string, string | undefined> | undefined {
    if (!headers) {
      return;
    }

    return Object.fromEntries(
      Object.entries(headers).map((
        [key, value],
      ) => [key, value?.toString()]),
    );
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
