import { Inject, Injectable, Logger } from '@nestjs/common';
import {
  EachBatchPayload,
  EachMessagePayload,
  Kafka,
  Producer,
  RecordMetadata,
} from 'kafkajs';

import { KafkaAdminService } from './kafka-admin.service';
import { KafkaRegistryService } from './kafka-registry.service';
import { KafkaSerdeService } from './kafka-serde.service';
import { KAFKA_CONFIG_TOKEN } from './kafka.constants';
import { KafkaConsumer } from './kafka.consumer';
import {
  KafkaBatchPayload,
  KafkaConfig,
  KafkaConsumerPayload,
  KafkaSendInput,
} from './kafka.interfaces';

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
    public readonly serde: KafkaSerdeService,
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
    await Promise.allSettled(
      this.consumers.map((consumer) => consumer.disconnect()),
    );

    this.consumers = [];
  }

  public async send(data: KafkaSendInput): Promise<RecordMetadata[]> {
    await this.ensureTopics(data.topic);

    return this.producer.send({
      topic: data.topic,
      messages: this.serde.serialize(data),
    });
  }

  public async ensureTopics(topic: string | string[]): Promise<void> {
    await this.admin.ensureTopics(topic);
  }

  protected async consume(consumer: KafkaConsumer): Promise<void> {
    const autoCommit = consumer.autoCommit;

    await consumer.run({
      autoCommit,
      ...consumer.batch
        ? { eachBatch: this.hangleEachBatch.bind(this, consumer) }
        : { eachMessage: this.hangleEachMessage.bind(this, consumer) },
    });
  }

  protected async hangleEachBatch(
    consumer: KafkaConsumer,
    payload: EachBatchPayload,
  ): Promise<void> {
    const messages = this.serde.deserialize(payload);
    const batch = { ...payload.batch, messages };

    this.logger.debug('Kafka - received batch of %d messages', messages.length);

    await this.handle({ ...payload, batch }).catch((err) =>
      this.onError(
        err,
        `Kafka - error handling batch in consumer ${consumer.groupId}`,
      )
    );
  }

  protected async hangleEachMessage(
    consumer: KafkaConsumer,
    payload: EachMessagePayload,
  ): Promise<void> {
    await this.handleMessage(consumer, payload).catch((err) =>
      this.onError(
        err,
        `Kafka - error handling message in consumer ${consumer.groupId}`,
      )
    );
  }

  protected async handleMessage(
    consumer: KafkaConsumer,
    payload: EachMessagePayload,
  ): Promise<void> {
    const message = this.serde.deserialize(payload);
    const ack = (): Promise<void> => this.commitOffset(payload, consumer);

    await this.handle({ ...payload, ack, message });
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

  protected async handle(
    payload: KafkaConsumerPayload | KafkaBatchPayload,
  ): Promise<void> {
    const topic = 'batch' in payload ? payload.batch.topic : payload.topic;
    const handlers = this.registry.getHandlers(topic);
    if (!handlers?.length) {
      return;
    }

    await Promise.allSettled(
      handlers.map((handler) =>
        handler
          .handle(payload as KafkaConsumerPayload & KafkaBatchPayload)
          .catch((err) => this.onError(err, 'Kafka - error handling message'))
      ),
    );
  }

  protected onError(err: any, message: string): void {
    this.logger.error(message);

    throw err;
  }
}
