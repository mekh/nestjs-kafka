import { Logger } from '@nestjs/common';
import { InstanceWrapper } from '@nestjs/core/injector/instance-wrapper';
import { ConsumerConfig, EachBatchPayload } from 'kafkajs';

import { KafkaBatch } from './kafka.batch';
import { KafkaConsumer } from './kafka.consumer';
import {
  KafkaBatchPayload,
  KafkaEachMessagePayload,
  KafkaSerde,
} from './kafka.interfaces';

type Provider = InstanceWrapper<object>;
type ProviderMethod<T = any> = (data: T) => Promise<void> | void;

export class KafkaHandler {
  public static create(
    config: ConsumerConfig,
    provider: Provider,
    methodName: string,
    serde: KafkaSerde,
  ): KafkaHandler {
    return new KafkaHandler(
      config,
      provider,
      methodName,
      serde,
    );
  }

  private readonly logger = new Logger(KafkaHandler.name);

  constructor(
    public readonly config: ConsumerConfig,
    public readonly provider: Provider,
    public readonly methodName: string,
    private readonly serde: KafkaSerde,
  ) {}

  get providerName(): string {
    return this.provider.instance.constructor.name;
  }

  get handlerName(): string {
    return `${this.providerName}.${this.methodName}`;
  }

  public async handle(
    payload: EachBatchPayload,
    consumer: KafkaConsumer,
  ): Promise<void> {
    const { topic, partition } = payload.batch;
    this.logger.debug('consuming batch - %s:%d', topic, partition);

    const createAck = (offset: string): () => Promise<void> => async () => {
      payload.resolveOffset(offset);
      if (consumer.autoCommit) {
        this.logger.debug('auto-commit (%s) - %s:%d', offset, topic, partition);

        return payload.commitOffsetsIfNecessary();
      }

      this.logger.debug('commit (%s) - %s:%d', offset, topic, partition);
      await consumer.commitOffset({
        topic,
        partition,
        offset: (Number(offset) + 1).toString(),
      });
    };

    const batch = KafkaBatch.create(payload, this.serde, createAck);

    return consumer.batch
      ? this.handleBatch(batch, consumer)
      : this.handleEachMessage(batch, consumer);
  }

  private async handleEachMessage(
    batch: KafkaBatch,
    consumer: KafkaConsumer,
  ): Promise<void> {
    const { topic, partition } = batch;
    this.logger.debug('handling each message - %s:%d', topic, partition);
    for (const message of batch) {
      if (batch.isStale() || !batch.isRunning()) {
        this.logger.debug('batch is stale/stopped -%s:%d', topic, partition);
        break;
      }

      await this.execute(message);

      if (consumer.autoCommit) {
        await message.ack();
      }

      await message.heartbeat();

      if (consumer.isPaused(batch.topic, batch.partition)) {
        this.logger.debug('consuming is paused - %s:%d', topic, partition);
        break;
      }
    }
  }

  private async handleBatch(
    batch: KafkaBatch,
    consumer: KafkaConsumer,
  ): Promise<void> {
    const { topic, partition } = batch;
    this.logger.debug('handling batch - %s:%d', topic, partition);
    const payload = batch.createPayload();

    await this.execute(payload);

    if (consumer.autoCommit) {
      await payload.ack();
    }
  }

  private async execute(
    data: KafkaEachMessagePayload | KafkaBatchPayload,
  ): Promise<void> {
    const method = this.methodName as keyof typeof this.provider.instance;
    const handler: ProviderMethod = this.provider.instance[method];

    await handler.call(this.provider.instance, data);
  }
}
