import { Logger } from '@nestjs/common';
import { InstanceWrapper } from '@nestjs/core/injector/instance-wrapper';
import { EachBatchPayload } from 'kafkajs';

import { KafkaBatch } from './kafka.batch';
import type { KafkaConsumer } from './kafka.consumer';
import { KafkaEachMessagePayload } from './kafka.interfaces';

type Provider = InstanceWrapper<object>;
type ProviderMethod<T = any> = (data: T) => Promise<void> | void;

export class KafkaHandler {
  public static create(
    provider: Provider,
    methodName: string,
    consumer: KafkaConsumer,
  ): KafkaHandler {
    return new KafkaHandler(
      provider,
      methodName,
      consumer,
    );
  }

  private readonly logger = new Logger(KafkaHandler.name);

  constructor(
    public readonly provider: Provider,
    public readonly methodName: string,
    public readonly consumer: KafkaConsumer,
  ) {}

  get providerName(): string {
    return this.provider.instance.constructor.name;
  }

  get handlerName(): string {
    return `${this.providerName}.${this.methodName}`;
  }

  public async handle(payload: EachBatchPayload): Promise<void> {
    const { topic, partition } = payload.batch;
    this.logger.debug('consuming batch - %s:%d', topic, partition);

    const batch = KafkaBatch.create(
      payload,
      (offset) => this.createAckFn(payload, offset),
    );

    return this.consumer.batch
      ? this.handleBatch(batch)
      : this.handleEachMessage(batch);
  }

  private createAckFn(
    payload: EachBatchPayload,
    offset: string,
  ) {
    return async (): Promise<void> => {
      const { topic, partition } = payload.batch;
      payload.resolveOffset(offset);
      if (this.consumer.autoCommit) {
        this.logger.debug('auto-commit (%s) - %s:%d', offset, topic, partition);

        return payload.commitOffsetsIfNecessary();
      }

      this.logger.debug('commit (%s) - %s:%d', offset, topic, partition);
      await this.consumer.commitOffset({
        topic,
        partition,
        offset: (Number(offset) + 1).toString(),
      });
    };
  }

  private async handleEachMessage(batch: KafkaBatch): Promise<void> {
    const { topic, partition } = batch;
    this.logger.debug('handling each message - %s:%d', topic, partition);
    for (const message of batch) {
      if (batch.isStale() || !batch.isRunning()) {
        this.logger.debug('batch is stale/stopped -%s:%d', topic, partition);
        break;
      }

      await this.execute(message);

      if (this.consumer.autoCommit) {
        await message.ack();
      }

      await message.heartbeat();

      if (this.consumer.isPaused(batch.topic, batch.partition)) {
        this.logger.debug('consuming is paused - %s:%d', topic, partition);
        break;
      }
    }
  }

  private async handleBatch(batch: KafkaBatch): Promise<void> {
    const { topic, partition } = batch;
    this.logger.debug('handling batch - %s:%d', topic, partition);

    await this.execute(batch);

    if (this.consumer.autoCommit) {
      await batch.ack();
    }
  }

  private async execute(
    data: KafkaEachMessagePayload | KafkaBatch,
  ): Promise<void> {
    const method = this.methodName as keyof typeof this.provider.instance;
    const handler: ProviderMethod = this.provider.instance[method];

    await handler.call(this.provider.instance, data);
  }
}
