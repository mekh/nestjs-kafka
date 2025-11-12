import { Logger } from '@nestjs/common';
import { InstanceWrapper } from '@nestjs/core/injector/instance-wrapper';
import { ConsumerConfig, EachBatchPayload } from 'kafkajs';

import { KafkaBatch } from './kafka.batch';
import { KafkaConsumer } from './kafka.consumer';
import { KafkaSerde } from './kafka.interfaces';

type Provider = InstanceWrapper<object>;

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
    const createAck = (offset: string): () => Promise<void> => async () => {
      payload.resolveOffset(offset);
      if (consumer.autoCommit) {
        return payload.commitOffsetsIfNecessary();
      }

      await consumer.commitOffset({
        topic: payload.batch.topic,
        partition: payload.batch.partition,
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
    for (const message of batch) {
      if (batch.isStale() || !batch.isRunning()) {
        break;
      }

      await this.execute(message);

      if (consumer.autoCommit) {
        await message.ack();
      }

      await message.heartbeat();

      if (consumer.isPaused(batch.topic, batch.partition)) {
        break;
      }
    }
  }

  private async handleBatch(
    batch: KafkaBatch,
    consumer: KafkaConsumer,
  ): Promise<void> {
    const payload = batch.createPayload();

    await this.execute(payload);

    if (consumer.autoCommit) {
      await payload.ack();
    }
  }

  private async execute(data: any): Promise<void> {
    const method = this.methodName as keyof typeof this.provider.instance;
    const handler: Function = this.provider.instance[method];

    await handler.call(this.provider.instance, data);
  }
}
