import { Logger } from '@nestjs/common';
import { InstanceWrapper } from '@nestjs/core/injector/instance-wrapper';
import { ConsumerConfig, EachBatchPayload, EachMessagePayload } from 'kafkajs';

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
    payload: EachMessagePayload | EachBatchPayload,
    consumer: KafkaConsumer,
  ): Promise<void> {
    return 'batch' in payload
      ? this.handleBatch(payload, consumer)
      : this.handleMessage(payload, consumer);
  }

  protected async handleBatch(
    payload: EachBatchPayload,
    consumer: KafkaConsumer,
  ): Promise<void> {
    if (payload.isStale() || !payload.isRunning()) {
      this.logger.error('the consumer is either stale or not running');

      return;
    }
    const ack = (): Promise<void> => this.commitOffset(payload, consumer);

    const messages = this.serde.deserialize(payload);
    const batch = { ...payload.batch, messages };

    await this.execute({ ...payload, batch, ack });
    if (consumer.autoCommit) {
      await ack();
    }
  }

  protected async handleMessage(
    payload: EachMessagePayload,
    consumer: KafkaConsumer,
  ): Promise<void> {
    const message = this.serde.deserialize(payload);
    const ack = (): Promise<void> => this.commitOffset(payload, consumer);

    return this.execute({ ...payload, message, ack });
  }

  protected async commitOffset(
    payload: EachMessagePayload | EachBatchPayload,
    consumer: KafkaConsumer,
  ): Promise<void> {
    if (consumer.autoCommit) {
      return;
    }

    if ('batch' in payload) {
      const lastOffset = payload.batch.lastOffset();
      this.logger.debug(
        'Kafka - committing batch offset %s for batch %s',
        lastOffset,
        payload.batch.topic,
      );
      payload.resolveOffset(payload.batch.lastOffset());

      await payload.commitOffsetsIfNecessary(payload.uncommittedOffsets());

      return;
    }

    await consumer.commitOffset({
      topic: payload.topic,
      partition: payload.partition,
      offset: (Number(payload.message.offset) + 1).toString(),
    });
  }

  private async execute(data: any): Promise<void> {
    const method = this.methodName as keyof typeof this.provider.instance;
    const handler: Function = this.provider.instance[method];

    await handler.call(this.provider.instance, data);
  }
}
