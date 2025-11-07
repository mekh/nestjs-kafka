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
      ? this.handleBatch(payload)
      : this.handleMessage(payload, consumer);
  }

  protected async handleBatch(payload: EachBatchPayload): Promise<void> {
    const messages = this.serde.deserialize(payload);
    const batch = { ...payload.batch, messages };

    return this.execute({ ...payload, batch });
  }

  protected async handleMessage(
    payload: EachMessagePayload,
    consumer: KafkaConsumer,
  ): Promise<void> {
    const message = this.serde.deserialize(payload);
    const ack = (): Promise<void> => this.commitOffset(payload, consumer);

    return this.execute({ ...payload, ack, message });
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

  private async execute(data: any): Promise<void> {
    const method = this.methodName as keyof typeof this.provider.instance;
    const handler: Function = this.provider.instance[method];

    await handler.call(this.provider.instance, data);
  }
}
