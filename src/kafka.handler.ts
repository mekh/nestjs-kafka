import { InstanceWrapper } from '@nestjs/core/injector/instance-wrapper';
import { ConsumerConfig } from 'kafkajs';

import { KafkaConsumerHandler } from './kafka.interfaces';

type Provider = InstanceWrapper<object>;

export class KafkaHandler {
  public static create(
    config: ConsumerConfig,
    provider: Provider,
    methodName: string,
  ): KafkaHandler {
    return new KafkaHandler(config, provider, methodName);
  }

  constructor(
    public readonly config: ConsumerConfig,
    public readonly provider: Provider,
    public readonly methodName: string,
  ) {}

  get providerName(): string {
    return this.provider.instance.constructor.name;
  }

  get handlerName(): string {
    return `${this.providerName}.${this.methodName}`;
  }

  get handle(): KafkaConsumerHandler {
    const method = this.methodName as keyof typeof this.provider.instance;
    const handle: KafkaConsumerHandler = this.provider.instance[method];

    return handle.bind(this.provider.instance);
  }
}
