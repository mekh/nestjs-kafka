import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { DiscoveryService, MetadataScanner } from '@nestjs/core';
import { InstanceWrapper } from '@nestjs/core/injector/instance-wrapper';

import { KafkaConfigService } from './kafka-config.service';
import { KafkaBatch } from './kafka.batch';
import { ConsumerCreateInput, KafkaConsumer } from './kafka.consumer';
import { ConsumerDecorator } from './kafka.decorators';

import {
  KafkaConsumerDecoratorConfig,
  KafkaEachMessagePayload,
} from './kafka.interfaces';

type Provider = InstanceWrapper<object>;
type MaybeProvider = InstanceWrapper<object | undefined>;
type Opts = KafkaConsumerDecoratorConfig;
type ProviderMethod<T = any> = (data: T) => Promise<void> | void;

@Injectable()
export class KafkaRegistryService implements OnModuleInit {
  private readonly logger = new Logger(KafkaRegistryService.name);

  private readonly topics = new Set<string>();

  private readonly consumers: KafkaConsumer[] = [];

  constructor(
    private readonly discoveryService: DiscoveryService,
    private readonly metadataScanner: MetadataScanner,
    private readonly configService: KafkaConfigService,
  ) {}

  onModuleInit(): void {
    this.scanAndRegister();
  }

  public getTopics(): string[] {
    return Array.from(this.topics.values());
  }

  public getConsumers(): KafkaConsumer[] {
    return this.consumers;
  }

  private scanAndRegister(): void {
    const providers: MaybeProvider[] = this.discoveryService.getProviders();

    for (const prov of providers) {
      if (this.isValidProvider(prov)) {
        this.scanProvider(prov);
      }
    }
  }

  private isValidProvider(provider: MaybeProvider): provider is Provider {
    return (
      !!provider.instance &&
      typeof provider.instance === 'object' &&
      !!provider.metatype
    );
  }

  private scanProvider(provider: Provider): void {
    const prototype: object = Object.getPrototypeOf(provider.instance);

    this.metadataScanner
      .getAllMethodNames(prototype)
      .forEach((method) => this.scanMethod(provider, method));
  }

  private scanMethod(provider: Provider, method: string): void {
    const meta = this.getMeta(provider, method);

    if (!meta) {
      return;
    }

    const config = this.configService.composeConsumerConfig(meta);
    this.registerConsumer(config, provider, method);
  }

  private getMeta(provider: Provider, method: string): Opts | undefined {
    return this.discoveryService.getMetadataByDecorator(
      ConsumerDecorator,
      provider,
      method,
    );
  }

  private registerConsumer(
    config: ConsumerCreateInput,
    provider: Provider,
    methodName: string,
  ): void {
    const { topics } = config.subscriptionConfig;
    const cb = this.createCb(provider, methodName);
    const consumer = KafkaConsumer.create(config, cb);

    topics.forEach((topic) => this.topics.add(topic));

    const consumerName = provider.instance.constructor.name;
    const handlerName = [consumerName, methodName].join('.');
    this.logger.log(
      'Kafka registry - registered consumer %s for topics %s',
      handlerName,
      topics.join(', '),
    );

    this.consumers.push(consumer);
  }

  private createCb(
    provider: Provider,
    methodName: string,
  ): ProviderMethod<KafkaEachMessagePayload | KafkaBatch> {
    const method = methodName as keyof typeof provider.instance;
    const cb: ProviderMethod = provider.instance[method];

    return cb.bind(provider.instance);
  }
}
