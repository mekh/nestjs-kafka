import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { DiscoveryService, MetadataScanner } from '@nestjs/core';
import { InstanceWrapper } from '@nestjs/core/injector/instance-wrapper';

import { ConsumerCreateInput, KafkaConsumer } from './kafka.consumer';
import { ConsumerDecorator } from './kafka.decorators';
import { KafkaHandler } from './kafka.handler';

import { KafkaConsumerDecoratorConfig } from './kafka.interfaces';

type Provider = InstanceWrapper<object>;
type MaybeProvider = InstanceWrapper<object | undefined>;
type Opts = KafkaConsumerDecoratorConfig;

@Injectable()
export class KafkaRegistryService implements OnModuleInit {
  private static readonly consumerGroups = new Map<
    string,
    ConsumerCreateInput
  >();

  public static addConsumerGroup(
    groupId: string,
    config: ConsumerCreateInput,
  ): void {
    this.consumerGroups.set(groupId, config);
  }

  private readonly logger = new Logger(KafkaRegistryService.name);

  public readonly consumers = new Map<string, KafkaConsumer>();

  public readonly handlers = new Map<string, KafkaHandler[]>();

  constructor(
    private readonly discoveryService: DiscoveryService,
    private readonly metadataScanner: MetadataScanner,
  ) {}

  onModuleInit(): void {
    this.scanAndRegister();
  }

  public getTopics(): string[] {
    return Array.from(this.handlers.keys());
  }

  public getHandlers(topic: string): KafkaHandler[] | undefined {
    return this.handlers.get(topic);
  }

  public getConsumers(): KafkaConsumer[] {
    return [...this.consumers.values()];
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

    const config = KafkaRegistryService.consumerGroups.get(meta.groupId);
    if (!config) {
      throw new Error(`Consumer group ${meta.groupId} is not registered`);
    }

    this.registerConsumer(config, meta, provider, method);

    meta.topics.forEach((topic) =>
      this.registerHandler(topic, provider, method)
    );
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
    subscription: KafkaConsumerDecoratorConfig,
    provider: Provider,
    methodName: string,
  ): void {
    const { groupId } = config.consumerConfig;
    const consumer = this.consumers.get(groupId) ??
      KafkaConsumer.create(config);
    consumer.addSubscription(subscription);

    const consumerName = provider.instance.constructor.name;
    const handlerName = [consumerName, methodName].join('.');
    this.logger.log(
      'Kafka registry - registered consumer %s for topics %s',
      handlerName,
      subscription.topics.join(', '),
    );

    this.consumers.set(groupId, consumer);
  }

  private registerHandler(
    topic: string,
    provider: Provider,
    methodName: string,
  ): void {
    const handlers = this.handlers.get(topic) ?? [];
    const handler = KafkaHandler.create(provider, methodName);

    handlers.push(handler);

    this.logger.log(
      'Kafka registry - registered handler %s for topic %s',
      handler.handlerName,
      topic,
    );

    this.handlers.set(topic, handlers);
  }
}
