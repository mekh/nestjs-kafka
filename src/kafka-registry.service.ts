import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { DiscoveryService, MetadataScanner, ModuleRef } from '@nestjs/core';
import { InstanceWrapper } from '@nestjs/core/injector/instance-wrapper';
import { EachBatchPayload } from 'kafkajs';

import { KafkaConsumer } from './kafka.consumer';
import { ConsumerDecorator } from './kafka.decorators';
import { KafkaHandler } from './kafka.handler';
import { KafkaConsumerDecoratorConfig } from './kafka.interfaces';

type Provider = InstanceWrapper<object>;
type MaybeProvider = InstanceWrapper<object | undefined>;
type Opts = KafkaConsumerDecoratorConfig;

@Injectable()
export class KafkaRegistryService implements OnModuleInit {
  private readonly logger = new Logger(KafkaRegistryService.name);

  public readonly consumers = new Map<string, KafkaConsumer>();

  public readonly handlers = new Map<string, KafkaHandler[]>();

  constructor(
    private readonly discoveryService: DiscoveryService,
    private readonly metadataScanner: MetadataScanner,
    private readonly moduleRef: ModuleRef,
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

    const consumer = this.registerConsumer(meta.groupId);
    consumer.addSubscription(meta);
    const consumerName = provider.instance.constructor.name;
    const handlerName = [consumerName, method].join('.');
    this.logger.log(
      'Kafka registry - registered consumer %s for topics %s',
      handlerName,
      meta.topics.join(', '),
    );

    meta.topics.forEach((topic) =>
      this.registerHandler(topic, provider, method, consumer)
    );
  }

  private getMeta(provider: Provider, method: string): Opts | undefined {
    return this.discoveryService.getMetadataByDecorator(
      ConsumerDecorator,
      provider,
      method,
    );
  }

  private registerConsumer(groupId: string): KafkaConsumer {
    const consumer = this.moduleRef.get<KafkaConsumer>(groupId, {
      strict: false,
    });

    this.consumers.set(groupId, consumer);

    return consumer;
  }

  private registerHandler(
    topic: string,
    provider: Provider,
    methodName: string,
    consumer: KafkaConsumer,
  ): void {
    const handlers = this.handlers.get(topic) ?? [];
    const handler = KafkaHandler.create(provider, methodName, consumer);

    handlers.push(handler);

    this.logger.log(
      'Kafka registry - registered handler %s for topic %s',
      handler.handlerName,
      topic,
    );

    this.handlers.set(topic, handlers);
  }

  public async handle(payload: EachBatchPayload): Promise<void> {
    const handlers = this.getHandlers(payload.batch.topic);
    if (!handlers?.length) {
      return;
    }

    await Promise.allSettled(
      handlers.map((handler) => handler.handle(payload)),
    );
  }
}
