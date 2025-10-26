import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { DiscoveryService, MetadataScanner } from '@nestjs/core';
import { InstanceWrapper } from '@nestjs/core/injector/instance-wrapper';
import { KAFKA_CONFIG_TOKEN } from './kafka.constants';

import { ConsumerCreateInput, KafkaConsumer } from './kafka.consumer';
import { ConsumerDecorator } from './kafka.consumer.decorator';
import { KafkaHandler } from './kafka.handler';
import {
  ConsumerConfig,
  KafkaConfig,
  KafkaConsumerConfig,
  KafkaConsumerDecoratorConfig,
} from './kafka.interfaces';

type Provider = InstanceWrapper<object>;
type MaybeProvider = InstanceWrapper<object | undefined>;
type Opts = KafkaConsumerDecoratorConfig;

@Injectable()
export class KafkaRegistryService implements OnModuleInit {
  private readonly logger = new Logger(KafkaRegistryService.name);

  public readonly handlers = new Map<string, KafkaHandler[]>();

  public readonly consumers = new Map<string, KafkaConsumer>();

  private readonly defaultConsumerConfig?: KafkaConsumerConfig;

  constructor(
    @Inject(KAFKA_CONFIG_TOKEN) config: KafkaConfig,
    private readonly discoveryService: DiscoveryService,
    private readonly metadataScanner: MetadataScanner,
  ) {
    this.defaultConsumerConfig = config.consumer;
  }

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
    return Array.from(this.consumers.values());
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

    const config = this.composeConfig(meta);
    const { topics, fromBeginning, autoCommit } = meta;

    this.registerConsumer({
      config,
      topics,
      fromBeginning: fromBeginning ?? false,
      autoCommit: autoCommit ?? true,
    });

    topics.forEach((topic) => {
      this.registerHandler(topic, config, provider, method);
    });
  }

  private getMeta(provider: Provider, method: string): Opts | undefined {
    return this.discoveryService.getMetadataByDecorator(
      ConsumerDecorator,
      provider,
      method,
    );
  }

  private registerConsumer(data: ConsumerCreateInput): void {
    const { config, topics, fromBeginning, autoCommit } = data;
    const consumer = this.consumers.get(config.groupId) ??
      KafkaConsumer.create({
        topics: [],
        config,
        fromBeginning,
        autoCommit,
      });

    consumer.addTopics(topics);

    this.consumers.set(config.groupId, consumer);
  }

  private registerHandler(
    topic: string,
    config: ConsumerConfig,
    provider: Provider,
    methodName: string,
  ): void {
    const handlers = this.handlers.get(topic) ?? [];
    const handler = KafkaHandler.create(config, provider, methodName);

    handlers.push(handler);

    this.logger.log(
      'Kafka registry - registered handler %s for topic %s',
      handler.handlerName,
      topic,
    );

    this.handlers.set(topic, handlers);
  }

  private composeConfig(meta: Opts): ConsumerConfig {
    const { topics, fromBeginning, autoCommit, ...consumerConfig } = meta;

    const config = { ...this.defaultConsumerConfig, ...consumerConfig };
    if (!this.isConsumerConfig(config)) {
      throw new Error('Invalid consumer configuration');
    }

    return config;
  }

  private isConsumerConfig(
    config: KafkaConsumerConfig,
  ): config is ConsumerConfig {
    if (!config.groupId) {
      this.logger.error(
        // eslint-disable-next-line max-len
        'The groupId was not provided either in the module config or in the decorator.',
      );

      return false;
    }

    return true;
  }
}
