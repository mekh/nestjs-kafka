import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { DiscoveryService, MetadataScanner } from '@nestjs/core';
import { InstanceWrapper } from '@nestjs/core/injector/instance-wrapper';
import { KafkaSerdeService } from './kafka-serde.service';
import { KAFKA_CONFIG_TOKEN } from './kafka.constants';

import { ConsumerCreateInput, KafkaConsumer } from './kafka.consumer';
import { ConsumerDecorator } from './kafka.decorators';
import { KafkaHandler } from './kafka.handler';
import {
  ConsumerConfig,
  KafkaConfig,
  KafkaConsumerConfig,
  KafkaConsumerDecoratorConfig,
  KafkaSerde,
  RunConfig,
  SubscriptionConfig,
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
    @Inject(KafkaSerdeService) private readonly serde: KafkaSerde,
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
    this.registerConsumer(config);

    config.subscriptionConfig.topics.forEach((topic) => {
      this.registerHandler(topic, config.consumerConfig, provider, method);
    });
  }

  private getMeta(provider: Provider, method: string): Opts | undefined {
    return this.discoveryService.getMetadataByDecorator(
      ConsumerDecorator,
      provider,
      method,
    );
  }

  private registerConsumer(config: ConsumerCreateInput): void {
    const { groupId: id } = config.consumerConfig;
    const consumer = this.consumers.get(id) ?? KafkaConsumer.create(config);

    consumer.addTopics(config.subscriptionConfig.topics);

    this.consumers.set(id, consumer);
  }

  private registerHandler(
    topic: string,
    config: ConsumerConfig,
    provider: Provider,
    methodName: string,
  ): void {
    const handlers = this.handlers.get(topic) ?? [];
    const handler = KafkaHandler.create(
      config,
      provider,
      methodName,
      this.serde,
    );

    handlers.push(handler);

    this.logger.log(
      'Kafka registry - registered handler %s for topic %s',
      handler.handlerName,
      topic,
    );

    this.handlers.set(topic, handlers);
  }

  private composeConfig(flatConfig: Opts): ConsumerCreateInput {
    const {
      topics,
      fromBeginning,
      batch,
      autoCommit,
      autoCommitInterval,
      autoCommitThreshold,
      partitionsConsumedConcurrently,
      ...consumerConfig
    } = flatConfig;

    const runConfig: RunConfig = {
      batch,
      autoCommit,
      autoCommitInterval,
      autoCommitThreshold,
      eachBatchAutoResolve: !!autoCommit,
      partitionsConsumedConcurrently,
    };

    const subConfig: SubscriptionConfig = {
      topics,
      fromBeginning: fromBeginning ?? false,
    };

    const consumer = {
      ...this.defaultConsumerConfig,
      ...consumerConfig,
    };

    if (!this.isConsumerConfig(consumer)) {
      throw new Error('Invalid consumer configuration');
    }

    return {
      consumerConfig: consumer,
      subscriptionConfig: subConfig,
      runConfig,
    };
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
