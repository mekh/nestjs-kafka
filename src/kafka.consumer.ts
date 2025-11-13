import { Logger } from '@nestjs/common';
import {
  Consumer,
  ConsumerConfig,
  ConsumerRunConfig,
  ConsumerSubscribeTopics,
  Kafka,
  TopicPartitionOffsetAndMetadata,
} from 'kafkajs';

import { KafkaBatch } from './kafka.batch';
import { KafkaHandler } from './kafka.handler';
import {
  KafkaEachMessagePayload,
  RunConfig,
  SubscriptionConfig,
} from './kafka.interfaces';

export interface ConsumerCreateInput {
  consumerConfig: ConsumerConfig;
  subscriptionConfig: SubscriptionConfig;
  runConfig: Required<RunConfig>;
}

type ProviderMethod = (
  data: KafkaEachMessagePayload | KafkaBatch,
) => Promise<void> | void;
type CommitOffsetsData = TopicPartitionOffsetAndMetadata;

export class KafkaConsumer {
  public static create(
    config: ConsumerCreateInput,
    cb: ProviderMethod,
  ): KafkaConsumer {
    return new KafkaConsumer(config, cb);
  }

  private readonly logger = new Logger(KafkaConsumer.name);

  private readonly topicsSet: Set<string>;

  private readonly handler: KafkaHandler;

  private kafkaConsumer?: Consumer;

  constructor(
    private readonly config: ConsumerCreateInput,
    private readonly cb: ProviderMethod,
  ) {
    this.handler = KafkaHandler.create(this);
    this.topicsSet = new Set(config.subscriptionConfig.topics);
  }

  public get consumer(): Consumer {
    if (!this.kafkaConsumer) {
      throw new Error('Kafka consumer is not created');
    }

    return this.kafkaConsumer;
  }

  public get groupId(): string {
    return this.config.consumerConfig.groupId;
  }

  public get subscriptionConfig(): ConsumerSubscribeTopics {
    return {
      topics: [...this.topicsSet.values()],
      fromBeginning: this.config.subscriptionConfig.fromBeginning,
    };
  }

  public get consumerConfig(): ConsumerConfig {
    return this.config.consumerConfig;
  }

  public get runConfig(): ConsumerRunConfig {
    const { batch, ...runConfig } = this.config.runConfig;

    return runConfig;
  }

  public get batch(): boolean {
    return this.config.runConfig.batch;
  }

  public get autoCommit(): boolean {
    return this.config.runConfig.autoCommit;
  }

  public createConsumer(kafka: Kafka): Consumer {
    this.kafkaConsumer = kafka.consumer(this.consumerConfig);

    return this.kafkaConsumer;
  }

  public async connect(): Promise<void> {
    await this.consumer.connect();

    this.logger.log('Kafka consumer - connected (%s)', this.groupId);
  }

  public async disconnect(): Promise<void> {
    const groupId = this.groupId;

    return this.consumer
      .disconnect()
      .then(() =>
        this.logger.log('Kafka consumer - disconnected (%s)', groupId)
      )
      .catch((err) => {
        this.logger.error('Kafka consumer - disconnect failed (%s)', groupId);
        throw err;
      });
  }

  public async run(): Promise<void> {
    await this.consumer.run({
      ...this.runConfig,
      eachBatch: (payload) => this.handler.handle(payload),
    });

    this.logger.log('Kafka consumer - started (%s)', this.groupId);
  }

  public async subscribe(): Promise<void> {
    const config = this.subscriptionConfig;
    await this.consumer.subscribe(config);

    this.logger.log(
      'Kafka consumer - subscribed to topics (%s): %s',
      this.groupId,
      config.topics.join(', '),
    );
  }

  public async commitOffset(data: CommitOffsetsData): Promise<void> {
    await this.consumer.commitOffsets([data]);

    this.logger.debug(
      'Kafka consumer - committed (%s): %o',
      this.groupId,
      data,
    );
  }

  public isPaused(topic: string, partition: number): boolean {
    return this.consumer.paused().some(({ topic: t, partitions: p }) =>
      t === topic && p.includes(partition)
    );
  }

  public async execute(
    payload: KafkaEachMessagePayload | KafkaBatch,
  ): Promise<void> {
    return this.cb(payload);
  }
}
