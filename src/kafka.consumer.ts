import { Logger } from '@nestjs/common';
import {
  Consumer,
  ConsumerConfig,
  ConsumerRunConfig,
  ConsumerSubscribeTopics,
  EachBatchPayload,
  EachMessagePayload,
  Kafka,
  TopicPartitionOffsetAndMetadata,
} from 'kafkajs';

import { RunConfig, SubscriptionConfig } from './kafka.interfaces';

export interface ConsumerCreateInput {
  consumerConfig: ConsumerConfig;
  subscriptionConfig: SubscriptionConfig;
  runConfig: RunConfig;
}

type CommitOffsetsData = TopicPartitionOffsetAndMetadata;
type ConsumerHandler = (
  consumer: KafkaConsumer,
  payload: EachMessagePayload | EachBatchPayload,
) => Promise<void>;

export class KafkaConsumer {
  public static create(input: ConsumerCreateInput): KafkaConsumer {
    return new KafkaConsumer(input);
  }

  protected readonly logger = new Logger(KafkaConsumer.name);

  private kafkaConsumer?: Consumer;

  private topicsSet: Set<string>;

  constructor(private readonly input: ConsumerCreateInput) {
    this.topicsSet = new Set(input.subscriptionConfig.topics);
  }

  public get consumer(): Consumer {
    if (!this.kafkaConsumer) {
      throw new Error('Kafka consumer is not created');
    }

    return this.kafkaConsumer;
  }

  public get groupId(): string {
    return this.input.consumerConfig.groupId;
  }

  public get subscriptionConfig(): ConsumerSubscribeTopics {
    return {
      topics: [...this.topicsSet.values()],
      fromBeginning: this.input.subscriptionConfig.fromBeginning,
    };
  }

  public get consumerConfig(): ConsumerConfig {
    return this.input.consumerConfig;
  }

  public get runConfig(): ConsumerRunConfig {
    const { batch, ...runConfig } = this.input.runConfig;

    return runConfig;
  }

  public get batch(): boolean | undefined {
    return this.input.runConfig.batch;
  }

  public get autoCommit(): boolean {
    return !!this.input.runConfig.autoCommit;
  }

  public addTopics(topics: string[]): void {
    topics.forEach((topic) => this.topicsSet.add(topic));
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

  public async run(handle: ConsumerHandler): Promise<void> {
    await this.consumer.run({
      ...this.runConfig,
      ...this.batch
        ? { eachBatch: handle.bind(handle, this) }
        : { eachMessage: handle.bind(handle, this) },
    });

    this.logger.log('Kafka consumer - started (%s)', this.groupId);
  }

  public async subscribe(): Promise<void> {
    const config = this.subscriptionConfig;
    await this.consumer.subscribe(config);

    this.logger.log(
      'Kafka consumer - subscribed to topics (%s): %s',
      this.groupId,
      config.topics.join(','),
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
}
