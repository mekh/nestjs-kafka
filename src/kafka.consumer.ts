import { Logger } from '@nestjs/common';
import {
  Consumer,
  ConsumerConfig,
  ConsumerRunConfig,
  EachBatchPayload,
  Kafka,
  TopicPartitionOffsetAndMetadata,
} from 'kafkajs';

import { KafkaSubscriptionConfig, RunConfig } from './kafka.interfaces';

export interface ConsumerCreateInput {
  consumerConfig: ConsumerConfig;
  runConfig: Required<RunConfig>;
}

type CommitOffsetsData = TopicPartitionOffsetAndMetadata;
type BatchHandler = (payload: EachBatchPayload) => Promise<void>;

export class KafkaConsumer {
  public static create(
    config: ConsumerCreateInput,
    cb: BatchHandler,
  ): KafkaConsumer {
    return new KafkaConsumer(config, cb);
  }

  private readonly logger = new Logger(KafkaConsumer.name);

  private kafkaConsumer?: Consumer;

  private subConfig = {
    fromBeginning: new Set<string>(),
    fromEnd: new Set<string>(),
  };

  constructor(
    private readonly config: ConsumerCreateInput,
    private readonly cb: BatchHandler,
  ) {}

  public get consumer(): Consumer {
    if (!this.kafkaConsumer) {
      throw new Error('Kafka consumer is not created');
    }

    return this.kafkaConsumer;
  }

  public get groupId(): string {
    return this.config.consumerConfig.groupId;
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

  public addSubscription(opts: KafkaSubscriptionConfig): void {
    const set = opts.fromBeginning
      ? this.subConfig.fromBeginning
      : this.subConfig.fromEnd;

    opts.topics.forEach((topic) => set.add(topic));
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
      eachBatch: this.cb.bind(this.cb),
    });

    this.logger.log('Kafka consumer - started (%s)', this.groupId);
  }

  public async subscribe(): Promise<void> {
    const fromBeginning = Array.from(this.subConfig.fromBeginning);
    const fromEnd = Array.from(this.subConfig.fromEnd);
    if (fromBeginning.length) {
      await this.consumer.subscribe({
        topics: fromBeginning,
        fromBeginning: true,
      });
    }

    if (fromEnd.length) {
      await this.consumer.subscribe({
        topics: fromEnd,
        fromBeginning: false,
      });
    }

    this.logger.log(
      'Kafka consumer - subscribed to topics (%s): %s',
      this.groupId,
      [...fromBeginning, ...fromEnd].join(','),
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
}
