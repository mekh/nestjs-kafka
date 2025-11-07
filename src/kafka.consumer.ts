import { Logger } from '@nestjs/common';
import {
  Consumer,
  ConsumerConfig,
  ConsumerRunConfig,
  Kafka,
  TopicPartitionOffsetAndMetadata,
} from 'kafkajs';

export interface ConsumerCreateInput {
  config: ConsumerConfig;
  topics: string[];
  fromBeginning: boolean;
  autoCommit: boolean;
  batch: boolean;
}

type CommitOffsetsData = TopicPartitionOffsetAndMetadata;

export class KafkaConsumer {
  public static create(input: ConsumerCreateInput): KafkaConsumer {
    return new KafkaConsumer(input);
  }

  protected readonly logger = new Logger(KafkaConsumer.name);

  private kafkaConsumer?: Consumer;

  private topicsSet: Set<string>;

  constructor(private readonly input: ConsumerCreateInput) {
    this.topicsSet = new Set(input.topics);
  }

  public get consumer(): Consumer {
    if (!this.kafkaConsumer) {
      throw new Error('Kafka consumer is not created');
    }

    return this.kafkaConsumer;
  }

  public get groupId(): string {
    return this.input.config.groupId;
  }

  public get topics(): string[] {
    return [...this.topicsSet.values()];
  }

  public get config(): ConsumerConfig {
    return this.input.config;
  }

  public get fromBeginning(): boolean {
    return this.input.fromBeginning;
  }

  public get autoCommit(): boolean {
    return this.input.autoCommit;
  }

  public get batch(): boolean {
    return this.input.batch;
  }

  public addTopics(topics: string[]): void {
    topics.forEach((topic) => this.topicsSet.add(topic));
  }

  public createConsumer(kafka: Kafka): Consumer {
    this.kafkaConsumer = kafka.consumer(this.config);

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

  public async run(input: ConsumerRunConfig): Promise<void> {
    await this.consumer.run(input);

    this.logger.log('Kafka consumer - started (%s)', this.groupId);
  }

  public async subscribe(): Promise<void> {
    await this.consumer.subscribe({
      topics: this.topics,
      fromBeginning: this.fromBeginning,
    });

    this.logger.log(
      'Kafka consumer - subscribed to topics (%s): %s',
      this.groupId,
      this.topics.join(','),
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
