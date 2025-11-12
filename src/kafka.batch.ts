import { Logger } from '@nestjs/common';
import {
  EachBatchPayload,
  KafkaMessage,
  Offsets,
  OffsetsByTopicPartition,
} from 'kafkajs';

import {
  KafkaBatch as IBatch,
  KafkaBatchPayload,
  KafkaEachMessagePayload,
  KafkaSerde,
} from './kafka.interfaces';

type Resume = () => void;
type AckFn = () => Promise<void>;
type CreateAckFn = (offset: string) => AckFn;

export class KafkaBatch<
  T extends Record<string, any> = Record<string, any>,
> implements Iterable<KafkaEachMessagePayload<T>> {
  public static create(
    payload: EachBatchPayload,
    serde: KafkaSerde,
    createAck: CreateAckFn,
  ): KafkaBatch {
    return new KafkaBatch(payload, serde, createAck);
  }

  private readonly logger = new Logger(KafkaBatch.name);

  constructor(
    private readonly rawPayload: EachBatchPayload,
    private readonly serde: KafkaSerde,
    private readonly createAck: CreateAckFn,
  ) {}

  public get topic(): string {
    return this.rawPayload.batch.topic;
  }

  public get partition(): number {
    return this.rawPayload.batch.partition;
  }

  public get lastOffset(): string {
    return this.rawPayload.batch.lastOffset();
  }

  *[Symbol.iterator](): Iterator<KafkaEachMessagePayload<T>> {
    for (const message of this.rawPayload.batch.messages) {
      yield this.formatMessage(message);
    }
  }

  public pause(): Resume {
    this.logger.debug('pausing batch - %s:%d', this.topic, this.partition);
    const resume = this.rawPayload.pause();

    return () => {
      this.logger.debug('resuming batch - %s:%d', this.topic, this.partition);
      resume();
    };
  }

  public async heartbeat(): Promise<void> {
    this.logger.debug('heartbeat - %s:%d', this.topic, this.partition);
    return this.rawPayload.heartbeat();
  }

  public isRunning(): boolean {
    return this.rawPayload.isRunning();
  }

  public isStale(): boolean {
    return this.rawPayload.isStale();
  }

  public resolveOffset(offset: string): void {
    return this.rawPayload.resolveOffset(offset);
  }

  public uncommittedOffsets(): OffsetsByTopicPartition {
    return this.rawPayload.uncommittedOffsets();
  }

  public commitOffsetsIfNecessary(offsets?: Offsets): Promise<void> {
    return this.rawPayload.commitOffsetsIfNecessary(offsets);
  }

  public ack(): Promise<void> {
    return this.createAck(this.lastOffset)();
  }

  public getMessages(): KafkaEachMessagePayload<T>[] {
    return this.rawPayload.batch.messages.map((message) =>
      this.formatMessage(message)
    );
  }

  public createPayload(): KafkaBatchPayload<T> {
    return {
      batch: this.createBatch(),
      resolveOffset: this.resolveOffset.bind(this),
      heartbeat: this.heartbeat.bind(this),
      pause: this.pause.bind(this),
      commitOffsetsIfNecessary: this.commitOffsetsIfNecessary.bind(this),
      uncommittedOffsets: this.uncommittedOffsets.bind(this),
      isRunning: this.isRunning.bind(this),
      isStale: this.isStale.bind(this),
      ack: this.createAck(this.lastOffset),
    };
  }

  private createBatch(): IBatch<T> {
    const { messages, ...batch } = this.rawPayload.batch;

    return {
      ...batch,
      messages: this.getMessages(),
    };
  }

  private formatMessage(message: KafkaMessage): KafkaEachMessagePayload<T> {
    return {
      message: this.serde.deserialize(message),
      topic: this.topic,
      partition: this.partition,
      pause: this.pause.bind(this),
      heartbeat: this.heartbeat.bind(this),
      ack: this.createAck(message.offset),
    };
  }
}
