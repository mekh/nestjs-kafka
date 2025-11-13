import { Logger } from '@nestjs/common';
import {
  EachBatchPayload,
  KafkaMessage,
  Offsets,
  OffsetsByTopicPartition,
} from 'kafkajs';

import { KafkaSerde } from './kafka-serde';
import { KafkaEachMessagePayload, Value } from './kafka.interfaces';

type Resume = () => void;
type AckFn = () => Promise<void>;
type CreateAckFn = (offset: string) => AckFn;

export class KafkaBatch<
  T extends Value = Value,
> implements Iterable<KafkaEachMessagePayload<T>> {
  public static create<T extends Value = Value>(
    payload: EachBatchPayload,
    createAck: CreateAckFn,
  ): KafkaBatch<T> {
    return new KafkaBatch<T>(payload, createAck);
  }

  private readonly logger = new Logger(KafkaBatch.name);

  private readonly serde: KafkaSerde<T>;

  constructor(
    private readonly rawPayload: EachBatchPayload,
    private readonly createAck: CreateAckFn,
  ) {
    this.serde = new KafkaSerde<T>();
  }

  public get length(): number {
    return this.rawPayload.batch.messages.length;
  }

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
