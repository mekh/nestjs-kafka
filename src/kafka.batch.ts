import { Logger } from '@nestjs/common';
import { EachBatchPayload, KafkaMessage } from 'kafkajs';

import {
  KafkaBatch as IBatch,
  KafkaBatchPayload,
  KafkaConsumerPayload,
  KafkaSerde,
} from './kafka.interfaces';

type Resume = () => void;
type AckFn = () => Promise<void>;
type CreateAckFn = (offset: string) => AckFn;

export class KafkaBatch<
  T extends Record<string, any> = Record<string, any>,
> implements Iterable<KafkaConsumerPayload<T>> {
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

  *[Symbol.iterator](): Iterator<KafkaConsumerPayload<T>> {
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

  public createPayload(): KafkaBatchPayload<T> {
    const { batch, ...payload } = this.rawPayload;

    return {
      ...payload,
      batch: this.createBatch(),
      ack: this.createAck(this.lastOffset),
    };
  }

  private createBatch(): IBatch<T> {
    const { messages, ...batch } = this.rawPayload.batch;

    return {
      ...batch,
      messages: messages.map((message) => this.formatMessage(message)),
    };
  }

  private formatMessage(message: KafkaMessage): KafkaConsumerPayload<T> {
    return {
      message: this.serde.deserialize(message),
      topic: this.topic,
      partition: this.partition,
      pause: this.pause.bind(this),
      heartbeat: this.heartbeat.bind(this),
      ack: this.createAck(message.offset),
    } as KafkaConsumerPayload<T>;
  }
}
