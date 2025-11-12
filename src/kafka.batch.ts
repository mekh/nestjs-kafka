import { EachBatchPayload, KafkaMessage } from 'kafkajs';

import {
  KafkaBatch as IBatch,
  KafkaBatchPayload,
  KafkaConsumerPayload,
  KafkaSerde,
} from './kafka.interfaces';

type Resume = () => void;
type Pause = () => Resume;
type Heartbeat = () => Promise<void>;
type AckFn = () => Promise<void>;
type CreateAckFn = (offset: string) => AckFn;
type IsStale = () => boolean;
type IsRunning = () => boolean;

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

  public get pause(): Pause {
    return this.rawPayload.pause.bind(null);
  }

  public get heartbeat(): Heartbeat {
    return this.rawPayload.heartbeat.bind(null);
  }

  public get isRunning(): IsRunning {
    return this.rawPayload.isRunning.bind(null);
  }

  public get isStale(): IsStale {
    return this.rawPayload.isStale.bind(null);
  }

  *[Symbol.iterator](): Iterator<KafkaConsumerPayload<T>> {
    for (const message of this.rawPayload.batch.messages) {
      yield this.formatMessage(message);
    }
  }

  public createPayload(): KafkaBatchPayload<T> {
    const { batch, ...payload } = this.rawPayload;

    return {
      ...payload,
      batch: this.createBatch(),
      ack: this.createAck(this.rawPayload.batch.lastOffset()),
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
      pause: this.pause,
      heartbeat: this.heartbeat,
      ack: this.createAck(message.offset),
    } as KafkaConsumerPayload<T>;
  }
}
