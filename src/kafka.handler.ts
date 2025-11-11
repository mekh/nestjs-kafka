import { Logger } from '@nestjs/common';
import { InstanceWrapper } from '@nestjs/core/injector/instance-wrapper';
import { ConsumerConfig, EachBatchPayload } from 'kafkajs';

import { KafkaConsumer } from './kafka.consumer';
import { KafkaConsumerPayload, KafkaSerde } from './kafka.interfaces';

type Provider = InstanceWrapper<object>;
type Resume = () => void;

export class KafkaHandler {
  public static create(
    config: ConsumerConfig,
    provider: Provider,
    methodName: string,
    serde: KafkaSerde,
  ): KafkaHandler {
    return new KafkaHandler(
      config,
      provider,
      methodName,
      serde,
    );
  }

  private readonly logger = new Logger(KafkaHandler.name);

  constructor(
    public readonly config: ConsumerConfig,
    public readonly provider: Provider,
    public readonly methodName: string,
    private readonly serde: KafkaSerde,
  ) {}

  get providerName(): string {
    return this.provider.instance.constructor.name;
  }

  get handlerName(): string {
    return `${this.providerName}.${this.methodName}`;
  }

  public async handle(
    payload: EachBatchPayload,
    consumer: KafkaConsumer,
  ): Promise<void> {
    const { topic, partition } = payload.batch;

    const messages: KafkaConsumerPayload[] = [];
    const isBatch = consumer.batch;

    const createAck = (offset: string): () => Promise<void> => async () => {
      payload.resolveOffset(offset);
      if (consumer.autoCommit) {
        return payload.commitOffsetsIfNecessary();
      }

      await consumer.commitOffset({
        topic,
        partition,
        offset: (Number(offset) + 1).toString(),
      });
    };

    const autoAck = async (ack: () => Promise<void>): Promise<void> => {
      return consumer.autoCommit ? ack() : undefined;
    };

    for (const message of payload.batch.messages) {
      if (payload.isStale() || !payload.isRunning()) {
        break;
      }

      const msgPayload: KafkaConsumerPayload = {
        message: this.serde.deserialize(message),
        topic,
        partition,
        ack: createAck(message.offset),
        pause: (): Resume => payload.pause(),
        heartbeat: async (): Promise<void> => payload.heartbeat(),
      };

      if (isBatch) {
        messages.push(msgPayload);
      } else {
        await this.execute(msgPayload);
        await autoAck(msgPayload.ack);

        await msgPayload.heartbeat();
      }

      if (!isBatch && consumer.isPaused(topic, partition)) {
        break;
      }
    }

    if (!isBatch) {
      return;
    }

    const batchPayload = Object.assign(payload, {
      batch: Object.assign(payload.batch, { messages }),
      ack: createAck(payload.batch.lastOffset()),
    });

    await this.execute(batchPayload);
    await autoAck(batchPayload.ack);
  }

  private async execute(data: any): Promise<void> {
    const method = this.methodName as keyof typeof this.provider.instance;
    const handler: Function = this.provider.instance[method];

    await handler.call(this.provider.instance, data);
  }
}
