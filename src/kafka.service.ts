import { Inject, Injectable, Logger } from '@nestjs/common';
import { Kafka, Producer, RecordMetadata } from 'kafkajs';

import { KafkaAdminService } from './kafka-admin.service';
import { KafkaRegistryService } from './kafka-registry.service';
import { KafkaSerde } from './kafka-serde';
import { KAFKA_CONFIG_TOKEN } from './kafka.constants';
import { KafkaConsumer } from './kafka.consumer';
import { KafkaConfig, KafkaSendInput } from './kafka.interfaces';

@Injectable()
export class KafkaService {
  public readonly kafka: Kafka;

  protected readonly logger = new Logger(KafkaService.name);

  protected readonly producer: Producer;

  private readonly serde: KafkaSerde;

  protected consumers: KafkaConsumer[] = [];

  constructor(
    @Inject(KAFKA_CONFIG_TOKEN) config: KafkaConfig,
    public readonly registry: KafkaRegistryService,
    public readonly admin: KafkaAdminService,
  ) {
    this.serde = new KafkaSerde();
    this.kafka = new Kafka(config);
    this.producer = this.kafka.producer();
  }

  public async connect(): Promise<void> {
    await this.producer.connect();
    await this.admin.connect();

    this.logger.log('Kafka - producer connected');

    const topics = this.registry.getTopics();
    await this.admin.ensureTopics(topics);

    for (const consumer of this.registry.getConsumers()) {
      consumer.createConsumer(this.kafka);

      await consumer.connect();
      await consumer.subscribe();
      await consumer.run();

      this.consumers.push(consumer);
    }

    this.logger.log('Kafka - initialization completed');
  }

  public async disconnect(): Promise<void> {
    await this.producer.disconnect();
    await Promise.allSettled(
      this.consumers.map((consumer) => consumer.disconnect()),
    );

    this.consumers = [];
  }

  public async send(data: KafkaSendInput): Promise<RecordMetadata[]> {
    await this.ensureTopics(data.topic);

    return this.producer.send({
      topic: data.topic,
      messages: this.serde.serialize(data),
    });
  }

  public async ensureTopics(topic: string | string[]): Promise<void> {
    await this.admin.ensureTopics(topic);
  }
}
