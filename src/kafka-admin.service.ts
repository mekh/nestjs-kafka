import { Inject, Injectable, Logger } from '@nestjs/common';
import { Admin, ConfigResourceTypes, ITopicConfig, Kafka } from 'kafkajs';

import { KAFKA_CONFIG_TOKEN } from './kafka.constants';
import { KafkaConfig } from './kafka.interfaces';

@Injectable()
export class KafkaAdminService {
  private readonly logger = new Logger(KafkaAdminService.name);

  private readonly knownTopics = new Set();

  public readonly kafka: Kafka;

  private readonly admin: Admin;

  private isConnected = false;

  private isConnecting = false;

  constructor(
    @Inject(KAFKA_CONFIG_TOKEN) private readonly config: KafkaConfig,
  ) {
    this.kafka = new Kafka(config);
    this.admin = this.kafka.admin();
  }

  public async ensureTopicsExist(topic: string | string[]): Promise<void> {
    if (!this.config.topicAutoCreate) {
      return;
    }

    const topics = Array.isArray(topic) ? topic : [topic];

    const unknownTopics = topics.filter((t) => !this.knownTopics.has(t));
    if (!unknownTopics.length) {
      return;
    }

    const existingTopics = await this.admin.listTopics();
    this.addExistingTopics(existingTopics);

    const missingTopics = unknownTopics.filter((t) => !this.knownTopics.has(t));

    if (!missingTopics.length) {
      return;
    }

    this.logger.log(
      'Kafka admin - creating missing topics: %s',
      missingTopics.join(', '),
    );

    const { numPartitions, replicationFactor } = await this.getTopicsConfig();

    await this.admin.createTopics({
      topics: missingTopics.map((topic) => ({
        topic,
        numPartitions,
        replicationFactor,
      })),
    });

    this.addExistingTopics(missingTopics);
  }

  public async connect(): Promise<void> {
    if (this.isConnected) {
      return;
    }

    if (this.isConnecting) {
      return this.waitConnection();
    }

    this.isConnecting = true;

    await this.admin.connect();
    this.isConnected = true;

    this.logger.log('Kafka admin - connected');
  }

  public async disconnect(): Promise<void> {
    await this.admin.disconnect();

    this.logger.log('Kafka admin - disconnected');
  }

  private async waitConnection(): Promise<void> {
    const connectionTimeout = this.config.connectionTimeout || 10000;
    const startTime = Date.now();

    while (Date.now() - startTime < connectionTimeout) {
      if (this.isConnected) {
        return;
      }

      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    throw new Error('Kafka admin - connection timeout');
  }

  private addExistingTopics(topics: string[]): void {
    topics.forEach((topic) => this.knownTopics.add(topic));
  }

  private async getTopicsConfig(): Promise<
    Pick<ITopicConfig, 'numPartitions' | 'replicationFactor'>
  > {
    const configNames = {
      numPartitions: 'num.partitions',
      replicationFactor: 'default.replication.factor',
    };

    const { brokers } = await this.admin.describeCluster();

    const configs = await this.admin.describeConfigs({
      resources: brokers.map((broker) => ({
        type: ConfigResourceTypes.BROKER,
        name: broker.nodeId.toString(),
        configNames: Object.values(configNames),
      })),
      includeSynonyms: false,
    });

    const [{ configEntries }] = configs.resources;

    const { configValue: numPartitions } = configEntries.find(
      (entry) => entry.configName === configNames.numPartitions,
    ) ?? { configValue: '1' };

    const { configValue: replicationFactor } = configEntries.find(
      (entry) => entry.configName === configNames.replicationFactor,
    ) ?? { configValue: '1' };

    return {
      numPartitions: parseInt(numPartitions, 10),
      replicationFactor: Math.min(
        parseInt(replicationFactor, 10),
        brokers.length,
      ),
    };
  }
}
