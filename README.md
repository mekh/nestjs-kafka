# NestJS Kafka Module

A powerful and easy-to-use Kafka integration for NestJS applications.

## Table of Contents

- [Installation](#installation)
- [Overview](#overview)
- [Environment Variables](#environment-variables)
- [Configuration](#configuration)
  - [forRoot](#forroot)
  - [forRootAsync](#forrootasync)
- [Usage](#usage)
  - [KafkaService](#kafkaservice)
  - [KafkaConsumer Decorator](#kafkaconsumer-decorator)
  - [Sending Messages](#sending-messages)
  - [Ensuring Topics Exist](#ensuring-topics-exist)
- [Topic Auto-Creation](#topic-auto-creation)
- [License](#license)

## Installation

```bash
npm install @toxicoder/nestjs-kafka kafkajs
```

## Overview

The NestJS Kafka module provides a seamless integration with Apache Kafka for NestJS applications. It leverages the `kafkajs` package and enhances it with NestJS-specific features like decorators, dependency injection, and lifecycle management.

This module helps you:

- Connect to Kafka brokers
- Produce messages to Kafka topics
- Consume messages from Kafka topics using decorators
- Automatically create topics if they don't exist
- Manage Kafka connections throughout your application's lifecycle

## Environment Variables

| Variable                 | Type     | Default            | Description                                             |
| ------------------------ | -------- | ------------------ | ------------------------------------------------------- |
| KAFKA_BROKER             | string[] | ['localhost:9092'] | Comma-separated list of Kafka brokers                   |
| KAFKA_CLIENT_ID          | string   | undefined          | Client ID for Kafka                                     |
| KAFKA_RETRY_COUNT        | number   | undefined          | Number of retries for Kafka operations                  |
| KAFKA_RETRY_DELAY        | number   | undefined          | Initial retry delay in milliseconds                     |
| KAFKA_RETRY_TIMEOUT      | number   | undefined          | Maximum retry time in milliseconds                      |
| KAFKA_ENFORCE_TIMEOUT    | boolean  | undefined          | Whether to enforce request timeout                      |
| KAFKA_CONNECTION_TIMEOUT | number   | undefined          | Connection timeout in milliseconds                      |
| KAFKA_REQUEST_TIMEOUT    | number   | undefined          | Request timeout in milliseconds                         |
| KAFKA_TOPIC_AUTO_CREATE  | boolean  | false              | Whether to auto-create topics                           |
| KAFKA_LOG_LEVEL          | string   | 'error'            | Log level ('nothing', 'error', 'warn', 'info', 'debug') |

## Configuration

### forRoot

Use `forRoot` to configure the module with static options:

```typescript
import { Module } from '@nestjs/common';
import { KafkaModule } from '@toxicoder/nestjs-kafka';

@Module({
  imports: [
    KafkaModule.forRoot({
      brokers: ['localhost:9092'],
      clientId: 'my-app',
      topicAutoCreate: true,
      retry: {
        retries: 3,
        initialRetryTime: 300,
        maxRetryTime: 30000,
      },
    }),
  ],
})
export class AppModule {}
```

### forRootAsync

Use `forRootAsync` for dynamic configuration, such as loading from a configuration service:

```typescript
import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { KafkaModule } from '@toxicoder/nestjs-kafka';

@Module({
  imports: [
    ConfigModule.forRoot(),
    KafkaModule.forRootAsync({
      imports: [ConfigModule],
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => ({
        brokers: configService.get<string>('KAFKA_BROKERS').split(','),
        clientId: configService.get<string>('KAFKA_CLIENT_ID'),
        topicAutoCreate: configService.get<boolean>('KAFKA_TOPIC_AUTO_CREATE'),
        retry: {
          retries: configService.get<number>('KAFKA_RETRY_COUNT'),
          initialRetryTime: configService.get<number>('KAFKA_RETRY_DELAY'),
          maxRetryTime: configService.get<number>('KAFKA_RETRY_TIMEOUT'),
        },
      }),
    }),
  ],
})
export class AppModule {}
```

## Usage

### KafkaService

The `KafkaService` provides methods for interacting with Kafka. You need to initialize it in your service's `onModuleInit` method and clean up in `onModuleDestroy`:

```typescript
import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { KafkaService } from '@toxicoder/nestjs-kafka';

@Injectable()
export class AppService implements OnModuleInit, OnModuleDestroy {
  constructor(private readonly kafkaService: KafkaService) {}

  async onModuleInit() {
    // Initialize Kafka connections
    await this.kafkaService.init();
  }

  async onModuleDestroy() {
    // Clean up Kafka connections
    await this.kafkaService.destroy();
  }
}
```

### KafkaConsumer Decorator

Use the `@KafkaConsumer` decorator to mark methods as Kafka message handlers:

#### Basic Usage

```typescript
import { Injectable } from '@nestjs/common';
import { KafkaConsumer, KafkaMessagePayload } from '@toxicoder/nestjs-kafka';

@Injectable()
export class UserService {
  @KafkaConsumer('user-created', { groupId: 'user-service' })
  async handleUserCreated(payload: KafkaMessagePayload) {
    const user = payload.message.value;
    console.log(`User created: ${user.name}`);
  }
}
```

#### Advanced Configuration

```typescript
import { Injectable } from '@nestjs/common';
import { KafkaConsumer, KafkaMessagePayload } from '@toxicoder/nestjs-kafka';

@Injectable()
export class NotificationService {
  @KafkaConsumer(
    ['user-created', 'user-updated'],
    {
      groupId: 'notification-service',
      fromBeginning: true,
      autoCommit: false, // for manual acknoledge
      sessionTimeout: 30000,
      heartbeatInterval: 3000,
    },
  )
  async handleUserEvents(payload: KafkaMessagePayload) {
    try {
      const user = payload.message.value;
      console.log(`Processing user event for: ${user.name}`);

      // Process the message

      // Manually acknowledge the message
      await payload.ack();
    } catch (error) {
      console.error('Error processing message:', error);
      // Don't ack the message, so it can be reprocessed
    }
  }
}
```

### Sending Messages

Use the `send` method to produce messages to Kafka topics:

```typescript
import { Injectable } from '@nestjs/common';
import { KafkaService } from '@toxicoder/nestjs-kafka';

@Injectable()
export class UserService {
  constructor(private readonly kafkaService: KafkaService) {}

  async createUser(user: any) {
    // Save user to database

    // Send event to Kafka
    await this.kafkaService.send({
      topic: 'user-created',
      messages: {
        key: user.id,
        value: user,
        headers: {
          source: 'user-service',
          timestamp: Date.now().toString(),
        },
      },
    });
  }

  async updateUsers(users: any[]) {
    // Update users in database

    // Send multiple messages in one request
    await this.kafkaService.send({
      topic: 'user-updated',
      messages: users.map((user) => ({
        key: user.id,
        value: user,
      })),
    });
  }
}
```

### Ensuring Topics Exist

You can explicitly ensure that topics exist before using them:

```typescript
import { Injectable, OnModuleInit } from '@nestjs/common';
import { KafkaService } from '@toxicoder/nestjs-kafka';

@Injectable()
export class AppService implements OnModuleInit {
  constructor(private readonly kafkaService: KafkaService) {}

  async onModuleInit() {
    await this.kafkaService.init();

    // Ensure a single topic exists
    await this.kafkaService.ensureTopics('user-created');

    // Or ensure multiple topics exist
    await this.kafkaService.ensureTopics([
      'user-created',
      'user-updated',
      'user-deleted',
    ]);
  }
}
```

## Topic Auto-Creation

The `topicAutoCreate` option enables automatic creation of topics when they are needed but don't exist. When enabled:

1. Before sending a message to a topic, the module checks if the topic exists
2. If the topic doesn't exist, it's automatically created
3. The module retrieves the default `numPartitions` and `replicationFactor` values from the Kafka broker configuration
4. If the `replicationFactor` is greater than the number of available brokers, it's limited to the number of brokers

This feature is particularly useful in development environments or when you want to avoid manual topic creation.

To enable topic auto-creation:

```typescript
KafkaModule.forRoot({
  brokers: ['localhost:9092'],
  clientId: 'my-app',
  topicAutoCreate: true,
});
```

Or via environment variable:

```
KAFKA_TOPIC_AUTO_CREATE=true
```

## License

This project is licensed under the ISC License.
