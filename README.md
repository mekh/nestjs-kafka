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
  - [Registering Consumers (consumer groups)](#registering-consumers-consumer-groups)
  - [KafkaService](#kafkaservice)
  - [KafkaConsumer Decorator](#kafkaconsumer-decorator)
    - [Parameter Decorators: Value, Headers, Key](#parameter-decorators-value-headers-key)
    - [Batch Mode](#batch-mode)
  - [Sending Messages](#sending-messages)
  - [Ensuring Topics Exist](#ensuring-topics-exist)
- [Topic Auto-Creation](#topic-auto-creation)
- [Serialization/Deserialization](#serializationdeserialization)
  - [Key and Headers normalization](#key-and-headers-normalization)
- [Consumer Configuration](#consumer-configuration)
  - [Module-level defaults (no groupId)](#module-level-defaults-no-groupid)
  - [Decorator vs Registration](#decorator-vs-registration)
  - [IMPORTANT: groupId requirement](#important-groupid-requirement)
- [Performance](#performance)
- [License](#license)

## Installation

```bash
npm install @toxicoder/nestjs-kafka
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

| Variable                               | Type     | Default            | Description                                                                                 |
| -------------------------------------- | -------- | ------------------ | ------------------------------------------------------------------------------------------- |
| KAFKA_BROKER                           | string[] | ['localhost:9092'] | Comma-separated list of Kafka brokers                                                       |
| KAFKA_CLIENT_ID                        | string   | undefined          | Client ID for Kafka                                                                         |
| KAFKA_ENFORCE_TIMEOUT                  | boolean  | undefined          | Whether to enforce request timeout                                                          |
| KAFKA_CONNECTION_TIMEOUT               | number   | undefined          | Connection timeout in milliseconds                                                          |
| KAFKA_REQUEST_TIMEOUT                  | number   | undefined          | Request timeout in milliseconds                                                             |
| KAFKA_AUTHENTICATION_TIMEOUT           | number   | undefined          | Authentication timeout in milliseconds                                                      |
| KAFKA_REAUTHENTICATION_THRESHOLD       | number   | undefined          | Minimum time in milliseconds between automatic re-authentication attempts                   |
| KAFKA_TOPIC_AUTO_CREATE                | boolean  | false              | Whether to auto-create topics                                                               |
| KAFKA_LOG_LEVEL                        | string   | 'error'            | Log level (`nothing`, `error`, `warn`, `info`, `debug`)                                     |
| KAFKA_RETRY_COUNT                      | number   | undefined          | Number of retries for Kafka operations                                                      |
| KAFKA_RETRY_DELAY                      | number   | undefined          | Initial retry delay in milliseconds                                                         |
| KAFKA_RETRY_TIMEOUT                    | number   | undefined          | Maximum total retry time in milliseconds                                                    |
| KAFKA_RETRY_FACTOR                     | number   | undefined          | Exponential backoff factor for retries                                                      |
| KAFKA_RETRY_MULTIPLIER                 | number   | undefined          | Multiplier applied to the retry delay per attempt                                           |
| KAFKA_CONSUMER_SESSION_TIMEOUT         | number   | undefined          | Consumer session timeout in milliseconds                                                    |
| KAFKA_CONSUMER_REBALANCE_TIMEOUT       | number   | undefined          | Timeout in milliseconds for rebalancing                                                     |
| KAFKA_CONSUMER_HEARTBEAT_INTERVAL      | number   | undefined          | Interval in milliseconds for consumer heartbeats                                            |
| KAFKA_CONSUMER_METADATA_MAX_AGE        | number   | undefined          | How long metadata is considered fresh (ms)                                                  |
| KAFKA_CONSUMER_MAX_BYTES_PER_PARTITION | number   | undefined          | Maximum bytes per partition to fetch                                                        |
| KAFKA_CONSUMER_MIN_BYTES               | number   | undefined          | Minimum bytes to accumulate before returning from fetch                                     |
| KAFKA_CONSUMER_MAX_BYTES               | number   | undefined          | Maximum bytes to accumulate before returning from fetch                                     |
| KAFKA_CONSUMER_MAX_WAIT_TIME_IN_MS     | number   | undefined          | Maximum wait time in milliseconds for fetch requests                                        |
| KAFKA_CONSUMER_MAX_IN_FLIGHT_REQUESTS  | number   | undefined          | Maximum number of in-flight requests per connection                                         |
| KAFKA_CONSUMER_READ_UNCOMMITTED        | boolean  | undefined          | If true, the consumer will read uncommitted messages                                        |
| KAFKA_CONSUMER_RACK_ID                 | string   | undefined          | Rack identifier for consumer (used for rack-aware partition assignment and fetch selection) |

Notes:

- Boolean variables must be provided as the strings `true` or `false`.
- Array variables are comma-separated lists without spaces (e.g., `host1:9092,host2:9092`).

## Configuration

### forRoot

Use `forRoot` to configure the module with static options. Note: the default consumer config here does NOT include `groupId` — you will register consumer groups separately (see "Registering Consumers"):

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
      // Default consumer configuration at module level
      consumer: {
        // you can set other kafkajs consumer options here as defaults
        sessionTimeout: 30000,
      },
    }),
  ],
})
export class AppModule {}
```

### forRootAsync

Use `forRootAsync` for dynamic configuration, such as loading from a configuration service.

You can set the `global` parameter to `true` to make the module global.
When a module is global, you don't need to import it in other modules
to use its providers. This is useful when you want to use the KafkaService
across multiple modules without having to import the KafkaModule in each one.

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
      global: true, // Makes the module global so you don't need to import it in other modules
      useFactory: (configService: ConfigService) => ({
        brokers: configService.get<string>('KAFKA_BROKER')?.split(',') ?? ['localhost:9092'],
        clientId: configService.get<string>('KAFKA_CLIENT_ID'),
        topicAutoCreate: configService.get<boolean>('KAFKA_TOPIC_AUTO_CREATE'),
        retry: {
          retries: configService.get<number>('KAFKA_RETRY_COUNT'),
          initialRetryTime: configService.get<number>('KAFKA_RETRY_DELAY'),
          maxRetryTime: configService.get<number>('KAFKA_RETRY_TIMEOUT'),
        },
        // Default consumer configuration at module level
        consumer: {
          // no groupId here
          sessionTimeout: 30000,
        },
      }),
    }),
  ],
})
export class AppModule {}
```

## Usage

### Registering Consumers (consumer groups)

You must register consumer groups before using the `@KafkaConsumer` decorator. Registration defines the consumer group configuration (including `groupId`, batch mode, auto-commit, concurrency, etc.).

Use `KafkaModule.registerConsumer` in the module where you declare your handlers:

```typescript
import { Module } from '@nestjs/common';
import { KafkaModule } from '@toxicoder/nestjs-kafka';
import { UserService } from './user.service';

@Module({
  imports: [
    // register by groupId string (uses module-level defaults)
    KafkaModule.registerConsumer('user-service'),

    // or register with full config per group
    KafkaModule.registerConsumer({
      groupId: 'notification-service',
      // run options (defaults shown)
      batch: true,
      autoCommit: true,
      autoCommitInterval: null,
      autoCommitThreshold: null,
      partitionsConsumedConcurrently: 1,
      // plus any kafkajs ConsumerConfig fields, except groupId is required here
      sessionTimeout: 30000,
    }),

    // register multiple groups at once
    KafkaModule.registerConsumer([
      'billing-service',
      { groupId: 'analytics', batch: false, autoCommit: false },
    ]),
  ],
  providers: [UserService],
})
export class UsersModule {}
```

Notes:

- `groupId` is required per registered consumer group.
- Batch mode is configured here (default: `true`).
- Manual commit is enabled by setting `autoCommit: false` here; then use `ack()` inside your handler.
- The decorator controls subscription details (`topics`, `fromBeginning`) per handler; group-level run behavior is defined at registration time.

### KafkaService

The `KafkaService` provides methods for interacting with Kafka. You need to initialize it
in your service's `onModuleInit` method and clean up in `onModuleDestroy`:

```typescript
import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { KafkaService } from '@toxicoder/nestjs-kafka';

@Injectable()
export class AppService implements OnModuleInit, OnModuleDestroy {
  constructor(private readonly kafkaService: KafkaService) {}

  async onModuleInit() {
    // Initialize Kafka connections
    await this.kafkaService.connect();
  }

  async onModuleDestroy() {
    // Clean up Kafka connections
    await this.kafkaService.disconnect();
  }
}
```

### KafkaConsumer Decorator

Use the `@KafkaConsumer` decorator to mark methods as Kafka message handlers. Signature:

```
@KafkaConsumer(groupId: string, topic: string | string[], config?: { fromBeginning?: boolean })
```

#### Basic Usage

```typescript
import { Injectable } from '@nestjs/common';
import { KafkaConsumer, KafkaEachMessagePayload } from '@toxicoder/nestjs-kafka';

@Injectable()
export class UserService {
  @KafkaConsumer('user-service', 'user-created')
  async handleUserCreated(payload: KafkaEachMessagePayload) {
    const user = payload.message.value;
    console.log(`User created: ${user?.name}`);
  }
}
```

#### Advanced Configuration

```typescript
import { Injectable } from '@nestjs/common';
import { KafkaConsumer, KafkaEachMessagePayload } from '@toxicoder/nestjs-kafka';

@Injectable()
export class NotificationService {
  // subscribe this handler of group "notification-service" to two topics
  @KafkaConsumer('notification-service', ['user-created', 'user-updated'], { fromBeginning: true })
  async handleUserEvents(payload: KafkaEachMessagePayload) {
    try {
      const user = payload.message.value;
      // ...process
      await payload.ack(); // works if the group was registered with autoCommit: false
    } catch (error) {
      // handle error (no ack -> message will not be committed for manual commit groups)
    }
  }
}
```

#### Parameter Decorators: Value, Headers, Key

These parameter decorators allow you to extract parts of the Kafka message directly into your handler parameters. You can combine them in any order. If a parameter is not decorated, the full payload object will be passed.

- `@Value()` extracts the parsed message value
- `@Key()` extracts the message key (always a string if present)
- `@Headers()` extracts the headers as a plain object with string values

Single-message mode example:

```typescript
import { Injectable } from '@nestjs/common';
import { Headers, KafkaConsumer, Key, Value } from '@toxicoder/nestjs-kafka';

interface UserCreatedEvent {
  id: string;
  name: string;
}

@Injectable()
export class UserService {
  @KafkaConsumer('user-service', 'user-created')
  async handleUserCreated(
    @Value() value: UserCreatedEvent, // parsed JSON
    @Key() key: string | undefined, // coerced to string
    @Headers() headers: Record<string, string | undefined>,
  ) {
    // value: { id: '123', name: 'John' }
    // key: '123'
    // headers: { source: 'api', traceId: '...' }
  }
}
```

Notes:

- If you omit all parameter decorators, your method receives the entire `KafkaEachMessagePayload` (which also includes `ack()` if the group was registered with `autoCommit: false`).
- You can mix decorated and non-decorated parameters; undecorated ones receive the full payload.

#### Batch Mode

Batch mode lets your handler receive and process a batch of messages at once.

How to enable batch mode:

- Configure the consumer group with `batch: true` when registering it via `KafkaModule.registerConsumer` (default is `true`).

```typescript
import { Injectable } from '@nestjs/common';
import { Headers, KafkaBatch, KafkaConsumer, Key, Value } from '@toxicoder/nestjs-kafka';

@Injectable()
export class BatchService {
  // Batch mode is defined at registration time for the group
  @KafkaConsumer('analytics', ['user-created', 'user-updated'])
  async handleBatch(payload: KafkaBatch) {
    // Iterate over normalized messages
    for (const msg of payload) {
      // msg: KafkaEachMessagePayload
      const { key, value, headers } = msg.message;
      // ...
      await msg.ack(); // works if autoCommit is true (auto-commit will happen) or false (manual)
      await msg.heartbeat();
    }
  }
}
```

What the handler receives in batch mode (`KafkaBatch` instance):

- `payload` is an instance of `KafkaBatch` exposing `topic`, `partition`, iterator over messages, and helper methods (`ack()`, `heartbeat()`, `pause()`, `commitOffsetsIfNecessary()`, etc.).
- Iterating over `payload` yields normalized messages where each message is:
  - `key?: string` — coerced to string if present
  - `value?: Record<string, any>` — parsed from JSON when possible
  - `headers?: Record<string, string | undefined>` — all header values coerced to string

Using parameter decorators in batch mode:

- `@Value()` provides an array of message values: `Record<string, any>[]`
- `@Key()` provides an array of keys: `(string | undefined)[]`
- `@Headers()` provides an array of headers objects: `Record<string, string | undefined>[]`
- Undecorated parameters receive the full `KafkaBatch` instance

Example with decorators in batch mode:

```typescript
@Injectable()
export class BatchServiceWithDecorators {
  @KafkaConsumer('analytics', 'user-created')
  async handle(
    @Value() values: Record<string, any>[],
    @Key() keys: (string | undefined)[],
    @Headers() headers: Record<string, string | undefined>[],
    payload: KafkaBatch, // optional extra param for full context
  ) {
    // values[i], keys[i], and headers[i] correspond to the same message
    for (let i = 0; i < values.length; i++) {
      // process values[i] with keys[i] and headers[i]
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
    // Establish connections and initialize consumers
    await this.kafkaService.connect();

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

## Serialization/Deserialization

This module automatically handles JSON serialization and deserialization of Kafka messages:

### Serialization

When sending messages to Kafka using the `send` method:

- The `value` field of each message is automatically serialized using `JSON.stringify`
- All other message properties (key, headers, etc.) remain unchanged
- This happens inside the module's serializer (`KafkaSerde`) used by `KafkaService`

```typescript
// Your original object
const user = { id: '123', name: 'John Doe', email: 'john@example.com' };

// When you send it:
await kafkaService.send({
  topic: 'user-created',
  messages: {
    key: user.id,
    value: user, // This object is automatically serialized to JSON string
  },
});

// What actually gets sent to Kafka:
// key: '123'
// value: '{"id":"123","name":"John Doe","email":"john@example.com"}'
```

### Deserialization

When consuming messages from Kafka:

- The message value is automatically parsed using `JSON.parse`
- If parsing fails, the original string value is preserved
- The parsed object replaces the original string value in the message

```typescript
import { Injectable } from '@nestjs/common';
import { KafkaConsumer, KafkaEachMessagePayload } from '@toxicoder/nestjs-kafka';

// What comes from Kafka:
// key: '123'
// value: '{"id":"123","name":"John Doe","email":"john@example.com"}'

@Injectable()
export class UserConsumer {
  @KafkaConsumer('user-service', 'user-created')
  async handleUserCreated(payload: KafkaEachMessagePayload) {
    const user = payload.message.value;
    // user is already a parsed object: { id: '123', name: 'John Doe', email: 'john@example.com' }
    console.log(`User created: ${user?.name}`);
  }
}
```

This automatic serialization/deserialization allows you to work directly with JavaScript objects
without having to manually handle JSON conversion in your application code.

### Key and Headers normalization

In addition to value parsing, the module normalizes `key` and `headers` for convenience:

- Message `key` is automatically coerced to a string (if present) on the consumer side.
  - Single-message mode: `payload.message.key` is `string | undefined`.
  - Batch mode: each `batch.messages[i].key` is `string | undefined`.
- Message `headers` values are automatically coerced to strings (if present) on the consumer side.
  - Single-message mode: `payload.message.headers` is `Record<string, string | undefined>`.
  - Batch mode: each `batch.messages[i].headers` is `Record<string, string | undefined>`.

This means your handlers can rely on string types for keys and header values without manual Buffer-to-string conversion.

## Consumer Configuration

### Module-level defaults (no groupId)

You can define default consumer options at the module level via the `consumer` field
in `KafkaModule.forRoot` / `forRootAsync`. These defaults (e.g., `sessionTimeout`, `metadataMaxAge`, etc.)
apply to all registered consumer groups unless overridden at registration time.

Group IDs are not defined here — you must provide `groupId` when registering consumers
with `KafkaModule.registerConsumer`.

### Decorator vs Registration

- `@KafkaConsumer(groupId, topics, { fromBeginning? })` controls subscriptions and topic list per handler.
- Group-level run behavior such as `batch`, `autoCommit`, `partitionsConsumedConcurrently`, and other `kafkajs` options
  is defined when calling `KafkaModule.registerConsumer({ groupId, ... })`.

### IMPORTANT: groupId requirement

IMPORTANT: A `groupId` must be registered via `KafkaModule.registerConsumer`. The `@KafkaConsumer` decorator's first
argument is the `groupId` and must match one of the registered groups. If you forget to register a group, the module
will fail to resolve the consumer and initialization will fail.

## Performance

This module is optimized to work efficiently with KafkaJS, but your handler patterns strongly influence throughput and latency. Consider the following recommendations and trade-offs:

Recommended for maximum throughput:

- Use batch mode with auto-commit enabled:
  - Register the group with `batch: true` and `autoCommit: true`.
  - In your handler, accept a `KafkaBatch` and iterate with `for ... of` over the batch instance. Avoid parameter decorators for hot paths.

Example (fast path):

```typescript
@Injectable()
export class FastConsumer {
  @KafkaConsumer('analytics', ['events-a', 'events-b'])
  async handle(batch: KafkaBatch) {
    // Iterate with minimal overhead
    for (const msg of batch) {
      const { key, value, headers } = msg.message;
      // ...process quickly
      // In autoCommit=true mode you don't need to call msg.ack() per message
      // (the framework will call batch.ack() once after your handler returns).
      // You may still call msg.ack() occasionally in very long loops if you
      // want to flush commits earlier, KafkaJS will respect interval/threshold.
      await msg.heartbeat();
    }
    // No explicit commit needed in autoCommit=true mode; the framework
    // will resolve and commit offsets according to KafkaJS policies.
  }
}
```

Notes on decorators overhead:

- For small message rates, using `@Value()`, `@Key()`, `@Headers()` is convenient and adds negligible overhead.
- For high-throughput streams, these decorators may noticeably impact performance because they allocate arrays and map over messages to extract values before calling your handler. Prefer handling the `KafkaBatch` directly in hot paths.

Manual commits vs auto commits:

- `autoCommit: false` gives you full control over commits, but disables KafkaJS scheduled commits (`autoCommitInterval`/`autoCommitThreshold`). In this mode, every `ack()` results in an immediate `consumer.commitOffsets({ ... })` under the hood, which can increase broker load.
- With `autoCommit: true`, commits are coalesced by KafkaJS according to `autoCommitInterval` and/or `autoCommitThreshold`. In batch mode, the framework will call `batch.ack()` for you after the handler completes; if you call `msg.ack()` inside the loop, KafkaJS will still apply its scheduling and batching rules.

## License

This project is licensed under the ISC License.
