import {
  DynamicModule,
  Global,
  Logger,
  Module,
  OnApplicationShutdown,
} from '@nestjs/common';
import { DiscoveryModule, ModuleRef } from '@nestjs/core';

import { KafkaAdminService } from './kafka-admin.service';
import { KafkaConfigService } from './kafka-config.service';
import { KafkaConsumersModule } from './kafka-consumers.module';
import { KafkaModuleConfig } from './kafka-module.config';
import { KafkaRegistryService } from './kafka-registry.service';
import { KAFKA_CONFIG_TOKEN } from './kafka.constants';

import {
  KafkaAsyncConfig,
  KafkaConfig as IKafkaConfig,
  KafkaConsumerConfig,
} from './kafka.interfaces';
import { KafkaService } from './kafka.service';

type ConsumerGroupConfig = Omit<KafkaConsumerConfig, 'fromBeginning'>;
type ConsumerGroup = string;
type RegisterConsumerInput =
  | ConsumerGroupConfig
  | ConsumerGroupConfig[]
  | ConsumerGroup
  | ConsumerGroup[];

const providers = [
  KafkaAdminService,
  KafkaRegistryService,
  KafkaService,
  KafkaConfigService,
];

const toExport = [
  KafkaAdminService,
  KafkaRegistryService,
  KafkaService,
  KafkaConfigService,
  KAFKA_CONFIG_TOKEN,
];

@Global()
@Module({
  imports: [DiscoveryModule],
  providers: [
    {
      provide: KAFKA_CONFIG_TOKEN,
      useFactory: (): IKafkaConfig => {
        const conf = KafkaModuleConfig.getConfig();
        const logger = new Logger('Kafka');

        return {
          ...conf,
          logCreator: () => (entity) => {
            logger.log(entity);
          },
        };
      },
    },
    ...providers,
  ],
  exports: toExport,
})
export class KafkaModule implements OnApplicationShutdown {
  public static forRoot(config: IKafkaConfig): DynamicModule {
    return {
      module: KafkaModule,
      imports: [DiscoveryModule],
      providers: [
        {
          provide: KAFKA_CONFIG_TOKEN,
          useValue: config,
        },
        ...providers,
      ],
      exports: toExport,
    };
  }

  public static forRootAsync(options: KafkaAsyncConfig): DynamicModule {
    const imports = options.imports ?? [];
    if (!imports.includes(DiscoveryModule)) {
      imports.push(DiscoveryModule);
    }

    return {
      module: KafkaModule,
      global: options.global,
      imports,
      providers: [
        ...options.providers ?? [],
        {
          provide: KAFKA_CONFIG_TOKEN,
          inject: options.inject ?? [],
          useFactory: options.useFactory,
        },
        ...providers,
      ],
      exports: toExport,
    };
  }

  public static registerConsumer(input: RegisterConsumerInput): DynamicModule {
    return KafkaConsumersModule.register(input);
  }

  constructor(private readonly moduleRef: ModuleRef) {}

  async onApplicationShutdown(signal?: string): Promise<void> {
    if (!['SIGTERM', 'SIGINT'].includes(signal ?? '')) {
      return;
    }

    const svc = this.moduleRef.get<KafkaService>(KafkaService);

    try {
      await svc.disconnect();
    } catch {
      //
    }
  }
}
