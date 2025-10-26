import { DiscoveryModule } from '@nestjs/core';
import { Test } from '@nestjs/testing';

import { KafkaModule } from '../src';
import { KAFKA_CONFIG_TOKEN } from '../src/kafka.constants';

describe('KafkaModule', () => {
  it('should compile with default providers', async () => {
    const mod = await Test.createTestingModule({
      imports: [KafkaModule],
    }).compile();

    const token = mod.get(KAFKA_CONFIG_TOKEN);

    expect(token).toBeDefined();
  });

  it('should compile with forRoot config', async () => {
    const cfg = { brokers: ['b:1'] };

    const mod = await Test.createTestingModule({
      imports: [KafkaModule.forRoot(cfg)],
    }).compile();

    expect(mod.get(KAFKA_CONFIG_TOKEN)).toBe(cfg);
  });

  it('should compile with forRootAsync config', async () => {
    const cfg = { brokers: ['b:1'] };

    const mod = await Test.createTestingModule({
      imports: [
        KafkaModule.forRootAsync({
          imports: [DiscoveryModule],
          useFactory: () => cfg,
          inject: [],
          providers: [],
          global: false,
        }),
      ],
    }).compile();

    expect(mod.get(KAFKA_CONFIG_TOKEN)).toBe(cfg);
  });
});
