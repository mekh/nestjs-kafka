let __admin: any = {
  listTopics: async () => [],
  createTopics: async () => true,
  describeCluster: async () => ({ brokers: [] }),
  describeConfigs: async () => ({ resources: [{ configEntries: [] }] }),
  connect: async () => undefined,
  disconnect: async () => undefined,
};

let __producer: any = {
  connect: async () => undefined,
  disconnect: async () => undefined,
  send: async () => [],
};

export class Kafka {
  constructor(config: any) {}

  public producer() {
    return __producer;
  }

  public admin() {
    return __admin;
  }
}

export const ConfigResourceTypes = {
  BROKER: 2,
};

export const logLevel = {
  NOTHING: 0,
  ERROR: 1,
  WARN: 2,
  INFO: 4,
  DEBUG: 5,
};

export const Partitioners = {
  LegacyPartitioner: Symbol('LegacyPartitioner'),
};

export const __setKafkaMock = (opts: { admin?: any; producer?: any }) => {
  if (opts.admin) {
    __admin = opts.admin;
  }

  if (opts.producer) {
    __producer = opts.producer;
  }
};
