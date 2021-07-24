import {
  Kafka,
  Consumer,
  Producer,
  Admin,
  EachMessagePayload,
  KafkaMessage,
  CompressionTypes,
} from 'kafkajs';

import {Channel} from 'node-channel';

import {Logger} from './logger';

export type MessageMetadata = Omit<EachMessagePayload, 'message'> & {
  message: Omit<KafkaMessage, 'value' | 'timestamp'> & {
    value?: Buffer | null;
    timestamp: number;
  };
  commitOffsets?: {
    topic: string;
    partition: number;
    offset: string;
  }[];
};

export interface Message<T> {
  metadata: MessageMetadata;
  value: T;
}

export interface Offset {
  partition: number;
  topic: string;
  offset: string;
}

export class Statistics {
  public recvTotal = 0;
  public sendTotal = 0;
}

export interface StreamContextOption {
  brokers: string[];
  inputTopic: string;
  clientId: string;
  commitInterval?: number;
  logger?: Logger;
}

export class StreamContext {
  kafka: Kafka;
  producer: Producer;
  consumer: Consumer;
  admin: Admin;
  inputTopic: string;
  statistics: Statistics = new Statistics();
  commitOffsets: Offset[] = [];
  commitInterval = 60_000;
  commitTimer?: ReturnType<typeof setInterval>;
  messageChannel: Channel<EachMessagePayload> = new Channel(10000);
  logger: Logger;

  constructor(option: StreamContextOption) {
    this.kafka = new Kafka({
      clientId: option.clientId,
      brokers: option.brokers,
    });
    this.producer = this.kafka.producer();
    this.consumer = this.kafka.consumer({
      groupId: option.clientId,
    });
    this.admin = this.kafka.admin();
    this.inputTopic = option.inputTopic;
    this.logger = option.logger ?? console;
    if (option.commitInterval) this.commitInterval = option.commitInterval;
  }
  async start() {
    await this.admin.connect();
    await this.producer.connect();
    await this.consumer.connect();
    this.consumer.subscribe({
      topic: this.inputTopic,
    });
    this.consumer.run({
      autoCommit: false,
      eachMessage: async payload => {
        await this.messageChannel.add(payload);
      },
    });

    this.commitTimer = setInterval(() => {
      this.consumer.commitOffsets(this.commitOffsets);
    }, this.commitInterval);
  }
  async send<T>(topic: string, messages: Message<T>[]) {
    await this.producer.send({
      topic,
      compression: CompressionTypes.GZIP,
      messages: messages.map(({value}) => ({value: JSON.stringify(value)})),
    });
    this.statistics.sendTotal += messages.length;
    this.updateCommitOffsets(messages);
  }
  async receive<T>(): Promise<Message<T>[] | undefined> {
    const payload = await this.messageChannel.get();
    if (payload === undefined) return undefined;
    const {
      message: {timestamp, value, ...restMessage},
      ...restPayload
    } = payload;
    const metadata: MessageMetadata = {
      message: {timestamp: Number(timestamp), ...restMessage},
      ...restPayload,
    };
    if (value) {
      const message = {metadata, value: JSON.parse(value.toString())};
      this.statistics.recvTotal += 1;
      return [message];
    } else return [];
  }
  async updateCommitOffsets<T>(messages: Message<T>[]): Promise<void> {
    const commitOffsets = messages.reduce((acc, m) => {
      if (m.metadata.commitOffsets) {
        for (const o of m.metadata.commitOffsets!) {
          const key = `${o.topic}:${o.partition}`;
          acc[key] = {
            partition: o.partition,
            topic: o.topic,
            offset: String(
              Math.max(
                Number(acc[key]?.offset || 0),
                Number(m.metadata.message.offset)
              )
            ),
          };
        }
      } else {
        const key = `${m.metadata.topic}:${m.metadata.partition}`;
        acc[key] = {
          partition: m.metadata.partition,
          topic: m.metadata.topic,
          offset: String(
            Math.max(
              Number(acc[key]?.offset || 0),
              Number(m.metadata.message.offset)
            )
          ),
        };
      }
      return acc;
    }, {} as Record<string, Offset>);
    this.commitOffsets = Object.values(commitOffsets);
  }
  async onDisconnect(callback: (...args: unknown[]) => void) {
    this.consumer.on(this.consumer.events.DISCONNECT, callback);
  }
  async seek(timestamp: number) {
    const offsets = await this.admin.fetchTopicOffsetsByTimestamp(
      this.inputTopic,
      timestamp
    );
    for (const offset of offsets) {
      this.consumer.seek({
        topic: this.inputTopic,
        partition: offset.partition,
        offset: offset.offset,
      });
    }
  }
  async disconnect() {
    await this.consumer.disconnect();
    await this.producer.disconnect();
    await this.admin.disconnect();
    if (this.commitTimer) clearInterval(this.commitTimer);
  }
  flushStatistics(): Statistics {
    const lastStatistics = this.statistics;
    this.statistics = new Statistics();
    return lastStatistics;
  }
}
