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
  contexts: StreamContext[];
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
  public lastSentMessageTimestamp = 0;
  merge(other: Statistics): Statistics {
    this.recvTotal += other.recvTotal;
    this.sendTotal += other.sendTotal;
    this.lastSentMessageTimestamp = Math.max(
      other.lastSentMessageTimestamp,
      this.lastSentMessageTimestamp
    );
    return this;
  }
}

export interface StreamContextOption {
  brokers: string[];
  inputTopic: string;
  groupId: string;
  fromBeginning?: boolean;
  clientId?: string;
  commitInterval?: number;
  logger?: Logger;
}

export class StreamContext {
  id: number;
  kafka: Kafka;
  producer: Producer;
  consumer: Consumer;
  admin: Admin;
  inputTopic: string;
  statistics: Statistics = new Statistics();
  commitOffsets: Offset[] = [];
  commitInterval = 60_000;
  commitTimer?: ReturnType<typeof setInterval>;
  messageChannel: Channel<EachMessagePayload> = new Channel(1000);
  fromBeginning: boolean;
  logger: Logger;
  disconnected = false;

  constructor(option: StreamContextOption) {
    this.kafka = new Kafka({
      clientId: option.clientId ?? option.groupId,
      brokers: option.brokers,
    });
    this.producer = this.kafka.producer();
    this.consumer = this.kafka.consumer({
      groupId: option.groupId,
    });
    this.admin = this.kafka.admin();
    this.inputTopic = option.inputTopic;
    this.logger = option.logger ?? console;
    this.fromBeginning = option.fromBeginning ?? false;
    this.id = Math.random();
    if (option.commitInterval) this.commitInterval = option.commitInterval;
  }
  async start() {
    await this.admin.connect();
    await this.producer.connect();
    await this.consumer.connect();
    this.consumer.subscribe({
      topic: this.inputTopic,
      fromBeginning: this.fromBeginning,
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

    this.consumer.on(this.consumer.events.DISCONNECT, () => {
      this.disconnected = true;
    });
  }
  async send<T>(topic: string, messages: Message<T>[]) {
    await this.producer.send({
      topic,
      compression: CompressionTypes.GZIP,
      messages: messages.map(({value}) => ({value: JSON.stringify(value)})),
    });
    this.statistics.sendTotal += messages.length;
    this.statistics.lastSentMessageTimestamp =
      messages[messages.length - 1].metadata.message.timestamp;
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
      contexts: [this],
    };
    if (value) {
      const message = {metadata, value: JSON.parse(value.toString())};
      this.statistics.recvTotal += 1;
      return [message];
    } else return [];
  }
  async updateCommitOffsets<T>(messages: Message<T>[]): Promise<void> {
    const contexts: Record<number, StreamContext> = {};
    const commitOffsetsByContextId: Record<number, Record<string, Offset>> = {};
    for (const m of messages) {
      for (const context of m.metadata.contexts) {
        commitOffsetsByContextId[context.id] = {};
        contexts[context.id] = context;
      }
      if (m.metadata.commitOffsets) {
        for (const o of m.metadata.commitOffsets!) {
          const key = `${o.topic}:${o.partition}`;
          for (const context of m.metadata.contexts) {
            commitOffsetsByContextId[context.id][key] = {
              partition: o.partition,
              topic: o.topic,
              offset: String(
                Math.max(
                  Number(
                    commitOffsetsByContextId[context.id][key]?.offset || 0
                  ),
                  Number(m.metadata.message.offset)
                )
              ),
            };
          }
        }
      } else {
        const key = `${m.metadata.topic}:${m.metadata.partition}`;
        for (const context of m.metadata.contexts) {
          commitOffsetsByContextId[context.id][key] = {
            partition: m.metadata.partition,
            topic: m.metadata.topic,
            offset: String(
              Math.max(
                Number(commitOffsetsByContextId[context.id][key]?.offset || 0),
                Number(m.metadata.message.offset)
              )
            ),
          };
        }
      }
    }
    for (const id in contexts) {
      contexts[id].commitOffsets = Object.values(commitOffsetsByContextId[id]);
    }
    //this.commitOffsets = Object.values(commitOffsets);
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
    this.disconnected = true;
    await this.consumer.disconnect();
    await this.producer.disconnect();
    await this.admin.disconnect();
    if (this.commitTimer) clearInterval(this.commitTimer);
  }
  isDisconnected(): boolean {
    return this.disconnected;
  }
  flushStatistics(): Statistics {
    const lastStatistics = this.statistics;
    this.statistics = new Statistics();
    return lastStatistics;
  }
}
