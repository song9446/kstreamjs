import {
  StreamContext,
  MessageMetadata,
  Message,
  Offset,
  StreamContextOption,
  Statistics,
} from './context';

import {
  ElementOf,
  sleep,
  findLastIndex,
  firstPromiseResolveOrSkip,
  SKIPPED,
} from './utils';

export class Stream<O> {
  contexts: StreamContext[];
  handleMessages: () => Promise<Message<O>[]>;
  constructor(
    contexts: StreamContext[],
    handleMessages?: () => Promise<Message<O>[]>
  ) {
    this.contexts = contexts;
    if (handleMessages !== undefined) this.handleMessages = handleMessages;
    else if (contexts.length === 1)
      this.handleMessages = async () => {
        const msg = await this.contexts[0].receive<O>();
        if (msg === undefined) throw Error('channel is closed');
        else return msg!;
      };
    else
      throw Error(
        'Wrong Stream Initializer Parameters. It must be either of Multiple contexts with handler or single context without handler'
      );
  }
  map<N>(next: (value: O, metadata: MessageMetadata) => N): Stream<N> {
    return new Stream(this.contexts, async () => {
      const messages = await this.handleMessages();
      return messages.map(msg => ({
        value: next(msg.value, msg.metadata),
        metadata: msg.metadata,
      }));
    });
  }
  filter(test: (value: O, metadata: MessageMetadata) => boolean): Stream<O> {
    const lastHandleMessages = this.handleMessages;
    this.handleMessages = async () => {
      let messages: Message<O>[];
      do {
        messages = await lastHandleMessages();
        messages = messages.filter(msg => test(msg.value, msg.metadata));
      } while (messages.length === 0);
      return messages;
    };
    return this;
  }
  explode<T extends ElementOf<O>>(): Stream<T> {
    return new Stream<T>(this.contexts, async () => {
      const messages = await this.handleMessages();
      //    [ [meta, [1,2]], [meta2, [3,4]] ]
      // => [ [meta, [1]], [meta, [2]], [meta2, [3]], [meta3, [4]] ]
      const newMessages = [];
      for (const {value, metadata} of messages)
        newMessages.push(
          ...(value as unknown as T[]).map(value => ({value, metadata}))
        );
      return newMessages;
    });
  }
  _concatMessages<T>(messages: Message<T>[]): Message<T[]> {
    /* keep first metadata while commit last metdata */
    const offsets = messages.reduce((acc, m) => {
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
      return acc;
    }, {} as Record<string, Offset>);
    const contexts = messages.map(m => m.metadata.contexts).flat();
    return {
      metadata: Object.assign(messages[0].metadata, {
        offsets: Object.values(offsets),
        contexts: contexts,
      }),
      value: messages.map(msg => msg.value),
    };
  }
  window<N>(option: {
    from: number;
    interval: number;
    collect: (msgs: O[]) => N;
  }): Stream<N> {
    const messageQueue: Message<O>[] = [];
    const interval = option.interval;
    const collect = option.collect;
    let from = option.from;
    let to = from + interval;
    return new Stream<N>(this.contexts, async () => {
      if (
        messageQueue.length > 0 &&
        messageQueue[messageQueue.length - 1].metadata.message.timestamp > to
      ) {
        this.contexts[0].logger.debug(
          `last message timestamp is larger than ${new Date(
            to
          )}. return message queue immediately`
        );
        const msg = this._concatMessages(
          messageQueue.splice(
            0,
            findLastIndex(
              messageQueue,
              (msg: Message<O>) => msg.metadata.message.timestamp < to
            ) + 1
          )
        );
        from = to;
        to = to + interval;
        return [
          {
            metadata: msg.metadata,
            value: collect(msg.value),
          },
        ];
      }
      let messages = await this.handleMessages();
      if (messages[0].metadata.message.timestamp > to) {
        this.contexts[0].logger.debug(
          `first message timestamp(${new Date(
            messages[0].metadata.message.timestamp
          )}) is larger than ${new Date(to)}. bump up window offset`
        );
      }
      while (messages[0].metadata.message.timestamp > to) {
        from = to;
        to = to + interval;
      }

      if (
        messages.length === 0 ||
        messages[messages.length - 1].metadata.message.timestamp < from
      ) {
        this.contexts[0].logger.warn(
          'last commit timestamp is too past from the window. skip all messages between the last commit ts and the start of window ts'
        );
        await Promise.all(this.contexts.map(c => c.seek(from)));
        do {
          messages = await this.handleMessages();
        } while (
          messages.length === 0 ||
          messages[messages.length - 1].metadata.message.timestamp < from
        );
      }
      messageQueue.push(
        ...messages.filter(msg => msg.metadata.message.timestamp >= from)
      );
      while (
        messageQueue.length === 0 ||
        messageQueue[messageQueue.length - 1].metadata.message.timestamp < to
      ) {
        messages = await this.handleMessages();
        messageQueue.push(...messages);
      }
      messageQueue.sort(
        (a, b) => a.metadata.message.timestamp - b.metadata.message.timestamp
      );

      const msg = this._concatMessages(
        messageQueue.splice(
          0,
          findLastIndex(
            messageQueue,
            (msg: Message<O>) => msg.metadata.message.timestamp < to
          ) + 1
        )
      );
      from = to;
      to = to + interval;
      return [
        {
          metadata: msg.metadata,
          value: collect(msg.value),
        },
      ];
    });
  }
  union(other: Stream<O>): Stream<O> {
    let messageQueue: Message<O>[] = [];
    const myHandleMessages = this.handleMessages;
    let handleMessages1IsRunning = false;
    let handleMessages2IsRunning = false;
    const handleMessages1 = async () => {
      if (handleMessages1IsRunning) throw SKIPPED;
      handleMessages1IsRunning = true;
      const res = await myHandleMessages();
      messageQueue.push(...res);
      handleMessages1IsRunning = false;
    };
    const handleMessages2 = async () => {
      if (handleMessages2IsRunning) throw SKIPPED;
      handleMessages2IsRunning = true;
      const res = await other.handleMessages();
      messageQueue.push(...res);
      handleMessages2IsRunning = false;
    };
    this.handleMessages = async () => {
      if (messageQueue.length === 0)
        await firstPromiseResolveOrSkip([handleMessages1(), handleMessages2()]);
      if (messageQueue.length > 0) {
        const messages = messageQueue;
        messageQueue = [];
        return messages;
      } else {
        return [];
      }
    };
    this.contexts.push(...other.contexts);
    return this;
  }
  blackhole(): Stream<O> {
    return new Stream(this.contexts, async () => {
      const messages = await this.handleMessages();
      while (this.contexts.every(c => !c.isDisconnected())) {
        await sleep(1000);
      }
      return messages;
    });
  }
  writeTo(topic: string): Stream<O> {
    return new Stream(this.contexts, async () => {
      const messages = await this.handleMessages();
      if (messages && messages.length > 0)
        await this.contexts[0].send(topic, messages);
      return messages;
    });
  }
  flushStatistics(): Statistics {
    return this.contexts
      .map(c => c.flushStatistics())
      .reduce((acc, s) => acc.merge(s));
  }
  async start(): Promise<Stream<O>> {
    await Promise.all(this.contexts.map(c => c.start()));
    while (this.contexts.every(c => !c.isDisconnected())) {
      await this.handleMessages();
    }
    return this;
  }
  async stop(): Promise<void> {
    await Promise.all(this.contexts.map(c => c.disconnect()));
  }
}

export function createStream<T>(option: StreamContextOption) {
  const context = new StreamContext(option);
  return new Stream<T>([context]);
}
