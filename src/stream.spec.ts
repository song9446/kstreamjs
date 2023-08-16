import {StreamContext, Message, Statistics} from './context.js';
import {createStream} from './stream.js';

function mockMessages<T>(
  values: T[],
  contexts: StreamContext[],
  timestampes: number[] = []
): Message<T>[] {
  return values.map((value, i) => ({
    metadata: {
      pause: () => {
        return () => {};
      },
      topic: '',
      partition: 0,
      heartbeat: async () => {},
      contexts: contexts,
      message: {
        timestamp: timestampes[i] || 0,
        key: Buffer.from('', 'utf8'),
        size: 0,
        attributes: 0,
        offset: '0',
      },
    },
    value,
  }));
}

describe('with mock kafka context', () => {
  let mockData: Message<unknown>[] = [];
  beforeAll(() => {
    jest.mock('./context');
    StreamContext.prototype.receive = jest.fn().mockImplementation(async () => {
      return mockData.splice(0, 1);
    });
    StreamContext.prototype.flushStatistics = jest
      .fn()
      .mockImplementation(() => {
        const s = new Statistics();
        s.recvTotal = 1;
        s.sendTotal = 1;
        return s;
      });
  });
  afterAll(() => {
    jest.clearAllMocks();
  });
  function createStreamHelper<T>(topic = 'in-topic') {
    return createStream<T>({
      brokers: ['a'],
      inputTopic: topic,
      groupId: 'group-id',
    });
  }
  it('creates identical stream', async () => {
    const stream = createStreamHelper();
    mockData = mockMessages([{a: 1}], stream.contexts);
    const msgs = await stream.handleMessages();
    const expected = [{value: {a: 1}}];
    expect(msgs).toMatchObject(expected);
  });
  it('maps stream', async () => {
    const data = {a: 1};
    const stream = createStreamHelper<typeof data>().map(i => ({
      a: i['a'] + 1,
    }));
    mockData = mockMessages([data], stream.contexts);
    const msgs = await stream.handleMessages();
    const expected = [{value: {a: 2}}];
    expect(msgs).toMatchObject(expected);
  });
  it('async maps stream', async () => {
    const data = {a: 1};
    const stream = createStreamHelper<typeof data>().mapAsync(async i => ({
      a: i['a'] + 1,
    }));
    mockData = mockMessages([data], stream.contexts);
    const msgs = await stream.handleMessages();
    const expected = [{value: {a: 2}}];
    expect(msgs).toMatchObject(expected);
  });

  it('filter stream', async () => {
    const data = [{a: 1}, {a: 2}, {a: 3}, {a: 1}];
    const stream = createStreamHelper<(typeof data)[number]>().filter(
      i => i['a'] > 2
    );
    mockData = mockMessages(data, stream.contexts);
    const msgs = await stream.handleMessages();
    const expected = [{value: {a: 3}}];
    expect(msgs).toMatchObject(expected);
  });

  it('explode stream', async () => {
    const stream = createStreamHelper<typeof mockData>().explode();
    mockData = mockMessages([[{a: 1}, {b: 2}]], stream.contexts);
    const msgs = await stream.handleMessages();
    const expected = [{value: {a: 1}}, {value: {b: 2}}];
    expect(msgs).toMatchObject(expected);
  });
  it('aggregates stream by window', async () => {
    const data = [{a: 1}, {a: 5}, {a: 2}, {a: 4}, {a: 3}];
    const stream = createStreamHelper<(typeof data)[0]>().window({
      from: 0,
      interval: 2,
      bufferInterval: 10,
      collect: msgs => msgs.reduce((acc, i) => i['a'] + acc, 0),
    });
    mockData = mockMessages(data, stream.contexts, [0, 4, 1, 3, 2]);
    let msgs = await stream.handleMessages();
    let expected = [{value: 3}];
    expect(msgs).toMatchObject(expected);
    msgs = await stream.handleMessages();
    expected = [{value: 7}];
    expect(msgs).toMatchObject(expected);
  });
  it('union streams', async () => {
    const data = [1, 2, 3, 4];
    const stream2 = createStreamHelper<(typeof data)[0]>('s2').map(i => i * 10);
    const stream = createStreamHelper<(typeof data)[0]>('s1').union(stream2);
    mockData = mockMessages(data.slice(0, 2), stream.contexts);
    mockData.push(...mockMessages(data.slice(2, 4), stream2.contexts));
    const msgs: Message<number>[] = [];
    for (let i = 0; i < 4; ++i) msgs.push(...(await stream.handleMessages()));
    const expected = [1, 20, 3, 40];
    expect(msgs.map(msg => msg.value)).toStrictEqual(expected);
  });
  it('union streams with dead stream', async () => {
    const data = [1, 2, 3, 4];
    const stream2 = createStreamHelper<(typeof data)[0]>().blackhole();
    const stream = createStreamHelper<(typeof data)[0]>().union(stream2);
    mockData = mockMessages(data.slice(0, 2), stream.contexts);
    mockData.push(...mockMessages(data.slice(2, 4), stream2.contexts));
    const msgs: Message<number>[] = [];
    for (let i = 0; i < 4; ++i) msgs.push(...(await stream.handleMessages()));
    const expected = [1, 3, 4];
    expect(msgs.map(m => m.value).sort()).toEqual(expected.sort());
    await stream.stop();
  });

  it('apply mix of transform to stream', async () => {
    const data = [{a: 1}, {b: 2}, {c: 3}, {d: 4}, {e: 5}];
    const stream = createStreamHelper<(typeof data)[number]>()
      .map(msg => Object.assign(msg, {z: 1}))
      .window({
        from: 0,
        interval: 2,
        collect: msgs => {
          return msgs.map(Object.keys).flat();
        },
      })
      .explode()
      .map(k => k + 'a');
    mockData = mockMessages(data, stream.contexts, [0, 1, 2, 3, 4, 5]);
    let msgs = await stream.handleMessages();
    let expected = [{value: 'aa'}, {value: 'za'}, {value: 'ba'}, {value: 'za'}];
    expect(msgs).toMatchObject(expected);
    msgs = await stream.handleMessages();
    expected = [{value: 'ca'}, {value: 'za'}, {value: 'da'}, {value: 'za'}];
    expect(msgs).toMatchObject(expected);
  });

  it('flush statistics', async () => {
    const stream1 = createStreamHelper();
    const stream2 = createStreamHelper();
    mockData = mockMessages([1, 2, 3, 4].slice(0, 2), stream1.contexts);
    mockData.push(...mockMessages([1, 2, 3, 4].slice(2, 4), stream2.contexts));
    const stats = stream1.union(stream2).flushStatistics();
    expect(stats).toMatchObject({
      recvTotal: 2,
      sendTotal: 2,
    });
  });
});
