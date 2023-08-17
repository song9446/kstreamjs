export type ElementOf<T> = T extends readonly (infer ET)[] ? ET : never;

export function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

export function findLastIndex<T>(
  array: Array<T>,
  predicate: (value: T, index: number, obj: T[]) => boolean
): number {
  let l = array.length;
  while (l--) {
    if (predicate(array[l], l, array)) return l;
  }
  return -1;
}

export const SKIPPED = Symbol('Skipped');

export function firstPromiseResolveOrSkip<T>(
  promises: Promise<T>[]
): Promise<T> {
  let skippedCount = 0;
  return new Promise((res, rej) => {
    promises.forEach(p => {
      Promise.resolve(p).then(
        val => {
          res(val);
        },
        err => {
          if (err === SKIPPED) {
            skippedCount += 1;
            if (skippedCount === promises.length) rej(SKIPPED);
          } else {
            rej(err);
          }
        }
      );
    });
  });
}

export function partition<T>(
  array: T[],
  filter: (cond: T, idx: number, arr: T[]) => boolean
): [T[], T[]] {
  const pass: T[] = [];
  const fail: T[] = [];
  array.forEach((e, idx, arr) => (filter(e, idx, arr) ? pass : fail).push(e));
  return [pass, fail];
}

export function isAsync<T, U extends unknown[]>(
  fn: ((...args: U) => T) | ((...args: U) => Promise<T>)
): fn is (...args: U) => Promise<T> {
  return fn.constructor.name === 'AsyncFunction';
}
