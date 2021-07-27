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

export function firstPromiseResolveOrSkip(promises: Promise<unknown>[]) {
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
