function reduce(key, values, context) {
    reduceOutKey.set(key);
    reduceOutValue.set(1);
    context.write(reduceOutKey, reduceOutValue);
}