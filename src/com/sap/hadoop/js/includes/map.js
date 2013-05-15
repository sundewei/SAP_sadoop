function map(key, value, context) {
    mapOutKey.set(value);
    mapOutValue.set(1);
    context.write(mapOutKey, mapOutValue);
}