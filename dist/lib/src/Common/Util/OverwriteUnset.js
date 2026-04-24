export function OverwriteUnset(target, source) {
    for (const s_key in source) {
        const key = s_key;
        if (target[key] == null) {
            target[key] = source[key];
        }
    }
    return target;
}
