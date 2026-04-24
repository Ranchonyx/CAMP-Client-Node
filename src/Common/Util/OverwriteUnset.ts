export function OverwriteUnset<T extends Record<string, any>, U extends Record<string, any>>(target: T, source: U): T & Required<U> {
    for(const s_key in source) {
        const key = s_key as keyof U;
        if(target[key as keyof T] == null) {
            (target as any)[key] = source[key];
        }
    }

    return target as T & Required<U>;
}