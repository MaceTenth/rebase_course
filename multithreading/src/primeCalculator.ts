const TRIAL_DIVISION_THRESHOLD = 10000;

function powerBigInt(base: bigint, exp: bigint, mod: bigint): bigint {
    let res = 1n;
    base %= mod;
    while (exp > 0n) {
        if (exp % 2n === 1n) res = (res * base) % mod;
        base = (base * base) % mod;
        exp /= 2n;
    }
    return res;
}

function millerTestBigInt(d: bigint, n: bigint, a: bigint): boolean {
    let x = powerBigInt(a, d, n);
    if (x === 1n || x === n - 1n) return true;

    let current_d = d;
    while (current_d * 2n <= n - 1n) {
        current_d *= 2n;
        x = (x * x) % n;
        if (x === 1n) return false;
        if (x === n - 1n) return true;
    }
    return false;
}

export function isPrime(num: number): boolean {
    if (num <= 1) return false;
    if (num <= 3) return true;
    if (num % 2 === 0 || num % 3 === 0) return false;

    if (num < TRIAL_DIVISION_THRESHOLD) {
        if (num < 25) {
            for (let i = 5; i * i <= num; i = i + 6) {
                if (num % i === 0 || num % (i + 2) === 0) return false;
            }
            return true;
        }

        for (let i = 5; i * i <= num; i = i + 6) {
            if (num % i === 0 || num % (i + 2) === 0) return false;
        }
        return true;
    }

    const n_bigint = BigInt(num);
    let d = n_bigint - 1n;
    while (d % 2n === 0n) {
        d /= 2n;
    }

    const bases_for_64bit = [2, 325, 9375, 28178, 450775, 9780504, 1795265022];

    for (const base_num of bases_for_64bit) {
        const a = BigInt(base_num);
        if (n_bigint === a) return true;
        if (a >= n_bigint - 1n) continue;
        if (!millerTestBigInt(d, n_bigint, a)) {
            return false;
        }
    }

    return true;
}