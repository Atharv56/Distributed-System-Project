# Deterministic Miller–Rabin for 64-bit
def _mr_decompose(n: int):
    d, s = n - 1, 0
    while d & 1 == 0:
        d >>= 1; s += 1
    return d, s

def _mr_try(a: int, d: int, s: int, n: int) -> bool:
    x = pow(a, d, n)
    if x == 1 or x == n-1:
        return True
    for _ in range(s-1):
        x = (x * x) % n
        if x == n-1:
            return True
    return False

def is_prime_64(n: int) -> bool:
    if n < 2: return False
    small = [2,3,5,7,11,13,17,19,23,29,31,37]
    for p in small:
        if n % p == 0:
            return n == p
    d, s = _mr_decompose(n)
    for a in [2,3,5,7,11,13,17]:
        if a % n == 0: 
            return True
        if not _mr_try(a, d, s, n):
            return False
    return True
