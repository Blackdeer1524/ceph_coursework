from ctypes import c_uint32, c_int64
from crush_ln_table import RH_LH_tbl, LL_tbl

CRUSH_HASH_SEED = 1315423911

def crush_hashmix(a: int, b: int, c: int) -> tuple[int, int, int]: 
    a = a-b;  a = a-c;  a = c_uint32(a ^ (c>>13)).value;
    b = b-c;  b = b-a;  b = c_uint32(b ^ (a<<+8)).value; 
    c = c-a;  c = c-b;  c = c_uint32(c ^ (b>>13)).value;
    a = a-b;  a = a-c;  a = c_uint32(a ^ (c>>12)).value;
    b = b-c;  b = b-a;  b = c_uint32(b ^ (a<<16)).value;
    c = c-a;  c = c-b;  c = c_uint32(c ^ (b>>+5)).value; 
    a = a-b;  a = a-c;  a = c_uint32(a ^ (c>>+3)).value; 
    b = b-c;  b = b-a;  b = c_uint32(b ^ (a<<10)).value;
    c = c-a;  c = c-b;  c = c_uint32(c ^ (b>>15)).value;
    return a, b, c
    

def crush_hash_2(a: int, b: int) -> int:
    h = CRUSH_HASH_SEED ^ a ^ b
    x = 231232;
    y = 1232;
    a, b, h = crush_hashmix(a, b, h);
    x, a, h = crush_hashmix(x, a, h);
    b, y, h = crush_hashmix(b, y, h);
    return h


def crush_hash32_3(a: int, b: int, c: int) -> int:
    h = CRUSH_HASH_SEED ^ a ^ b ^ c;
    x = 231232;
    y = 1232;
    a, b, h = crush_hashmix(a, b, h);
    c, x, h = crush_hashmix(c, x, h);
    y, a, h=  crush_hashmix(y, a, h);
    b, x, h = crush_hashmix(b, x, h);
    y, c, h = crush_hashmix(y, c, h);
    return h


def __builtin_clz(x: int) -> int:
    c = 0
    for i in range(31, -1, -1):
        if (x >> i) & 1 == 0:
            c += 1
        else:
            break
    return c


def crush_ln(xin: int) -> int:
    # uint64 RH, LH, LL, xl64, result;
    x = xin + 1 

    # /* normalize input */
    iexpon = 15;

    # // figure out number of bits we need to shift and
    # // do it in one step instead of iteratively
    if (not (x & 0x18000)):
        bits = __builtin_clz(x & 0x1FFFF) - 16;
        x <<= bits;
        iexpon = 15 - bits;

    index1 = (x >> 8) << 1;
    # /* RH ~ 2^56/index1 */
    RH = RH_LH_tbl[c_uint32(index1 - 256).value];
    # /* LH ~ 2^48 * log2(index1/256) */
    LH = RH_LH_tbl[c_uint32(index1 + 1 - 256).value];

    # /* RH*x ~ 2^48 * (2^15 + xf), xf<2^8 */
    xl64 = c_int64( x * RH).value;
    xl64 >>= 48;

    result = iexpon;
    result <<= (12 + 32);

    index2 = xl64 & 0xff;
    # /* LL ~ 2^48*log2(1.0+index2/2^15) */
    LL = LL_tbl[index2];

    LH = LH + LL;

    LH >>= (48 - 12 - 32);
    result += LH;

    return result;
