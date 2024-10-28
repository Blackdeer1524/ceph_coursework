from ctypes import c_uint32 

CRUSH_HASH_SEED = 1315423911

def crush_hashmix(a: int, b: int, c: int): 
    a = a-b;  a = a-c;  a = c_uint32(c_uint32(a).value ^ (c_uint32(c).value>>13)).value;
    b = b-c;  b = b-a;  b = c_uint32(c_uint32(b).value^(c_uint32(a).value<<8)).value; 
    c = c-a;  c = c-b;  c = c_uint32(c_uint32(c).value^(c_uint32(b).value>>13)).value;
    a = a-b;  a = a-c;  a = c_uint32(c_uint32(a).value^(c_uint32(c).value>>12)).value;
    b = b-c;  b = b-a;  b = c_uint32(c_uint32(b).value^(c_uint32(a).value<<16)).value;
    c = c-a;  c = c-b;  c = c_uint32(c_uint32(c).value^(c_uint32(b).value>>5)).value; 
    a = a-b;  a = a-c;  a = c_uint32(c_uint32(a).value^(c_uint32(c).value>>3)).value; 
    b = b-c;  b = b-a;  b = c_uint32(c_uint32(b).value^(c_uint32(a).value<<10)).value;
    c = c-a;  c = c-b;  c = c_uint32(c_uint32(c).value^(c_uint32(b).value>>15)).value;
    return a, b, c
    

def crush_hash_2(a: int, b: int):
    h = CRUSH_HASH_SEED ^ a ^ b
    x = 231232;
    y = 1232;
    a, b, h = crush_hashmix(a, b, h);
    x, a, h = crush_hashmix(x, a, h);
    b, y, h = crush_hashmix(b, y, h);
    return h
    
