
CRUSH_HASH_SEED = 1315423911

def crush_hashmix(a: int, b: int, c: int): 
    a = a-b;  a = a-c;  a = a^(c>>13);
    b = b-c;  b = b-a;  b = b^(a<<8); 
    c = c-a;  c = c-b;  c = c^(b>>13);
    a = a-b;  a = a-c;  a = a^(c>>12);
    b = b-c;  b = b-a;  b = b^(a<<16);
    c = c-a;  c = c-b;  c = c^(b>>5); 
    a = a-b;  a = a-c;  a = a^(c>>3); 
    b = b-c;  b = b-a;  b = b^(a<<10);
    c = c-a;  c = c-b;  c = c^(b>>15);
    return a, b, c
    

def crush_hash_2(a: int, b: int):
    h = CRUSH_HASH_SEED ^ a
    b = a
    x = 231232;
    y = 1232;

    b, x, h = crush_hashmix(b, x, h);
    y, a, h = crush_hashmix(y, a, h);
    return h
    
