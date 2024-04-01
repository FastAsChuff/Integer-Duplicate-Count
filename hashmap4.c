#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdbool.h>
#include <assert.h>
#include <time.h>
#include <pthread.h>

// gcc hashmap4.c -o hashmap4.bin -O3 -march=native -Wall -pthread

// See https://gcc.gnu.org/onlinedocs/gcc/Other-Builtins.html
#define BUILTIN_PREFETCH_ARG2_FOR_READ 0
#define BUILTIN_PREFETCH_ARG2_FOR_WRITE 1
#define BUILTIN_PREFETCH_ARG3_TEMPORAL_LOCALITY_LOWEST 0 // Don't keep in cache for long
#define BUILTIN_PREFETCH_ARG3_TEMPORAL_LOCALITY_LOW 1
#define BUILTIN_PREFETCH_ARG3_TEMPORAL_LOCALITY_MEDIUM 2
#define BUILTIN_PREFETCH_ARG3_TEMPORAL_LOCALITY_HIGH 3 // Keep in cache for long time
// e.g. __builin_prefetch(ptr, BUILTIN_PREFETCH_ARG2_FOR_WRITE, BUILTIN_PREFETCH_ARG3_TEMPORAL_LOCALITY_LOW);

#define HASHMAP_NOTSET 0xffffffffffffffffULL
#define HASHMAP_FACTOR 2
#define HASHMAP_MEMALIGN 2097152
#define HASHMAP_BUFSIZE 100000
#define MIN(x,y) ((x) < (y) ? (x) : (y))
// LOADINTS_INT_TYPE must allow value (10*LOADINTS_INT_TYPE_LIMIT)+9 
#define LOADINTS_INT_TYPE_LIMIT_UINT64 1844674407370955160ULL
#define LOADINTS_INT_TYPE_LIMIT_INT64 922337203685477579ULL
#define LOADINTS_INT_FORMAT_INT64 "%lli"
#define LOADINTS_INT_FORMAT_UINT64 "%llu"
#define LOADINTS_BUFSIZE 100000
#define LOADINTS_FREADSIZE 10000

#define LOADINTS_INT_TYPE int64_t
#define LOADINTS_INT_TYPE_LIMIT LOADINTS_INT_TYPE_LIMIT_INT64
#define LOADINTS_INT_FORMAT LOADINTS_INT_FORMAT_INT64
#define LOADINTS_INT_TYPE_MAX ((LOADINTS_INT_TYPE_LIMIT*10) + 9)
//#define LOADINTS_USE_FSCANF 

typedef struct {
  uint64_t count;
  uint64_t arraysize;
  int64_t *array;
} hashmap_t;

#ifdef LOADINTS_USE_FSCANF
static inline uint64_t nextintsbuffer(FILE *fp, LOADINTS_INT_TYPE *ints, uint64_t buffixmax) {  
  uint64_t buffix = 0;
  char format[8];
  sprintf(format, "%s", LOADINTS_INT_FORMAT);
  while (buffix < buffixmax) {
    if (fscanf(fp, format, &ints[buffix]) == 1) {
      buffix++;
    } else {
      break;
    }
  }
  return buffix;
}
#else
static inline uint64_t nextintsbuffer(FILE *fp, LOADINTS_INT_TYPE *ints, uint64_t buffixmax) {  
  char readbuf[LOADINTS_FREADSIZE+1];
  readbuf[LOADINTS_FREADSIZE] = 0;
  uint64_t buffix = 0;
  uint64_t readbuffix = 0;
  int64_t readbuffintstartix = 0;
  int32_t thisintsign = 1;
  int64_t thisint = 0;
  _Bool voidint = true;
  size_t bytes = fread(readbuf, 1, LOADINTS_FREADSIZE, fp);
  while ((buffix < buffixmax) && (bytes > 0)) {
    readbuffix = 0;
    readbuffintstartix = 0;
    thisintsign = 1;
    thisint = 0;
    voidint = true;
    while ((readbuffix < bytes) && (buffix < buffixmax)) {
      if (readbuf[readbuffix] == '-') {
        thisintsign = -1;
        voidint = false;
        readbuffintstartix = readbuffix;
      } else {
        if ((readbuf[readbuffix] >= '0') && (readbuf[readbuffix] <= '9') && (thisint <= LOADINTS_INT_TYPE_LIMIT)) {      
          thisint *= 10;
          thisint += readbuf[readbuffix] - 0x30;
          if (voidint) readbuffintstartix = readbuffix;
          voidint = false;
        } else {
          if (!voidint) {
            ints[buffix] = thisintsign*thisint;
            buffix++;
          }
          thisintsign = 1;
          thisint = 0;
          voidint = true;
        } 
      }
      readbuffix++;
    }
    if (buffix >= buffixmax) {
      fseek(fp, readbuffix-bytes, SEEK_CUR);
      return buffix;
    }
    if (!voidint) {
      if (readbuffintstartix == 0) return buffix;
      if (feof(fp))  {
        ints[buffix] = thisintsign*thisint;
        buffix++;
        return buffix;
      }
      fseek(fp, readbuffintstartix-bytes, SEEK_CUR);
    }
    bytes = fread(readbuf, 1, LOADINTS_FREADSIZE, fp);
  }
  return buffix;
}
#endif

hashmap_t *hashmapnew(uint64_t size) {
  if (size == 0) return NULL;  
  hashmap_t *hashmap = malloc(sizeof(hashmap_t));
  if (hashmap == NULL) return NULL;  
  hashmap->arraysize = HASHMAP_FACTOR*size;
  hashmap->count = 0;
  
  if (size <= 1000000) { 
    hashmap->array = malloc(hashmap->arraysize*sizeof(int64_t));
  } else {
    hashmap->array = aligned_alloc(HASHMAP_MEMALIGN, hashmap->arraysize*sizeof(int64_t));
  }
  if (hashmap->array == NULL) {
    free(hashmap);
    return NULL;
  }
  memset(hashmap->array, 0xff, hashmap->arraysize*sizeof(int64_t));
  return hashmap;
}

static inline uint64_t hash(hashmap_t hashmap, uint64_t input) {
  uint64_t A = 0x34c1e258e25845d1;
  uint64_t B = 0x146634c134c1e258;
  uint64_t E = 0;
  uint64_t T = 0;
  uint64_t K64 = 0;
  uint32_t i, keylen;
  char*key = (char*)&input;
  keylen=sizeof(input);
  for (i=0; i<keylen; i++) {
    K64 = (uint64_t)key[i] + (uint64_t)key[keylen - 1 - i];
    E += ((A*K64) + (B^K64) + B);
    T = 1 + (E & 0x1f);
    E = (E >> T) | (E << (64 - T));
  }
  return E % hashmap.arraysize;
};

void hashmapfree(hashmap_t **hashmap) {
  free((*hashmap)->array);
  free(*hashmap);
  *hashmap = NULL;
}

void maketestfile(char *filename, int64_t size) {
  if (size < 1) return;
  FILE *fp = fopen(filename, "w");
  if (fp == NULL) return;
  for (int64_t i=0; i<size; i++) {
    //fprintf(fp, "%lli ", ((((int64_t)rand() << 30) + rand()) % 1999999999999998ULL) - 999999999999999ULL); // 15 digits
    fprintf(fp, "%lli ", ((((int64_t)rand() << 30) + rand()) % 19999999998ULL) - 9999999999ULL); // 10 digits
    //fprintf(fp, "%lli ", ((((int64_t)rand() << 30) + rand()) % 19999998ULL) - 9999999ULL); // 7 digits
  }
  fclose(fp);
}

_Bool hashmapinsert(hashmap_t *hashmap, int64_t arrval) {
  if (arrval == HASHMAP_NOTSET) return false;
  if (hashmap->arraysize <= hashmap->count) return false;
  uint64_t arrix = hash(*hashmap, arrval);
  while ((hashmap->array[arrix] != HASHMAP_NOTSET) && (hashmap->array[arrix] != arrval)) {
    arrix = (arrix == hashmap->arraysize-1 ? 0 : arrix+1);
  }
  if (hashmap->array[arrix] == arrval) return false;
  hashmap->array[arrix] = arrval;
  hashmap->count++;
  return true;
}

_Bool hashmapselect(hashmap_t *hashmap, int64_t arrval, uint64_t *ix) {
  if (arrval == HASHMAP_NOTSET) return false;
  uint64_t arrix = hash(*hashmap, arrval);
  while ((hashmap->array[arrix] != HASHMAP_NOTSET) && (hashmap->array[arrix] != arrval)) {
    arrix = (arrix == hashmap->arraysize-1 ? 0 : arrix+1);
  }
  if (hashmap->array[arrix] == arrval) {
    *ix = arrix;
    return true;
  }
  return false;
}

typedef struct {
  hashmap_t *hashmap;
  int64_t *vals;
  uint64_t *hashes;
  int64_t valscount;
  int64_t dupcount;
} hashmapbulkinsertargs_t;

void *hashmapbulkinsert(void *input) {
  uint64_t prevarrix, arrix;
  hashmapbulkinsertargs_t *args = input;
  hashmap_t *hashmap = args->hashmap;
  int64_t prevval, val, ix = 0;
  while (ix < args->valscount) {
    prevval = args->vals[ix];
    val = args->vals[ix+1];
    if (prevval != HASHMAP_NOTSET) {
      prevarrix = args->hashes[ix];
      __builtin_prefetch(&hashmap->array[prevarrix], BUILTIN_PREFETCH_ARG2_FOR_WRITE, BUILTIN_PREFETCH_ARG3_TEMPORAL_LOCALITY_HIGH);
    } 
    if (val != HASHMAP_NOTSET) {
      arrix = args->hashes[ix+1];
      while ((hashmap->array[arrix] != HASHMAP_NOTSET) && (hashmap->array[arrix] != val)) {
        arrix = (arrix == hashmap->arraysize-1 ? 0 : arrix+1);
      }
      if (hashmap->array[arrix] != val) {
        hashmap->array[arrix] = val;
        hashmap->count++;  
      } else {
        args->dupcount++;
      }
    }
    if (prevval != HASHMAP_NOTSET) {
      while ((hashmap->array[prevarrix] != HASHMAP_NOTSET) && (hashmap->array[prevarrix] != prevval)) {
        prevarrix = (prevarrix == hashmap->arraysize-1 ? 0 : prevarrix+1);
      }
      if (hashmap->array[prevarrix] != prevval) {
        hashmap->array[prevarrix] = prevval;
        hashmap->count++;  
      } else {
        args->dupcount++;
      }
    } 
    ix += 2;
  }
  return NULL;
}

int main(int argc, char*argv[]) {
  if (argc == 4) {
    if (strcmp(argv[3], "test") == 0) {
      maketestfile(argv[1], atol(argv[2]));
      exit(0);
    }
  }
  if (argc != 3) {
    printf("This program reads a file of n integers -2^63 <= a_i < 2^63 - 1 and inserts them into a hash map.\n");
    printf("A count of integers which have already appeared earlier in the list is output.\n");
    printf("Author: Simon Goater Mar 2024.\n");
    printf("Usage: %s filename n\n", argv[0]);
    printf("1 <= n < 2^32\n");
    printf("Make a test file of n random 10 digit integers with %s filename n test\n", argv[0]);
    exit(0);
  }
  FILE *fp = fopen(argv[1], "r");
  assert(fp != NULL);
  int64_t n = 0;
  uint64_t intscount = 0;
  n = atol(argv[2]);
  assert(n > 0);
  assert(n <= 0xffffffff);
  hashmap_t *hashmap = hashmapnew(n);
  assert(hashmap != NULL);
  int64_t prevval;  
  if ((n % 2) == 1) {
    assert(fscanf(fp, "%li ", &prevval) != EOF);
    assert(hashmapinsert(hashmap, prevval));
    intscount = 1;
  }
  LOADINTS_INT_TYPE *intsA = malloc(LOADINTS_BUFSIZE*sizeof(LOADINTS_INT_TYPE));
  LOADINTS_INT_TYPE *intsB = malloc(LOADINTS_BUFSIZE*sizeof(LOADINTS_INT_TYPE));
  uint64_t *hashesA = malloc(HASHMAP_BUFSIZE*sizeof(uint64_t));
  uint64_t *hashesB = malloc(HASHMAP_BUFSIZE*sizeof(uint64_t));
  LOADINTS_INT_TYPE *ints = intsA;
  uint64_t *hashes = hashesA;
  uint64_t buffix = nextintsbuffer(fp, ints, MIN(LOADINTS_BUFSIZE, n));
  for (uint64_t i=0; i<buffix; i++) hashes[i] = hash(*hashmap, ints[i]);
  intscount += buffix;
  pthread_t hashmapthread;
  hashmapbulkinsertargs_t hashmapbulkinsertargs;
  hashmapbulkinsertargs.hashmap = hashmap;
  hashmapbulkinsertargs.dupcount = 0;
  while (buffix > 0) {
    hashmapbulkinsertargs.vals = ints;
    hashmapbulkinsertargs.hashes = hashes;
    hashmapbulkinsertargs.valscount = buffix;
    pthread_create(&hashmapthread, NULL, hashmapbulkinsert, (void*)&hashmapbulkinsertargs); 
    if (ints == intsA) {
      ints = intsB;
      hashes = hashesB;
    } else {
      ints = intsA;
      hashes = hashesA;
    }
    buffix = nextintsbuffer(fp, ints, MIN(LOADINTS_BUFSIZE, n-intscount));
    for (uint64_t i=0; i<buffix; i++) hashes[i] = hash(*hashmap, ints[i]);
    intscount += buffix;
    pthread_join(hashmapthread, NULL); 
  }
  printf("%li duplicates found from %li integers.\n", hashmapbulkinsertargs.dupcount, intscount);
  hashmapfree(&hashmap);
  free(intsA);
  free(intsB);
  fclose(fp);
}
// ~17million inserts/s 3.2GHz Ivy Bridge Xeon once input file is cached.
