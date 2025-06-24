// Matthew Du    mhd210005
// Ashley Nguyen atn210009

#include "common.h"
#include "common_threads.h"
#include <errno.h>            // EINTR
#include <fcntl.h>            // open
#include <inttypes.h>         // uint
#include <stdint.h>
#include <stdio.h>            // printf, sprintf, NULL
#include <stdlib.h>           // strtoul
#include <string.h>           // strlen
#include <sys/mman.h>         // MAP_PRIVATE
#include <sys/stat.h>         // fstat
#include <sys/types.h>        // pthread_t
#include <unistd.h>

// Function declarations
void Usage(char *);
void* tree(void *);
uint64_t* returnHash(void *);
uint32_t jenkins_one_at_a_time_hash(const uint8_t *, uint64_t);

// block size
#define BSIZE 4096

// struct holding thread data
typedef struct threadArgs {
  uint64_t nodeNum;
  uint64_t mThreads;
  uint64_t nBlocks;
  uint32_t *ptr;
} tArgs;

int main(int argc, char **argv) {
  // declare variables for file input, hash value, and hash tree
  int32_t fd;
  struct stat buffer;
  uint32_t nblocks, hash,*arr;
  pthread_t htree;
  tArgs args;
  args.nodeNum = 0;
  void* tmp = NULL;

  // input checking
  if (argc != 3)
    Usage(argv[0]);

  // open input file, check for errors
  fd = open(argv[1], O_RDWR);
  if (fd == -1) {
    perror("open failed");
    exit(EXIT_FAILURE);
  }
  // use fstat to get file size
  if (fstat(fd, &buffer) == -1) {
    perror("fstat failed");
    exit(EXIT_FAILURE);
  }
  // calculate nblocks
  nblocks = buffer.st_size / BSIZE;

  // call mmap to map the file to memory
  arr = mmap(NULL, buffer.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
  if (arr == MAP_FAILED) {
    perror("mmap failed");
    exit(EXIT_FAILURE);
  }
  // store number of threads, blocks, file starting position, and assigned blocks per thread in struct
  args.mThreads = atoi(argv[2]);
  args.nBlocks = nblocks;
  args.ptr = arr;
  printf("num Threads = %" PRIu64 "\n", args.mThreads);
  printf("Blocks per Thread = %" PRIu64 "\n", nblocks / args.mThreads);

  // record process start time
  double start = GetTime();

  // create hash tree recursively using tree function
  Pthread_create(&htree, NULL, tree, &args);
  // calculate hash value of the input file (done in tree), tmp holds the final hash value returned from the tree of threads
  Pthread_join(htree, &tmp);
  hash = *(uint32_t*)tmp;
  // record process end time
  double end = GetTime();

  // print hash value and processing time
  printf("hash value = %u \n", hash);
  printf("time taken = %f \n", (end - start));
  // close file and exit program
  close(fd);
  return EXIT_SUCCESS;
}

/*
tree hashing process:
- leaf thread(s) will return hash values to its parent
- leaf thread parent will have computed its hash value of assigned blocks, then concatenates leaf thread hash to their hash string, then it will rehash the concatenated string and return that
- process will repeat recursively until level 1 hash values concatenated to root thread 0 and final hash value returned to main
*/
void *tree(void *arg) {
  // thread variables for each possible child
  pthread_t p1 = 0, p2 = 0;
  // void variables to store return value of child hash value(s)
  void *ret1 = NULL, *ret2 = NULL;
  // tArgs variables to store information of child thread(s)
  tArgs args = *(tArgs *)arg;
  tArgs args2 = *(tArgs *)arg;
  // variables storing current (parent) thread number and starting block
  uint64_t curNode = args.nodeNum;
  uint64_t conBlocks = (args.nBlocks / args.mThreads) * curNode;
  // loop counter variable used to assign blocks to thread
  uint64_t i;
  // file offset calculated based on starting block position
  off_t offset = conBlocks * BSIZE;
  // variable to store hash value
  uint64_t *hashVal = NULL;
  // variables used to assign blocks to thread
  unsigned char *blocks, *startBlock;
  // variable used to store converted hash value to string
  char *hashToString;

  // thread starting block calculated by adding offset to file starting position in memory
  startBlock = (unsigned char*)args.ptr + offset;

  // allocate memory for thread's assigned blocks
  blocks = (unsigned char*)malloc(BSIZE * (args.nBlocks / args.mThreads) * sizeof(unsigned char));
  // assign the blocks being hashed for the thread to blocks
  for(i = 0; i < BSIZE * (args.nBlocks / args.mThreads); i++) {
    blocks[i] = startBlock[i];
  }
  // check if current thread number is less than number of threads (not last thread)
  if (curNode < args.mThreads) {
    // check if thread not last hash tree thread
    args.nodeNum = ((2*curNode) + 1);
    if (args.nodeNum < args.mThreads) {
      // create left child thread
      Pthread_create(&p1, NULL, tree, &args);
      // check if thread not last hash tree thread
      args2.nodeNum = ((2*curNode) + 2);
      if (args2.nodeNum < args2.mThreads) {
        // create right child thread
        Pthread_create(&p2, NULL, tree, &args2);
        // calculate hash value of current (parent) thread
        hashVal = returnHash(blocks);
        // make parent thread wait until child threads terminate
        Pthread_join(p1, &ret1);
        Pthread_join(p2, &ret2);
        // allocating memory in hashToString to be able to hold the concatenated hash values
        hashToString = (char*)malloc(sizeof(uint32_t)*9 + 1);
        // concatenate children to current (parent) thread hash value and store in string
        sprintf(hashToString, "%u%u%u", *(uint32_t*)hashVal, *(uint32_t*)ret1, *(uint32_t*)ret2);
        // rehash the combined string and store it in hashVal
        hashVal = returnHash(hashToString);
        // terminate thread and return hashVal to parent thread or to main if it is the root thread
        pthread_exit((uint32_t*)hashVal);
      }
      // if parent only has left child
      else {
        // calculate hash value of current (parent) thread
        hashVal = returnHash(blocks);
        // make parent thread wait until left child thread terminates
        Pthread_join(p1, &ret1);
        // allocating memory in hashToString to be able to hold the concatenated hash values
        hashToString = (char*)malloc(sizeof(uint32_t)*6 + 1);
        // convert parent hash value to string while also concatenating the left child
        sprintf(hashToString, "%u%u", *(uint32_t*)hashVal, *(uint32_t*)ret1);
        // rehash the combined string and store it in hashVal
        hashVal = returnHash(hashToString);
        // terminate thread and return hashVal to parent thread
        pthread_exit((uint32_t*)hashVal);
      }
    }
    // thread has no children, return hash value and terminate thread
    else {
      // calculate hash value of current thread
      hashVal = returnHash(blocks);
      // terminate thread and return hashVal to parent thread or main if it is the root (and only) thread
      pthread_exit((uint32_t*)hashVal);
    }
  }
  // terminate thread
  pthread_exit(NULL);
  return NULL;
}

// returns hash value of thread's assigned block
uint64_t* returnHash(void *arg) {
  // holds starting address of blocks
  char *a = (char *)arg;
  // pointer to hash value
  uint64_t *ret;
  // allocate memory for unsigned 32-bit integer
  ret = malloc(sizeof(uint64_t));
  // set pointer value to hash value
  *ret = (uint64_t)(jenkins_one_at_a_time_hash((uint8_t *)a, (uint64_t)strlen(a)));
  // return pointer to hash value
  return ret;
}

// Jenkins hash function
uint32_t jenkins_one_at_a_time_hash(const uint8_t *key, uint64_t length) {
  uint64_t i = 0;
  uint32_t hash = 0;

  while (i != length) {
    hash += key[i++];
    hash += hash << 10;
    hash ^= hash >> 6;
  }
  hash += hash << 3;
  hash ^= hash >> 11;
  hash += hash << 15;
  return hash;
}

// if incorrect number of arguments, print correct usage of the program and exit
void Usage(char *s) {
  fprintf(stderr, "Usage: %s filename num_threads \n", s);
  exit(EXIT_FAILURE);
}