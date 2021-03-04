#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/mman.h>
#include <string.h>
#include <fcntl.h>

#define PATH "/mnt/pmem/pmem-file"
#define SIZE 4096

int main(int argc, char *argv[]) {
  //open the pmem file
  int fd = open(PATH, O_RDWR);
  if (fd < 0) {
    perror("failed to open the file");
    exit(1);
  }

  //mmap the pmem file
  void *addr = mmap(NULL, SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (addr == MAP_FAILED) {
    perror("failed to mmap the file");
    exit(1);
  }

  //we now hava the memory region start from "addr" with the offset "size"

  //use parts of the memory region as a buf
  char *pmem_buf = addr + (1 << 5);
  const char* str = "hello,  persistent memory";
  strcpy(pmem_buf, str);
  printf("pmem_str: %s", pmem_buf);

  if (munmap(addr, SIZE) != 0) {
    perror("failed to unmap the file");
    exit(1);
  }
  close(fd);
  return 0;
}
