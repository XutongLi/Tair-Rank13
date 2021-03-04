#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <libpmem.h>

/* using 4k of pmem for this example */
#define PMEM_LEN 4096

#define PATH "/mnt/pmem/pmem-file"

int main(int argc, char *argv[])
{
  char *pmemaddr;
  size_t mapped_len;
  int is_pmem;

  /* create a pmem file and memory map it */

  if ((pmemaddr = pmem_map_file(PATH, PMEM_LEN, PMEM_FILE_CREATE,
          0666, &mapped_len, &is_pmem)) == NULL) {
    perror("pmem_map_file");
    exit(1);
  }

  /* store a string to the persistent memory */
  strcpy(pmemaddr, "hello, persistent memory");
  printf("pmem_str: %s", pmemaddr);

  /* flush above strcpy to persistence */
  if (is_pmem)
    pmem_persist(pmemaddr, mapped_len);
  else
    pmem_msync(pmemaddr, mapped_len);

  /*
   * Delete the mappings. The region is also
   * automatically unmapped when the process is
   * terminated.
   **/
  pmem_unmap(pmemaddr, mapped_len);
}
