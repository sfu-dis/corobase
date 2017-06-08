#include "dynarray.h"

#include "sm-common.h"

#include <signal.h>
#include <setjmp.h>
#include <cerrno>
#include <cstring>
#include <cstdio>

sigjmp_buf jbuf;

extern "C" void handler(int, siginfo_t *si, void *) {
  ASSERT(si->si_code);
  siglongjmp(jbuf, si->si_code);
}

static void access(dynarray &d, size_t offset, bool should_fault) {
  fprintf(stderr, "About to make an access, should %sfault\n",
          should_fault ? "" : "not ");
#ifdef __CYGWIN__
  if (should_fault) {
    /* Our code enters an infinite loop on cygwin due to
       repeatedly retrying the same seg fault. The handler is
       never called.
     */
    fprintf(stderr,
            "   (Never mind. Skipping because Cygwin can't catch SIGSEGV.)\n");
    return;
  }
#endif
  if (int err = sigsetjmp(jbuf, 1)) {
    ASSERT(should_fault);
    ASSERT(err == SEGV_ACCERR);
    fprintf(stderr, "   Faulted, as designed\n");
  } else {
    // first time
    d[offset] = 0;
    ASSERT(not should_fault);
    fprintf(stderr, "   No fault, as designed\n");
  }
}

int main() {
  dynarray<char> dummy;

  // test movable status
  dummy = dynarray(1024 * 1024, 1);

  // not copyable
  // dynarray d2(dummy);

  // swappable
  dynarray d;
  swap(dummy, d);

  struct sigaction sa;
  memset(&sa, 0, sizeof(sa));
  sa.sa_sigaction = &handler;
  sa.sa_flags = SA_SIGINFO;
  sigemptyset(&sa.sa_mask);
  int err = sigaction(SIGSEGV, &sa, NULL);
  THROW_IF(err, os_error, errno, "Call to sigaction() failed");

  access(d, d.size() - 1, false);
  access(d, d.size(), true);

  access(d, d.size() - 1, false);
  access(d, d.size(), true);

  d.resize(2 * d.size());
  access(d, d.size() - 1, false);
  access(d, d.size(), true);

  d.truncate(d.size() / 2);
  access(d, d.size() - 1, false);
  access(d, d.size(), true);
}
