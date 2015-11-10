#include "window-buffer.h"

#include "sm-common.h"
#include "sm-exceptions.h"
#include "sm-config.h"

#include <cstring>

#ifdef __CYGWIN__
#define USE_WINAPI
#endif

#ifdef USE_WINAPI
#include <windows.h>
#else
#include <unistd.h>
#include <sys/mman.h>
#include <cstdlib>
#include <cerrno>
#endif

/* Implementation details:

   We create a mappable object the size of the buffer window, and
   arrange for it to be mapped twice into memory, at adjacent
   locations:

   | a b c d ... | a b c d ... |

   This vastly simplifies the management of a window buffer, because
   any address that starts inside the first mapping of the buffer can
   access the entire buffer without concern for wraparound.

   The mechanism to accomplish the double-mapping differs slightly
   depending on the platform.

   - In Unix-like systems, we use mmap to reserve a double-sized chunk
     of address space, and then replace that reservation (using
     MAP_FIXED) with two shared mappings of a deleted temporary file
     (truncated to the desired buffer size). There does not seem to be
     any mechanism create a second mapping of an anonymous shared
     memory segment. The downside of this approach is that it may
     generate unwanted I/O when the kernel attempts to write back
     "dirty" pages to the deleted file [1] (workaround would be to
     allocate the file in tmpfs). This approach has been tested in
     both Linux 3.2 (though any 2.4+ should work) and Solaris 10. Note
     that this technique does NOT work under Cygwin, because the
     latter's mmap implementation doesn't support MAP_FIXED.

   - In Windows (and Cygwin), we can avoid any risk of unwated I/O by
     creating an anonymous file mapping object, and then creating two
     adjacent "views" of that one object. However, Windows does not
     provide a clean mechanism to reserve a chunk of address space and
     then to fill it with the mapping(s) of choice, so we have to
     manually locate a suitable hole in the address space and hope the
     hole doesn't disappear before we can claim it (which would cause
     our allocation to fail).
   
     [1] http://lkml.iu.edu//hypermail/linux/kernel/1401.2/02190.html
 */

char *
window_buffer::_get_ptr(size_t offset)
{
    /* It's not actually difficult to compute where an out of bounds
       offset would land... but attempting to access space outside the
       current buffer window is almost certainly a bug (at best, it's
       a dangerous and bug-prone practice).
     */
    ASSERT (read_begin() <= offset and offset < write_end());

    /* Every offset maps to *two* addresses. We always return the on
       in the first mapping, thus ensuring that the address is
       followed by window_size() contiguous bytes of buffer space.
    */
    return &_data[offset & (window_size()-1)];
}

void
window_buffer::advance_writer(size_t new_wbegin)
{
    THROW_IF(new_wbegin < write_begin(),
             illegal_argument, "Attempt to advance writer backwards");
    THROW_IF(write_end() < new_wbegin,
             illegal_argument, "Cannot advance past end of write window");
    _tail = new_wbegin;
}

void
window_buffer::advance_reader(size_t new_rbegin)
{
    THROW_IF(new_rbegin < read_begin(),
             illegal_argument, "Attempt to advance reader backwards");
    THROW_IF(read_end() < new_rbegin,
             illegal_argument, "Cannot advance past end of read window");
    _head = new_rbegin;
}

window_buffer::window_buffer(size_t bufsz, size_t start_offset)
    : _size(bufsz)
    , _head(start_offset)
    , _tail(start_offset)
{
    THROW_IF(bufsz & (bufsz-1), illegal_argument, "Power of two buffer size required");

    size_t map_size = 2*bufsz;
    
#ifdef USE_WINAPI
    static_assert(sizeof(DWORD) <= sizeof(int),
                  "Fix os_error to work with Windows exceptions");
    
    HANDLE h = CreateFileMapping(INVALID_HANDLE_VALUE, NULL, PAGE_READWRITE,
                                 0, (DWORD) map_size, NULL);
    THROW_IF(not h, os_error, (int) GetLastError(), "Unable to allocate buffer");
    DEFER(CloseHandle(h));

    // find a suitable piece of address space
    _data = (char*) VirtualAlloc(0, map_size, MEM_RESERVE, PAGE_NOACCESS);
    THROW_IF(not _data, os_error, (int) GetLastError(), "Unable to reserve address space");
    THROW_IF(not VirtualFree(_data, 0, MEM_RELEASE),
             os_error, (int) GetLastError(), "Unable to clear reservation");

    char *data2 = (char*) MapViewOfFileEx(h, FILE_MAP_ALL_ACCESS,
                                          0, 0, bufsz, _data);
    THROW_IF(data2 != _data,
             os_error, (int) GetLastError(), "Unable to map buffer #1");
    DEFER_UNLESS(keep_it, UnmapViewOfFile(data2));
    
    char *data3 = (char*) MapViewOfFileEx(h, FILE_MAP_ALL_ACCESS,
                                          0, 0, bufsz, _data+bufsz);
    THROW_IF(data3 != _data+bufsz,
             os_error, (int) GetLastError(), "Unable to map buffer #2");
    keep_it = true;
    
#else
    // step 1: create temporary file of the correct size
    auto sfname = (sysconf::tmpfs_dir + std::string("/buffer-XXXXXX"));
    char *fname = (char *)sfname.c_str();
    int fd = mkstemp(fname);
    THROW_IF(fd < 0, os_error, errno, "Unable to create temp file");
    DEFER(close(fd));

    THROW_IF(unlink(fname),
             os_error, errno, "Unable to unlink temp file");
    THROW_IF(ftruncate(fd, bufsz),
             os_error, errno, "Unable to size temp file");

    _data = (char*) mmap(0, map_size, PROT_NONE,
                         MAP_ANONYMOUS|MAP_PRIVATE, -1, 0);
    THROW_IF(_data == MAP_FAILED,
             os_error, errno, "mmap failed to acquire address space");
    DEFER_UNLESS(keep_it, munmap(_data, map_size));

    void *data2 = mmap(_data, bufsz, PROT_READ|PROT_WRITE,
                       MAP_FIXED|MAP_SHARED, fd, 0);
    THROW_IF(_data != data2, os_error, errno, "failed to mmap temp file");
    
    void *data3 = mmap(_data+bufsz, bufsz, PROT_READ|PROT_WRITE,
                       MAP_FIXED|MAP_SHARED, fd, 0);
    THROW_IF(_data+bufsz != data3, os_error, errno, "failed to mmap temp file");
    keep_it = true;
#endif

#ifndef NDEBUG
    char const *msg = "This is a test of the replicated buffer system";
    strcpy(_data, msg);
    ASSERT(not strcmp(_data+bufsz, msg));
#endif    
    // prefault the buffer
    memset(_data, '\0', 2*window_size());
}

window_buffer::~window_buffer() {
    ASSERT(not available_to_read());
    
#ifdef USE_WINAPI
    UnmapViewOfFile(_data);
    UnmapViewOfFile(_data+window_size());
#else
    munmap(_data, 2*window_size());
#endif
}
