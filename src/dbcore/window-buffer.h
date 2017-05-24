// -*- mode:c++ -*-
#ifndef __WINDOW_BUFFER_H
#define __WINDOW_BUFFER_H

#include <cstddef>
#include <stdint.h>

#include <glog/logging.h>

#include "sm-exceptions.h"

/* A window buffer implementation that maintains a fixed-size
   sliding window over a large or infinite stream of data.

   The abstraction is of two adjacent windows, one containing space to
   be written into, and the other containing bytes to be read out:

       | --------- buffer size ----------|
       
   ... | readable bytes | writable bytes | ...
   
       ^                ^                ^
       read_begin()     read_end()
                        write_begin()    write_end()

   The combined size of the two windows is fixed, with new writable
   bytes becoming available as the reader consumes them, and more
   readable bytes becoming available as the writer produces them. The
   implementation makes no assumptions about what readers and writers
   do with their windows (e.g. the order in which bytes are accessed),
   but it does require that writers make data available to readers in
   sequential fashion (and readers return bytes to writer).

   This implementation uses virtual memory hackery to ensure that both
   push and pop can always work with a single contiguous chunk of
   memory. Thus, buffer operations can use normal pointers and
   functions like memcpy without concern for wraparound.

   This class provides only buffer management (tracking what data has
   been produced but not yet consumed, and notifying producers
   (consumers) when the buffer is full (empty). It makes no provision
   for concurrent access; the user must coordinate that by other
   means, if such is needed. In a similar vein, attempts to insert
   into a full buffer, or to consume from an empty one, fail cleanly
   rather than blocking; users can implement whatever waiting
   mechanism they choose on top.
 */
struct window_buffer {
    /* The windows size must be a power of two multiple of the system
       page size (e.g. 4kB on x86 Linux, 8kB on Solaris/Sparc, 64kB on
       Windows).
     */
    window_buffer(size_t bufsz, size_t start_offset=0);
    ~window_buffer();

    // no copying allowed, sorry
    void operator=(window_buffer const &)=delete;
    window_buffer(window_buffer const &)=delete;

    /* The logical start of the buffer window, which marks the
       location of the next readable byte.
     */
    size_t read_begin() { return _head; }

    /* The logical end of the read portion of the buffer window, which
       marks the start of the next writable byte.
     */
    size_t read_end() { return _tail; }
    size_t write_begin() { return read_end(); }

    /* The logical end of the buffer window, which marks the first
       byte that is past the end of the current window (e.g. the next
       byte to become available for writing once the reader consumes
       some data).
     */
    size_t write_end() { return read_begin() + window_size(); }

    /* The size of the buffer, which is the maximum number of bytes
       that can be written at once (e.g. without a reader consuming
       some of them along the way).
     */
    size_t window_size() { return _size; }
    
    /* The number of bytes that can safely be written to the buffer
     */
    size_t available_to_write() {
        return write_end() - write_begin();
    }
    
    /* The number of bytes available to read from the buffer
     */
    size_t available_to_read() {
        return read_end() - read_begin();
    }

    /* Return a pointer to the writable buffer region. The memory
       remains available for writing until a call to advance_writer()
       transfers ownership to the consumer. 

       WARNING: it is the caller's responsibility not to access more
       than available_to_write() bytes of the buffer, in particular
       when the write region has size zero.
       
       WARNING: The caller should assume that the pointer returned by
       write_buf changes in unpredictable ways after every call to
       advance_writer(). However, the implementation preserves all
       data previously written to the buffer, whether transferred to
       the reader or not.
     */
    char *write_buf(size_t offset, size_t size) {
        LOG_IF(FATAL, write_begin() > offset)
            << "Attempted write to region before before window "
            << write_begin() << "/" << offset;
        if (write_end() < offset+size) {
            return NULL;
        }
        return _get_ptr(offset);
    }

    /* Return a pointer to the first byte of readable space. The
       semantics are identical to write_buf(), except that
       advance_reader() is used to return buffer space back to the
       writer.
     */
    char const *read_buf(size_t offset, size_t size) {
		THROW_IF(read_begin() > offset, illegal_argument,
				"Attempted read from region before before window");
        if (read_end() < offset+size) {
            return NULL;
        }
        return _get_ptr(offset);
    }

    void advance_writer(size_t new_wbegin);
    void advance_reader(size_t new_rbegin);

    /* Return a pointer to the part of the buffer that the given
       offset would map to if it were the start of the buffer window.

       WARNING: Undefined behavior results if the requested offset
       falls outside the current buffer window.
     */
    char *_get_ptr(size_t offset);
        
    
    size_t _size;
    size_t _head;
    size_t _tail;
    char *_data;
};

#endif

