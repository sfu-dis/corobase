#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include "sm-chkpt.h"
#include "sm-log.h"
#include "sm-oid.h"

sm_chkpt_mgr *chkptmgr;

sm_chkpt_mgr::sm_chkpt_mgr(LSN last_cstart) :
    _shutdown(false), _buf_pos(0), _dur_pos(0),
    _fd(-1), _last_cstart(last_cstart)
{
    ALWAYS_ASSERT(not mlock(_buffer, BUFFER_SIZE));
}

sm_chkpt_mgr::~sm_chkpt_mgr()
{
    volatile_write(_shutdown, true);
    take();
    _daemon->join();
}

void
sm_chkpt_mgr::start_chkpt_thread()
{
    ASSERT(logmgr and oidmgr);
    _daemon = new std::thread(&sm_chkpt_mgr::do_chkpt, this);
}

void
sm_chkpt_mgr::take()
{
    _daemon_cv.notify_all();
}

void
sm_chkpt_mgr::do_chkpt()
{
    RCU::rcu_register();
start:
    std::unique_lock<std::mutex> lock(_daemon_mutex);
    // Take a chkpt every 10 seconds
    _daemon_cv.wait_for(lock, std::chrono::seconds(10));
    if (volatile_read(_shutdown)) {
        RCU::rcu_deregister();
        return;
    }
    RCU::rcu_enter();
    auto cstart = logmgr->flush();
    prepare_file(cstart);
    oidmgr->take_chkpt(cstart);
    // FIXME (tzwang): originally we should put info about the chkpt
    // in a log record and then commit that sys transaction that's
    // responsible for doing chkpt. But that would interfere with
    // normal forward processing. Instead, here we don't use a system
    // transaction to chkpt, but use a dedicated thread and avoid going
    // to the log at all. As a result, we only need to care abou the
    // chkpt begin stamp, and only cstart is useful in this case. cend
    // is ignored and emulated as cstart+1.
    //
    // Note that the chkpt data file's name only contains cstart, and
    // we only write the chkpt marker file (chk-cstart-cend) when chkpt
    // is succeeded.
    //
    // TODO: modify update_chkpt_mark etc to remove/ignore cend related.
    //
    // (align_up is there to supress an assert in sm-log-file.cpp when
    // iterating files in the log dir)
    os_fsync(_fd);
    os_close(_fd);
    logmgr->update_chkpt_mark(cstart,
            LSN::make(align_up(cstart.offset()+1), cstart.segment()));
    scavenge();
    _last_cstart = cstart;
    RCU::rcu_exit();
    printf("[Checkpoint] marker: 0x%lx\n", cstart.offset());
    if (not volatile_read(_shutdown))
        goto start;
    RCU::rcu_deregister();
}

void
sm_chkpt_mgr::scavenge()
{
    if (not _last_cstart.offset())
        return;
    char buf[CHKPT_DATA_FILE_NAME_BUFSZ];
    size_t n = os_snprintf(buf, sizeof(buf),
                           CHKPT_DATA_FILE_NAME_FMT, _last_cstart._val);
    ASSERT(n < sizeof(buf));
    ASSERT(oidmgr and oidmgr->dfd);
    os_unlinkat(oidmgr->dfd, buf);
}

void
sm_chkpt_mgr::prepare_file(LSN cstart)
{
    char buf[CHKPT_DATA_FILE_NAME_BUFSZ];
    size_t n = os_snprintf(buf, sizeof(buf),
                           CHKPT_DATA_FILE_NAME_FMT, cstart._val);
    ASSERT(n < sizeof(buf));
    ASSERT(oidmgr and oidmgr->dfd);
    _fd = os_openat(oidmgr->dfd, buf, O_CREAT|O_WRONLY);
}

void
sm_chkpt_mgr::write_buffer(void *p, size_t s)
{
    if (s > BUFFER_SIZE) {
        // Too large, write to file directly
        sync_buffer();
        ALWAYS_ASSERT(false);
    }
    else {
        if (_buf_pos + s > BUFFER_SIZE) {
            sync_buffer();
            _buf_pos = _dur_pos = 0;
        }
        ASSERT(_buf_pos + s <= BUFFER_SIZE);
        memcpy(_buffer + _buf_pos, p, s);
        _buf_pos += s;
    }
}

void
sm_chkpt_mgr::sync_buffer()
{
    if (_buf_pos > _dur_pos) {
        os_write(_fd, _buffer + _dur_pos, _buf_pos - _dur_pos);
        _dur_pos = _buf_pos;
    }
    os_fsync(_fd);
}

