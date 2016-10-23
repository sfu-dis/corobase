#pragma once
#include <chrono>
#include <condition_variable>
#include <thread>
#include <mutex>
#include "sm-common.h"
#include "sm-log-impl.h"

#define CHKPT_DATA_FILE_NAME_FMT "oac-%016zx"
#define CHKPT_DATA_FILE_NAME_BUFSZ sizeof("chd-0123456789abcdef")

class sm_chkpt_mgr {
public:
    sm_chkpt_mgr(LSN last_cstart);
    ~sm_chkpt_mgr();
    void take(bool wait = false);
    void do_chkpt();
    void daemon();
    void write_buffer(void *p, size_t s);
    void sync_buffer();
    void start_chkpt_thread();
    static void recover(LSN chkpt_start, sm_log_recover_mgr *lm);

    static const size_t BUFFER_SIZE = 512 * 1024 * 1024;

private:
    bool                    _shutdown;
    std::thread*            _daemon;
    std::mutex              _daemon_mutex;
    std::condition_variable _daemon_cv;
    size_t                  _buf_pos;
    size_t                  _dur_pos;
    char                    _buffer[BUFFER_SIZE];
    int                     _fd;
    LSN                     _last_cstart;
    std::condition_variable _wait_chkpt_cv;
    std::mutex              _wait_chkpt_mutex;
    bool                    _in_progress;

    void prepare_file(LSN cstart);
    void scavenge();
};

extern sm_chkpt_mgr *chkptmgr;

