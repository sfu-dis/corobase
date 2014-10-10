#include "sm-log-file.h"

#include <string>
#include <vector>
#include <unistd.h>
#include <cerrno>

using namespace RCU;

int main() {
    rcu_register();
    rcu_enter();
    try {

        tmp_dir dname;

        {
            sm_log_file_mgr lm(dname, 1);
            
            fprintf(stderr, "Listing contents of newly-created log at %s:\n", *dname);
            for (char const *fname : dirent_iterator(dname)) 
                fprintf(stderr, "\t%s\n", fname);

            char msg[100];
            for (int i=0; i < 10; i++) {
                lm.set_segment_size(lm.segment_size*2);
                auto *sid = lm.prepare_new_segment(lm.segment_size);
                bool rval = lm.create_segment(sid);
                DIE_IF(not rval, "Unable to create segment");
                int fd = lm.open_for_write(sid);
                DEFER(os_close(fd));
                int nbytes = snprintf(msg, sizeof(msg), "Log segment %u covers %zx to %zx\n",
                                      sid->segnum,
                                      sid->start_offset, sid->end_offset);
                os_pwrite(fd, msg, nbytes, 0);
                if (sid->segnum < 8) {
                    LSN x = sid->make_lsn(sid->start_offset + 80);
                    lm.update_durable_mark(x);
                }
            }

            LSN
                cb = LSN::make(0x6000, 5),
                ce = LSN::make(0x12000, 6);
            lm.update_chkpt_mark(cb, ce);
        
            fprintf(stderr, "Listing contents of expanded log:\n");
            for (char const *fname : dirent_iterator(dname)) 
                fprintf(stderr, "\t%s\n", fname);

            lm.truncate_after(8, 0x30000);

            auto *sid = lm._newest_segment();
            LSN dmark = sid->make_lsn(sid->start_offset+384);
            lm.update_durable_mark(dmark);
            fprintf(stderr, "Contents of log after truncation:\n");
            for (char const *fname : dirent_iterator(dname)) 
                fprintf(stderr, "\t%s\n", fname);
        
            lm.reclaim_before(3);
            fprintf(stderr, "Contents of log after reclamation:\n");
            for (char const *fname : dirent_iterator(dname)) 
                fprintf(stderr, "\t%s\n", fname);
        }
        
        {
            sm_log_file_mgr lm(dname, 1);
            fprintf(stderr, "Listing contents of existing log at %s:\n", *dname);
            for (char const *fname : dirent_iterator(dname)) 
                fprintf(stderr, "\t%s\n", fname);
        }
        
        
    }
    catch (os_error &err) {
        fprintf(stderr, "Yikes! Caught OS error %d: %s", err.err, err.msg);
        exit(-1);
    }
    catch (log_file_error &err) {
        fprintf(stderr, "Yikes! Log file error: %s", err.msg);
        exit(-1);
    }
    catch (illegal_argument &err) {
        fprintf(stderr, "Yikes! Illegal argument: %s", err.msg);
        exit(-1);
    }
    
    rcu_deregister();
}
