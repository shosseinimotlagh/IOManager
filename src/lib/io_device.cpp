//
// Created by Seyed Mehdi Hosseini Motlagh on 12/23/25.
//


#include <iomgr/io_device.hpp>
#include <sisl/logging/logging.h>
#include <cstdint>

SISL_LOGGING_DEF(iocb)
namespace iomgr
{


        void IODevice::observe_metrics(drive_iocb* iocb, bool normal_path) {
    if (!m_metrics.get()) { return; }
    static uint64_t count = 0;
    static uint64_t wcount = 0;
    static uint64_t rcount = 0;
    static uint64_t fcount = 0;
    static uint64_t wdur = 0;
    static uint64_t rdur = 0;
    static uint64_t fdur = 0;
    static uint64_t wsize = 0;
    static uint64_t rsize = 0;
    static uint64_t fsize = 0;

    std::string normal_path_str = normal_path ? "iocomplete_path" : "folly_path";
    auto dur = get_elapsed_time_us(iocb->op_start_time);

    // Determine buffer address safely depending on whether this IOCB uses iovecs or a single buffer.
    std::uintptr_t addr = 0;
    if (iocb->has_iovs()) {
        const iovec* iovs = iocb->get_iovs();
        const int iovcnt = iocb->iovcnt;
        if (iovs != nullptr && iovcnt > 0 && iovs[0].iov_base != nullptr) {
            addr = reinterpret_cast<std::uintptr_t>(iovs[0].iov_base);
        } else {
            // If no valid iovecs, leave addr as 0; alignment checks will reflect that.
            addr = 0;
        }
    } else {
        addr = reinterpret_cast<std::uintptr_t>(iocb->get_data());
    }

    switch (iocb->op_type) {
    case DriveOpType::WRITE: {
        const bool off_4k_aligned = (iocb->offset % 4096) == 0;
        const bool off_512_aligned = (iocb->offset % 512) == 0;
        const bool addr_4k_aligned = (addr % 4096) == 0;
        const bool addr_512_aligned = (addr % 512) == 0;

        if ((iocb->offset % 4096) || (iocb->offset % 512)) {
            LOGWARNMOD(iocb,
                       "{} single write iocb_id {}, size {}, lat {} offset {} 4k/512 offset aligned {}/{} address 4k/512 aligned {}/{}",
                       normal_path_str, iocb->iocb_id, iocb->size, dur, iocb->offset,
                       off_4k_aligned, off_512_aligned, addr_4k_aligned, addr_512_aligned);
        }

        LOGTRACEMOD(iocb,
                    "{} single write iocb_id {}, size {}, lat {} offset {} 4k/512 offset aligned {}/{} address 4k/512 aligned {}/{}",
                    normal_path_str, iocb->iocb_id, iocb->size, dur, iocb->offset,
                    off_4k_aligned, off_512_aligned, addr_4k_aligned, addr_512_aligned);

        if (!normal_path) {
            wcount++;
            wdur += dur;
            wsize += iocb->size;
            if (wcount % 10000 == 0) {
                LOGINFOMOD(iocb, "write, total count {} average size {}, average lat {}",
                           wcount, wsize / 10000, wdur / 10000);
                wdur = 0;
                wsize = 0;
            }
        }
        break;
    }
    case DriveOpType::READ: {
        const bool off_4k_aligned = (iocb->offset % 4096) == 0;
        const bool off_512_aligned = (iocb->offset % 512) == 0;
        const bool addr_4k_aligned = (addr % 4096) == 0;
        const bool addr_512_aligned = (addr % 512) == 0;

        if ((iocb->offset % 4096) || (iocb->offset % 512)) {
            LOGWARNMOD(iocb,
                       "{} single read iocb_id {}, size {}, lat {} offset {} 4k/512 aligned {}/{} address 4k/512 aligned {}/{}",
                       normal_path_str, iocb->iocb_id, iocb->size, dur, iocb->offset,
                       off_4k_aligned, off_512_aligned, addr_4k_aligned, addr_512_aligned);
        }

        LOGTRACEMOD(iocb,
                    "{} single read iocb_id {}, size {}, lat {} 4k/512 offset {} aligned {}/{} address 4k/512 aligned {}/{}",
                    normal_path_str, iocb->iocb_id, iocb->size, dur, iocb->offset,
                    off_4k_aligned, off_512_aligned, addr_4k_aligned, addr_512_aligned);

        if (!normal_path) {
            rcount++;
            rdur += dur;
            rsize += iocb->size;
            if (rcount % 10000 == 0) {
                LOGINFOMOD(iocb, "read, total count {} average size {}, average lat {}",
                           rcount, rsize / 10000, rdur / 10000);
                rdur = 0;
                rsize = 0;
            }
        }
        break;
    }
    case DriveOpType::FSYNC:
        if (count % 10000 == 0) LOGWARNMOD(iocb, "fsync, size {}, lat {}", iocb->size, dur);
        fcount++;
        fdur += dur;
        fsize += iocb->size;
        if (fcount % 10000 == 0) {
            LOGINFOMOD(iocb, "fsync, total count {} average size {}, average lat {}", fcount, fsize / 10000, fdur / 10000);
            fdur = 0;
            fsize = 0;
        }
        break;
    default:
        break;
    }
    count++;
}

} // namespace iomgr
