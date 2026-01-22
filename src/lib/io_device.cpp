//
// Created by Seyed Mehdi Hosseini Motlagh on 12/23/25.
//


#include <iomgr/io_device.hpp>
#include <sisl/logging/logging.h>

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
      	const std::uintptr_t addr = reinterpret_cast<std::uintptr_t>(iocb->get_data());
        const bool off_4k_aligned = (iocb->offset % 4096) == 0;
        const bool off_512_aligned = (iocb->offset % 512) == 0;
        const bool addr_4k_aligned = (addr % 4096) == 0;
        const bool addr_512_aligned = (addr % 512) == 0;

        switch (iocb->op_type) {
        case DriveOpType::WRITE:
         //   HISTOGRAM_OBSERVE(*m_metrics, write_lat, dur);
         //   HISTOGRAM_OBSERVE(*m_metrics, write_size, iocb->size);
         //   LOGTRACE("write, size {}, lat {}", iocb->size, dur);
            // HISTOGRAM_OBSERVE(*m_metrics, write_lat, dur);
            // HISTOGRAM_OBSERVE(*m_metrics, write_size, iocb->size);
           // if (count % 10000 == 0) LOGINFO("write, size {}, lat {}", iocb->size, dur);
			//LOGINFO("single write, size {}, lat {}", iocb->size, dur);
        	if ((iocb->offset)%4096 || (iocb->offset)%512)
        	{
        		LOGWARNMOD(iocb, "{} single write iocb_id {}, size {}, lat {} offset {} 4k/512 offset aligned {}/{} address 4k/512 aligned {}/{}",normal_path_str, iocb->iocb_id, iocb->size, dur, iocb->offset, off_4k_aligned, off_512_aligned, addr_4k_aligned, addr_512_aligned);
        	}
        	LOGTRACEMOD(iocb, "{} single write iocb_id {}, size {}, lat {} offset {} 4k/512 offset aligned {}/{} address 4k/512 aligned {}/{}",normal_path_str, iocb->iocb_id, iocb->size, dur, iocb->offset, off_4k_aligned, off_512_aligned, addr_4k_aligned, addr_512_aligned);
			if (!normal_path)
			{
				wcount++;
				wdur += dur;
				wsize += iocb->size;

				if(wcount % 10000 == 0) {
					LOGINFOMOD(iocb, "write, total count {} average size {}, average lat {}",wcount, wsize/ 10000 , wdur/ 10000);
					wdur = 0;
					wsize = 0;
				}
			}
            break;
        case DriveOpType::READ:
          //  HISTOGRAM_OBSERVE(*m_metrics, read_lat, dur);
          //  HISTOGRAM_OBSERVE(*m_metrics, read_size, iocb->size);
          // LOGTRACE("read, size {}, lat {}", iocb->size, dur);
            // HISTOGRAM_OBSERVE(*m_metrics, read_lat, dur);
            // HISTOGRAM_OBSERVE(*m_metrics, read_size, iocb->size);
        	if ((iocb->offset)%4096 || (iocb->offset)%512)
        	{

        		LOGWARNMOD(iocb, "{} single read iocb_id {}, size {}, lat {} offset {} 4k/512 aligned {}/{} address 4k/512 aligned {}/{}",normal_path_str,iocb->iocb_id, iocb->size, dur, iocb->offset, off_4k_aligned, off_512_aligned, addr_4k_aligned, addr_512_aligned);
        	}
        	LOGTRACEMOD(iocb, "{} single read iocb_id {}, size {}, lat {} 4k/512 offset {} aligned {}/{} address 4k/512 aligned {}/{}",normal_path_str,iocb->iocb_id, iocb->size, dur, iocb->offset, off_4k_aligned, off_512_aligned, addr_4k_aligned, addr_512_aligned);

           //if (count % 10000 == 0) LOGINFO("read, size {}, lat {}", iocb->size, dur);
        	if (!normal_path)
        	{
        		rcount++;
        		rdur += dur;
        		rsize += iocb->size;
        		if(rcount % 10000 == 0) {
        			LOGINFOMOD(iocb, "read, total count {} average size {}, average lat {}",rcount, rsize/ 10000 , rdur/ 10000);
        			rdur = 0;
        			rsize = 0;
        		}
        	}
            break;
        case DriveOpType::FSYNC:
          //  HISTOGRAM_OBSERVE(*m_metrics, fsync_lat, dur);
          //  HISTOGRAM_OBSERVE(*m_metrics, fsync_size, iocb->size);
          //  LOGTRACE("fsync, size {}, lat {}", iocb->size, dur);
            // HISTOGRAM_OBSERVE(*m_metrics, fsync_lat, dur);
            // HISTOGRAM_OBSERVE(*m_metrics, fsync_size, iocb->size);
            if (count % 10000 == 0) LOGWARNMOD(iocb, "fsync, size {}, lat {}", iocb->size, dur);
			fcount++;
			fdur += dur;
			fsize += iocb->size;
			if(fcount % 10000 == 0) {
				LOGINFOMOD(iocb, "fsync, total count {} average size {}, average lat {}", fcount, fsize/ 10000 , fdur/ 10000);
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
