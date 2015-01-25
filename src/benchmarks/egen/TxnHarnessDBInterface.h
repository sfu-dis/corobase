/*
 * Legal Notice
 *
 * This document and associated source code (the "Work") is a part of a
 * benchmark specification maintained by the TPC.
 *
 * The TPC reserves all right, title, and interest to the Work as provided
 * under U.S. and international laws, including without limitation all patent
 * and trademark rights therein.
 *
 * No Warranty
 *
 * 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION
 *     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE
 *     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER
 *     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY,
 *     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES,
 *     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR
 *     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF
 *     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE.
 *     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT,
 *     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT
 *     WITH REGARD TO THE WORK.
 * 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO
 *     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE
 *     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS
 *     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT,
 *     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
 *     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT
 *     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD
 *     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES.
 *
 * Contributors
 * - Christopher Chan-Nui, Matt Emmerton
 */

/*
 * Interface file for the various C<txn_name> classes.
 */

#ifndef DBINTERFACE_H_INCLUDED
#define DBINTERFACE_H_INCLUDED

#include <string>
#include <cassert>
#include <cstdio>
#include "TxnHarnessStructs.h"
#include "TxnHarnessSendToMarketInterface.h"
#include "error.h"
#include "../bench.h"

using namespace std;

namespace TPCE {

class CTradeOrderDBInterface {
    public:
        virtual bench_worker::txn_result DoTradeOrderFrame1(const TTradeOrderFrame1Input *pIn, TTradeOrderFrame1Output *pOut) = 0;
        virtual bench_worker::txn_result DoTradeOrderFrame2(const TTradeOrderFrame2Input *pIn, TTradeOrderFrame2Output *pOut) = 0;
        virtual bench_worker::txn_result DoTradeOrderFrame3(const TTradeOrderFrame3Input *pIn, TTradeOrderFrame3Output *pOut) = 0;
        virtual bench_worker::txn_result DoTradeOrderFrame4(const TTradeOrderFrame4Input *pIn, TTradeOrderFrame4Output *pOut) = 0;
        virtual bench_worker::txn_result DoTradeOrderFrame5(void                                                            ) = 0;
        virtual bench_worker::txn_result DoTradeOrderFrame6(void                                                            ) = 0;
        virtual ~CTradeOrderDBInterface  () { }
};

class CTradeStatusDBInterface {
    public:
        virtual bench_worker::txn_result DoTradeStatusFrame1(const TTradeStatusFrame1Input *pIn, TTradeStatusFrame1Output *pOut) = 0;
        virtual ~CTradeStatusDBInterface  () { }
};

class CCustomerPositionDBInterface {
    public:
        virtual bench_worker::txn_result DoCustomerPositionFrame1(const TCustomerPositionFrame1Input *pIn, TCustomerPositionFrame1Output *pOut) = 0;
        virtual bench_worker::txn_result DoCustomerPositionFrame2(const TCustomerPositionFrame2Input *pIn, TCustomerPositionFrame2Output *pOut) = 0;
        virtual bench_worker::txn_result DoCustomerPositionFrame3(void                                                                        ) = 0;
        virtual ~CCustomerPositionDBInterface  () { }
};

class CBrokerVolumeDBInterface {
    public:
        virtual bench_worker::txn_result DoBrokerVolumeFrame1(const TBrokerVolumeFrame1Input *pIn, TBrokerVolumeFrame1Output *pOut) = 0;
        virtual ~CBrokerVolumeDBInterface  () { }
};

class CSecurityDetailDBInterface {
    public:
        virtual bench_worker::txn_result DoSecurityDetailFrame1(const TSecurityDetailFrame1Input *pIn, TSecurityDetailFrame1Output *pOut) = 0;
        virtual ~CSecurityDetailDBInterface  () { }
};

class CMarketWatchDBInterface {
    public:
        virtual bench_worker::txn_result DoMarketWatchFrame1 (const TMarketWatchFrame1Input *pIn, TMarketWatchFrame1Output *pOut) = 0;
        virtual ~CMarketWatchDBInterface   () { }
};

class CTradeLookupDBInterface {
    public:
        virtual bench_worker::txn_result DoTradeLookupFrame1(const TTradeLookupFrame1Input *pIn, TTradeLookupFrame1Output *pOut) = 0;
        virtual bench_worker::txn_result DoTradeLookupFrame2(const TTradeLookupFrame2Input *pIn, TTradeLookupFrame2Output *pOut) = 0;
        virtual bench_worker::txn_result DoTradeLookupFrame3(const TTradeLookupFrame3Input *pIn, TTradeLookupFrame3Output *pOut) = 0;
        virtual bench_worker::txn_result DoTradeLookupFrame4(const TTradeLookupFrame4Input *pIn, TTradeLookupFrame4Output *pOut) = 0;
        virtual ~CTradeLookupDBInterface  () { }
};

class CTradeUpdateDBInterface {
    public:
        virtual bench_worker::txn_result DoTradeUpdateFrame1(const TTradeUpdateFrame1Input *pIn, TTradeUpdateFrame1Output *pOut) = 0;
        virtual bench_worker::txn_result DoTradeUpdateFrame2(const TTradeUpdateFrame2Input *pIn, TTradeUpdateFrame2Output *pOut) = 0;
        virtual bench_worker::txn_result DoTradeUpdateFrame3(const TTradeUpdateFrame3Input *pIn, TTradeUpdateFrame3Output *pOut) = 0;
        virtual ~CTradeUpdateDBInterface  () { }
};

class CTradeResultDBInterface {
    public:
        virtual bench_worker::txn_result DoTradeResultFrame1(const TTradeResultFrame1Input *pIn, TTradeResultFrame1Output *pOut) = 0;
        virtual bench_worker::txn_result DoTradeResultFrame2(const TTradeResultFrame2Input *pIn, TTradeResultFrame2Output *pOut) = 0;
        virtual bench_worker::txn_result DoTradeResultFrame3(const TTradeResultFrame3Input *pIn, TTradeResultFrame3Output *pOut) = 0;
        virtual bench_worker::txn_result DoTradeResultFrame4(const TTradeResultFrame4Input *pIn, TTradeResultFrame4Output *pOut) = 0;
        virtual bench_worker::txn_result DoTradeResultFrame5(const TTradeResultFrame5Input *pIn                                ) = 0;
        virtual bench_worker::txn_result DoTradeResultFrame6(const TTradeResultFrame6Input *pIn, TTradeResultFrame6Output *pOut) = 0;
        virtual ~CTradeResultDBInterface  () { }
};

class CMarketFeedDBInterface {
    public:
        virtual bench_worker::txn_result DoMarketFeedFrame1(const TMarketFeedFrame1Input *pIn, TMarketFeedFrame1Output *pOut, CSendToMarketInterface *pSendToMarket) = 0;
        virtual ~CMarketFeedDBInterface  () { }
};

class CDataMaintenanceDBInterface {
    public:
        virtual bench_worker::txn_result DoDataMaintenanceFrame1(const TDataMaintenanceFrame1Input *pIn) = 0;
        virtual ~CDataMaintenanceDBInterface  () { }
};

class CTradeCleanupDBInterface {
    public:
        virtual bench_worker::txn_result DoTradeCleanupFrame1(const TTradeCleanupFrame1Input *pIn) = 0;
        virtual ~CTradeCleanupDBInterface  () { }
};

}
#endif // DBINTERFACE_H_INCLUDED
