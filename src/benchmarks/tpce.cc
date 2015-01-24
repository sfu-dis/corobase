/*
   TODOs

   ASSERT to INVARIANT when doing real performance measurement
   after dotxn, check status and make return value
   global last trade id variable -> thread-local 
   stop scanning if given selectivity is satisfied -> custom scanner
   partitioning and scaling, input parameter( configured customers ), CPU pinning for loaders and workers
   string->c_str
   scan key
   sanity check & carninality check 
   for non unique key indices, add ROWID as a new key.  masstree can't deal with duplicate key. 
 */

#include <sys/time.h>
#include <string>
#include <ctype.h>
#include <stdlib.h>
#include <malloc.h>

#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>
#include <vector>

#include "../txn.h"
#include "../macros.h"
#include "../scopedperf.hh"
#include "../spinlock.h"

#include "bench.h"
#include "tpce.h"

using namespace std;
using namespace util;
using namespace TPCE;


//typedef TUPLE std::vector<std::pair<std::string *, const std::string*>>;
// TPC-E workload mix
uint64_t lastTradeId;
static double g_txn_workload_mix[] = { 4.9, 13, 1, 18, 14, 8, 10.1, 10, 19, 2 }; 

// Egen
int egen_init(int argc, char* argv[]);
void egen_release();
CCETxnInputGenerator* 		transactions_input_init(int customers, int sf, int wdays);
CDM* 						data_maintenance_init(int customers, int sf, int wdays);
CMEE* 						market_init(INT32 TradingTimeSoFar, CMEESUTInterface *pSUT, UINT32 UniqueId);
extern CGenerateAndLoad*	pGenerateAndLoad;
CCETxnInputGenerator*		m_TxnInputGenerator;
CDM*						m_CDM;
CMEESUT*					meesut;
CMEE* 						mee; 
MFBuffer* 					MarketFeedInputBuffer;
TRBuffer* 					TradeResultInputBuffer;

//Buffers
const int loadUnit = 1000;
AccountPermissionBuffer accountPermissionBuffer (3015);
CustomerBuffer customerBuffer (1005);
CustomerAccountBuffer customerAccountBuffer (1005);
CustomerTaxrateBuffer  customerTaxrateBuffer (2010);
HoldingBuffer holdingBuffer(10000);
HoldingHistoryBuffer holdingHistoryBuffer(2*loadUnit);
HoldingSummaryBuffer holdingSummaryBuffer(6000);
WatchItemBuffer watchItemBuffer (iMaxItemsInWL*1020+5000);
WatchListBuffer watchListBuffer (1020);
BrokerBuffer brokerBuffer(100);
CashTransactionBuffer cashTransactionBuffer(loadUnit);
ChargeBuffer chargeBuffer(20);
CommissionRateBuffer commissionRateBuffer (245);
SettlementBuffer settlementBuffer(loadUnit);
TradeBuffer tradeBuffer(loadUnit);
TradeHistoryBuffer tradeHistoryBuffer(3*loadUnit);
TradeTypeBuffer tradeTypeBuffer (10);
CompanyBuffer companyBuffer (1000);
CompanyCompetitorBuffer companyCompetitorBuffer(3000);
DailyMarketBuffer dailyMarketBuffer(3000);
ExchangeBuffer exchangeBuffer(9);
FinancialBuffer financialBuffer (1500);
IndustryBuffer industryBuffer(107);
LastTradeBuffer lastTradeBuffer (1005);
NewsItemBuffer newsItemBuffer(200); 
NewsXRefBuffer newsXRefBuffer(200);//big
SectorBuffer sectorBuffer(17);
SecurityBuffer securityBuffer(1005);
AddressBuffer addressBuffer(1005);
StatusTypeBuffer statusTypeBuffer (10);
TaxrateBuffer taxrateBuffer (325);
ZipCodeBuffer zipCodeBuffer (14850);

// Utils
class table_scanner: public abstract_ordered_index::scan_callback {
	public:
		table_scanner( str_arena* arena) : _arena(arena) {}
		virtual bool invoke( const char *keyp, size_t keylen, const string &value)
		{
			std::string * const k = _arena->next();
			INVARIANT(k && k->empty());
			k->assign(keyp, keylen);
			output.emplace_back(k, &value);
			return true;
		}
		std::vector<std::pair<std::string *, const std::string*>> output;
		str_arena* _arena;
};

int64_t GetLastTradeID()
{
	//TODO. decentralize,  thread ID + local counter and TLS
	auto ret = __sync_add_and_fetch(&lastTradeId,1);
	ALWAYS_ASSERT( ret );
	return ret;
}

int64_t CDateTimeToTimeT(CDateTime &cdt)
{ 
	struct tm ts;
	int msec; 
	cdt.GetYMDHMS(&ts.tm_year, &ts.tm_mon, &ts.tm_mday, &ts.tm_hour, &ts.tm_min, &ts.tm_sec, &msec);
	ts.tm_year -= 1900; // counts after 1900;
	ts.tm_mon -= 1; // expects zero based month
	ts.tm_isdst=1; // daylight saving time

	time_t x = mktime (&ts);
	return (int64_t)x;
}

TIMESTAMP_STRUCT TimeTToTimeStamp(time_t t) 
{ 
	struct tm* ts = gmtime(&t);
	TIMESTAMP_STRUCT tss;
	tss.year  = ts->tm_year + 1900; 
	tss.month = ts->tm_mon+1; 
	tss.day   = ts->tm_mday; 
	tss.hour  = ts->tm_hour; 
	tss.minute= ts->tm_min; 
	tss.second= ts->tm_sec; 
	return tss;
}

int64_t TimeStampToTimeT(TIMESTAMP_STRUCT tss) //Converts EGEN TIMESTAMP representation to time_t structure
{ 
	struct tm ts;
	ts.tm_year = tss.year -1900;
	ts.tm_mon = tss.month-1;
	ts.tm_mday =  tss.day; 
	ts.tm_hour =tss.hour;
	ts.tm_min =tss. minute;
	ts.tm_sec = tss.second; 
	ts.tm_isdst=1; // daylight saving time
	time_t x = mktime (&ts);
	return (int64_t)x;
}

int dayOfMonth(int64_t& t)
{  
	struct tm* ts=localtime((time_t*)&t);
	return ts->tm_mday;
}

static inline ALWAYS_INLINE size_t NumPartitions()
{
	return (size_t) scale_factor;
}

void setRNGSeeds(CCETxnInputGenerator* gen, unsigned int UniqueId )
{
	CDateTime   Now;
	INT32       BaseYear;
	INT32       Tmp1, Tmp2;

	Now.GetYMD( &BaseYear, &Tmp1, &Tmp2 );

	// Set the base year to be the most recent year that was a multiple of 5.
	BaseYear -= ( BaseYear % 5 );
	CDateTime   Base( BaseYear, 1, 1 ); // January 1st in the BaseYear

	// Initialize the seed with the current time of day measured in 1/10's of a second.
	// This will use up to 20 bits.
	RNGSEED Seed;
	Seed = Now.MSec() / 100;

	// Now add in the number of days since the base time.
	// The number of days in the 5 year period requires 11 bits.
	// So shift up by that much to make room in the "lower" bits.
	Seed <<= 11;
	Seed += (RNGSEED)((INT64)Now.DayNo() - (INT64)Base.DayNo());

	// So far, we've used up 31 bits.
	// Save the "last" bit of the "upper" 32 for the RNG id.
	// In addition, make room for the caller's 32-bit unique id.
	// So shift a total of 33 bits.
	Seed <<= 33;

	// Now the "upper" 32-bits have been set with a value for RNG 0.
	// Add in the sponsor's unique id for the "lower" 32-bits.
	// Seed += UniqueId;
	Seed += UniqueId;

	// Set the TxnMixGenerator RNG to the unique seed.
	gen->SetRNGSeed( Seed );
	//    m_DriverCESettings.cur.TxnMixRNGSeed = Seed;

	// Set the RNG Id to 1 for the TxnInputGenerator.
	Seed |= UINT64_CONST(0x0000000100000000);
	gen->SetRNGSeed( Seed );
	//    m_DriverCESettings.cur.TxnInputRNGSeed = Seed;
}

unsigned int AutoRand()
{
	struct timeval tv;
	struct tm ltr;
	gettimeofday(&tv, NULL);
	struct tm* lt = localtime_r(&tv.tv_sec, &ltr);
	return (((lt->tm_hour * MinutesPerHour + lt->tm_min) * SecondsPerMinute +
				lt->tm_sec) * MsPerSecond + tv.tv_usec / 1000);
}


struct _dummy {}; // exists so we can inherit from it, so we can use a macro in
// an init list...

class tpce_worker_mixin : private _dummy {

#define DEFN_TBL_INIT_X(name) \
	, tbl_ ## name ## _vec(partitions.at(#name))

	public:
		tpce_worker_mixin(const map<string, vector<abstract_ordered_index *>> &partitions) :
			_dummy() // so hacky...
			TPCE_TABLE_LIST(DEFN_TBL_INIT_X)
	{
	}

#undef DEFN_TBL_INIT_X

	protected:

#define DEFN_TBL_ACCESSOR_X(name) \
	private:  \
			  vector<abstract_ordered_index *> tbl_ ## name ## _vec; \
	protected: \
			   inline ALWAYS_INLINE abstract_ordered_index * \
		tbl_ ## name (unsigned int pid) \
		{ \
			return tbl_ ## name ## _vec[pid - 1];	\
		}

		TPCE_TABLE_LIST(DEFN_TBL_ACCESSOR_X)

#undef DEFN_TBL_ACCESSOR_X

			// only TPCE loaders need to call this- workers are automatically
			// pinned by their worker id (which corresponds to partition id
			// in TPCE)
			//
			// pins the *calling* thread
			static void
			PinToPartition(unsigned int pid)
			{
			}

	public:

		static inline uint32_t
			GetCurrentTimeMillis()
			{
				//struct timeval tv;
				//ALWAYS_ASSERT(gettimeofday(&tv, 0) == 0);
				//return tv.tv_sec * 1000;

				// XXX(stephentu): implement a scalable GetCurrentTimeMillis()
				// for now, we just give each core an increasing number

				static __thread uint32_t tl_hack = 0;
				return tl_hack++;
			}

		// utils for generating random #s and strings

		static inline ALWAYS_INLINE int
			CheckBetweenInclusive(int v, int lower, int upper)
			{
				INVARIANT(v >= lower);
				INVARIANT(v <= upper);
				return v;
			}

		static inline ALWAYS_INLINE int
			RandomNumber(fast_random &r, int min, int max)
			{
				return CheckBetweenInclusive((int) (r.next_uniform() * (max - min + 1) + min), min, max);
			}

		static inline ALWAYS_INLINE int
			NonUniformRandom(fast_random &r, int A, int C, int min, int max)
			{
				return (((RandomNumber(r, 0, A) | RandomNumber(r, min, max)) + C) % (max - min + 1)) + min;
			}

		// following oltpbench, we really generate strings of len - 1...
		static inline string
			RandomStr(fast_random &r, uint len)
			{
				// this is a property of the oltpbench implementation...
				if (!len)
					return "";

				uint i = 0;
				string buf(len - 1, 0);
				while (i < (len - 1)) {
					const char c = (char) r.next_char();
					// XXX(stephentu): oltpbench uses java's Character.isLetter(), which
					// is a less restrictive filter than isalnum()
					if (!isalnum(c))
						continue;
					buf[i++] = c;
				}
				return buf;
			}

		// RandomNStr() actually produces a string of length len
		static inline string
			RandomNStr(fast_random &r, uint len)
			{
				const char base = '0';
				string buf(len, 0);
				for (uint i = 0; i < len; i++)
					buf[i] = (char)(base + (r.next() % 10));
				return buf;
			}
};

// TPCE workers implement TxnHarness interfaces
class tpce_worker : 
	public bench_worker, 
	public tpce_worker_mixin, 
	public CBrokerVolumeDBInterface,
	public CCustomerPositionDBInterface,
	public CMarketFeedDBInterface,
	public CMarketWatchDBInterface,
	public CSecurityDetailDBInterface,
	public CTradeLookupDBInterface,
	public CTradeOrderDBInterface,
	public CTradeResultDBInterface,
	public CTradeStatusDBInterface,
	public CTradeUpdateDBInterface,
	public CDataMaintenanceDBInterface,
	public CTradeCleanupDBInterface,
	public CSendToMarketInterface
{
	public:
		// resp for [partition_id_start, partition_id_end)
		tpce_worker(unsigned int worker_id,
				unsigned long seed, abstract_db *db,
				const map<string, abstract_ordered_index *> &open_tables,
				const map<string, vector<abstract_ordered_index *>> &partitions,
				spin_barrier *barrier_a, spin_barrier *barrier_b,
				uint partition_id_start, uint partition_id_end)
			: bench_worker(worker_id, true, seed, db,
					open_tables, barrier_a, barrier_b),
			tpce_worker_mixin(partitions),
			partition_id_start(partition_id_start),
			partition_id_end(partition_id_end)
	{
		INVARIANT(partition_id_start >= 1);
		INVARIANT(partition_id_start <= NumPartitions());
		INVARIANT(partition_id_end > partition_id_start);
		INVARIANT(partition_id_end <= (NumPartitions() + 1));
		if (verbose) {
			cerr << "tpce: worker id " << worker_id
				<< " => partitions [" << partition_id_start
				<< ", " << partition_id_end << ")"
				<< endl;
		}
		obj_key0.reserve(str_arena::MinStrReserveLength);
		obj_key1.reserve(str_arena::MinStrReserveLength);
		obj_v.reserve(str_arena::MinStrReserveLength);
	}

		// Market Interface
		bool SendToMarket(TTradeRequest &trade_mes)
		{
			mee->SubmitTradeRequest(&trade_mes);
			return true;
		}

		// BrokerVolume transaction
		static txn_result BrokerVolume(bench_worker *w)
		{
			ANON_REGION("BrokerVolume:", &tpce_txn_cg);
			return static_cast<tpce_worker *>(w)->broker_volume();
		}
		txn_result broker_volume()
		{
			TBrokerVolumeTxnInput input;
			TBrokerVolumeTxnOutput output;
			m_TxnInputGenerator->GenerateBrokerVolumeInput(input);
			CBrokerVolume* harness= new CBrokerVolume(this);

			try{
				harness->DoTxn( (PBrokerVolumeTxnInput)&input, (PBrokerVolumeTxnOutput)&output);
			} catch (abstract_db::abstract_abort_exception &ex) {
				db->abort_txn(txn);
				return txn_result(false, 0);
			}
			return txn_result(true, 0);
		}
		void DoBrokerVolumeFrame1(const TBrokerVolumeFrame1Input *pIn, TBrokerVolumeFrame1Output *pOut);

		// CustomerPosition transaction
		static txn_result CustomerPosition(bench_worker *w)
		{
			ANON_REGION("CustomerPosition:", &tpce_txn_cg);
			return static_cast<tpce_worker *>(w)->customer_position();
		}
		txn_result customer_position()
		{
			TCustomerPositionTxnInput input;
			TCustomerPositionTxnOutput output;
			m_TxnInputGenerator->GenerateCustomerPositionInput(input);
			CCustomerPosition* harness= new CCustomerPosition(this);

			try{
				// FIXME. input generator's tax_id doesn't exist. 
//			harness->DoTxn( (PCustomerPositionTxnInput)&input, (PCustomerPositionTxnOutput)&output);
			} catch (abstract_db::abstract_abort_exception &ex) {
				db->abort_txn(txn);
				return txn_result(false, 0);
			}
			return txn_result(true, 0);
		}
		void DoCustomerPositionFrame1(const TCustomerPositionFrame1Input *pIn, TCustomerPositionFrame1Output *pOut);
		void DoCustomerPositionFrame2(const TCustomerPositionFrame2Input *pIn, TCustomerPositionFrame2Output *pOut);
		void DoCustomerPositionFrame3(void                                                                        );

		// MarketFeed transaction
		static txn_result MarketFeed(bench_worker *w)
		{
			ANON_REGION("MarketFeed:", &tpce_txn_cg);
			return static_cast<tpce_worker *>(w)->market_feed();
		}
		txn_result market_feed()
		{
			TMarketFeedTxnInput* input= MarketFeedInputBuffer->get();
			if( not input )
				return txn_result(false, 0);		// XXX. do we have to do this? MFQueue is empty, meaning no Trade-order submitted yet

			TMarketFeedTxnOutput output;
			CMarketFeed* harness= new CMarketFeed(this, this);

			try{
			harness->DoTxn( (PMarketFeedTxnInput)input, (PMarketFeedTxnOutput)&output);
			} catch (abstract_db::abstract_abort_exception &ex) {
				db->abort_txn(txn);
				return txn_result(false, 0);
			}
			return txn_result(true, 0);
		}
		void DoMarketFeedFrame1(const TMarketFeedFrame1Input *pIn, TMarketFeedFrame1Output *pOut, CSendToMarketInterface *pSendToMarket);

		// MarketWatch
		static txn_result MarketWatch(bench_worker *w)
		{
			ANON_REGION("MarketWatch:", &tpce_txn_cg);
			return static_cast<tpce_worker *>(w)->market_watch();
		}
		txn_result market_watch()
		{
			TMarketWatchTxnInput input;
			TMarketWatchTxnOutput output;
			m_TxnInputGenerator->GenerateMarketWatchInput(input);
			CMarketWatch* harness= new CMarketWatch(this);

			try{
			harness->DoTxn( (PMarketWatchTxnInput)&input, (PMarketWatchTxnOutput)&output);
			} catch (abstract_db::abstract_abort_exception &ex) {
				db->abort_txn(txn);
				return txn_result(false, 0);
			}
			return txn_result(true, 0);
		}
		void DoMarketWatchFrame1 (const TMarketWatchFrame1Input *pIn, TMarketWatchFrame1Output *pOut);

		// SecurityDetail
		static txn_result SecurityDetail(bench_worker *w)
		{
			ANON_REGION("SecurityDetail:", &tpce_txn_cg);
			return static_cast<tpce_worker *>(w)->security_detail();
		}
		txn_result security_detail()
		{
			TSecurityDetailTxnInput input;
			TSecurityDetailTxnOutput output;
			m_TxnInputGenerator->GenerateSecurityDetailInput(input);
			CSecurityDetail* harness= new CSecurityDetail(this);

			try{
			harness->DoTxn( (PSecurityDetailTxnInput)&input, (PSecurityDetailTxnOutput)&output);
			} catch (abstract_db::abstract_abort_exception &ex) {
				db->abort_txn(txn);
				return txn_result(false, 0);
			}
			return txn_result(true, 0);
		}
		void DoSecurityDetailFrame1(const TSecurityDetailFrame1Input *pIn, TSecurityDetailFrame1Output *pOut);

		// TradeLookup
		static txn_result TradeLookup(bench_worker *w)
		{
			ANON_REGION("TradeLookup:", &tpce_txn_cg);
			return static_cast<tpce_worker *>(w)->trade_lookup();
		}
		txn_result trade_lookup()
		{
			TTradeLookupTxnInput input;
			TTradeLookupTxnOutput output;
			m_TxnInputGenerator->GenerateTradeLookupInput(input);
			CTradeLookup* harness= new CTradeLookup(this);

			try{
//			harness->DoTxn( (PTradeLookupTxnInput)&input, (PTradeLookupTxnOutput)&output);
			} catch (abstract_db::abstract_abort_exception &ex) {
				db->abort_txn(txn);
				return txn_result(false, 0);
			}
			return txn_result(true, 0);
		}
		void DoTradeLookupFrame1(const TTradeLookupFrame1Input *pIn, TTradeLookupFrame1Output *pOut);
		void DoTradeLookupFrame2(const TTradeLookupFrame2Input *pIn, TTradeLookupFrame2Output *pOut);
		void DoTradeLookupFrame3(const TTradeLookupFrame3Input *pIn, TTradeLookupFrame3Output *pOut);
		void DoTradeLookupFrame4(const TTradeLookupFrame4Input *pIn, TTradeLookupFrame4Output *pOut);

		// TradeOrder
		static txn_result TradeOrder(bench_worker *w)
		{
			ANON_REGION("TradeOrder:", &tpce_txn_cg);
			return static_cast<tpce_worker *>(w)->trade_order();
		}
		txn_result trade_order()
		{
			TTradeOrderTxnInput input;
			TTradeOrderTxnOutput output;
			bool    bExecutorIsAccountOwner;
			int32_t iTradeType;
			m_TxnInputGenerator->GenerateTradeOrderInput(input, iTradeType, bExecutorIsAccountOwner);
			CTradeOrder* harness= new CTradeOrder(this, this);

			try{
				harness->DoTxn( (PTradeOrderTxnInput)&input, (PTradeOrderTxnOutput)&output);
			} catch (abstract_db::abstract_abort_exception &ex) {
				db->abort_txn(txn);
				return txn_result(false, 0);
			}
			return txn_result(true, 0);
		}
		void DoTradeOrderFrame1(const TTradeOrderFrame1Input *pIn, TTradeOrderFrame1Output *pOut);
		void DoTradeOrderFrame2(const TTradeOrderFrame2Input *pIn, TTradeOrderFrame2Output *pOut);
		void DoTradeOrderFrame3(const TTradeOrderFrame3Input *pIn, TTradeOrderFrame3Output *pOut);
		void DoTradeOrderFrame4(const TTradeOrderFrame4Input *pIn, TTradeOrderFrame4Output *pOut);
		void DoTradeOrderFrame5(void                                                            );
		void DoTradeOrderFrame6(void                                                            );

		// TradeResult
		static txn_result TradeResult(bench_worker *w)
		{
			ANON_REGION("TradeResult:", &tpce_txn_cg);
			return static_cast<tpce_worker *>(w)->trade_result();
		}
		txn_result trade_result()
		{
			TTradeResultTxnInput* input = TradeResultInputBuffer->get();
			if( not input )
				return txn_result(false, 0);		// XXX. do we have to do this? TRQueue is empty, meaning no Trade-order submitted yet

			TTradeResultTxnOutput output;
			CTradeResult* harness= new CTradeResult(this);

			try{
				harness->DoTxn( (PTradeResultTxnInput)input, (PTradeResultTxnOutput)&output);
			} catch (abstract_db::abstract_abort_exception &ex) {
				db->abort_txn(txn);
				return txn_result(false, 0);
			}
			return txn_result(true, 0);
		}
		void DoTradeResultFrame1(const TTradeResultFrame1Input *pIn, TTradeResultFrame1Output *pOut);
		void DoTradeResultFrame2(const TTradeResultFrame2Input *pIn, TTradeResultFrame2Output *pOut);
		void DoTradeResultFrame3(const TTradeResultFrame3Input *pIn, TTradeResultFrame3Output *pOut);
		void DoTradeResultFrame4(const TTradeResultFrame4Input *pIn, TTradeResultFrame4Output *pOut);
		void DoTradeResultFrame5(const TTradeResultFrame5Input *pIn                                );
		void DoTradeResultFrame6(const TTradeResultFrame6Input *pIn, TTradeResultFrame6Output *pOut);

		// TradeStatus
		static txn_result TradeStatus(bench_worker *w)
		{
			ANON_REGION("TradeStatus:", &tpce_txn_cg);
			return static_cast<tpce_worker *>(w)->trade_status();
		}
		txn_result trade_status()
		{
			TTradeStatusTxnInput input;
			TTradeStatusTxnOutput output;
			m_TxnInputGenerator->GenerateTradeStatusInput(input);
			CTradeStatus* harness= new CTradeStatus(this);

			try{
			harness->DoTxn( (PTradeStatusTxnInput)&input, (PTradeStatusTxnOutput)&output);
			} catch (abstract_db::abstract_abort_exception &ex) {
				db->abort_txn(txn);
				return txn_result(false, 0);
			}
			return txn_result(true, 0);
		}
		void DoTradeStatusFrame1(const TTradeStatusFrame1Input *pIn, TTradeStatusFrame1Output *pOut);

		// TradeUpdate
		static txn_result TradeUpdate(bench_worker *w)
		{
			ANON_REGION("TradeUpdate:", &tpce_txn_cg);
			return static_cast<tpce_worker *>(w)->trade_update();
		}
		txn_result trade_update()
		{
			TTradeUpdateTxnInput input;
			TTradeUpdateTxnOutput output;
			m_TxnInputGenerator->GenerateTradeUpdateInput(input);
			CTradeUpdate* harness= new CTradeUpdate(this);

			try{
//				harness->DoTxn( (PTradeUpdateTxnInput)&input, (PTradeUpdateTxnOutput)&output);
			} catch (abstract_db::abstract_abort_exception &ex) {
				db->abort_txn(txn);
				return txn_result(false, 0);
			}
			return txn_result(true, 0);
		}
		void DoTradeUpdateFrame1(const TTradeUpdateFrame1Input *pIn, TTradeUpdateFrame1Output *pOut);
		void DoTradeUpdateFrame2(const TTradeUpdateFrame2Input *pIn, TTradeUpdateFrame2Output *pOut);
		void DoTradeUpdateFrame3(const TTradeUpdateFrame3Input *pIn, TTradeUpdateFrame3Output *pOut);

		// DataMaintenance
		static txn_result DataMaintenance(bench_worker *w)
		{
			ANON_REGION("DataMaintenance:", &tpce_txn_cg);
			return static_cast<tpce_worker *>(w)->data_maintenance();
		}
		txn_result data_maintenance()
		{
			TDataMaintenanceTxnInput* input = m_CDM->createDMInput();
			TDataMaintenanceTxnOutput output;
			CDataMaintenance* harness= new CDataMaintenance(this);

			try{
			//	harness->DoTxn( (PDataMaintenanceTxnInput)&input, (PDataMaintenanceTxnOutput)&output);
			} catch (abstract_db::abstract_abort_exception &ex) {
				db->abort_txn(txn);
				return txn_result(false, 0);
			}
			return txn_result(true, 0);
		}
		void DoDataMaintenanceFrame1(const TDataMaintenanceFrame1Input *pIn);

		// TradeCleanup
		static txn_result TradeCleanup(bench_worker *w)
		{
			ANON_REGION("TradeCleanup:", &tpce_txn_cg);
			return static_cast<tpce_worker *>(w)->trade_cleanup();
		}
		txn_result trade_cleanup()
		{
			TTradeCleanupTxnInput*  input = m_CDM->createTCInput();
			TTradeCleanupTxnOutput output;
			CTradeCleanup* harness= new CTradeCleanup(this);

			try{
			//	harness->DoTxn( (PTradeCleanupTxnInput)&input, (PTradeCleanupTxnOutput)&output);
			} catch (abstract_db::abstract_abort_exception &ex) {
				db->abort_txn(txn);
				return txn_result(false, 0);
			}
			return txn_result(true, 0);
		}
		void DoTradeCleanupFrame1(const TTradeCleanupFrame1Input *pIn);

		virtual workload_desc_vec
			get_workload() const
			{

//static double g_txn_workload_mix[] = { 4.9, 13, 1, 18, 14, 8, 10.1, 10, 19, 2 }; 

				workload_desc_vec w;
				double m = 0;
				for (size_t i = 0; i < ARRAY_NELEMS(g_txn_workload_mix); i++)
					m += g_txn_workload_mix[i];
				ALWAYS_ASSERT(m == 100);
				if (g_txn_workload_mix[0])
					w.push_back(workload_desc("BrokerVolume", double(g_txn_workload_mix[0])/100.0, BrokerVolume));
				if (g_txn_workload_mix[1])
					w.push_back(workload_desc("CustomerPosition", double(g_txn_workload_mix[1])/100.0, CustomerPosition));
				if (g_txn_workload_mix[2])
					w.push_back(workload_desc("MarketFeed", double(g_txn_workload_mix[2])/100.0, MarketFeed));
				if (g_txn_workload_mix[3])
					w.push_back(workload_desc("MarketWatch", double(g_txn_workload_mix[3])/100.0, MarketWatch));
				if (g_txn_workload_mix[4])
					w.push_back(workload_desc("SecurityDetail", double(g_txn_workload_mix[4])/100.0, SecurityDetail));
				if (g_txn_workload_mix[5])
					w.push_back(workload_desc("TradeLookup", double(g_txn_workload_mix[5])/100.0, TradeLookup));
				if (g_txn_workload_mix[6])
					w.push_back(workload_desc("TradeOrder", double(g_txn_workload_mix[6])/100.0, TradeOrder));
				if (g_txn_workload_mix[7])
					w.push_back(workload_desc("TradeResult", double(g_txn_workload_mix[7])/100.0, TradeResult));
				if (g_txn_workload_mix[8])
					w.push_back(workload_desc("TradeStatus", double(g_txn_workload_mix[8])/100.0, TradeStatus));
				if (g_txn_workload_mix[9])
					w.push_back(workload_desc("TradeUpdate", double(g_txn_workload_mix[9])/100.0, TradeUpdate));
				//    if (g_txn_workload_mix[10])
				//      w.push_back(workload_desc("DataMaintenance", double(g_txn_workload_mix[10])/100.0, DataMaintenance));
				//    if (g_txn_workload_mix[11])
				//      w.push_back(workload_desc("TradeCleanup", double(g_txn_workload_mix[11])/100.0, TradeCleanup));
				return w;
			}

	protected:

		virtual void
			on_run_setup() OVERRIDE
			{
				if (!pin_cpus)
					return;
				const size_t a = worker_id % coreid::num_cpus_online();
				const size_t b = a % nthreads;
				RCU::pin_current_thread(b);
			}

		inline ALWAYS_INLINE string &
			str()
			{
				return *arena.next();
			}

	private:
		void* txn;
		const uint partition_id_start;
		const uint partition_id_end;

		// some scratch buffer space
		string obj_key0;
		string obj_key1;
		string obj_v;
};

void tpce_worker::DoBrokerVolumeFrame1(const TBrokerVolumeFrame1Input *pIn, TBrokerVolumeFrame1Output *pOut)
{
	/* SQL
	start transaction
	// Should return 0 to 40 rows
	select
		broker_name[] = B_NAME,
		volume[] = sum(TR_QTY * TR_BID_PRICE)
	from
		TRADE_REQUEST,
		SECTOR,
		INDUSTRY
		COMPANY,
		BROKER,
		SECURITY
	where
		TR_B_ID = B_ID and
		TR_S_SYMB = S_SYMB and
		S_CO_ID = CO_ID and
		CO_IN_ID = IN_ID and
		SC_ID = IN_SC_ID and
		B_NAME in (broker_list) and
		SC_NAME = sector_name
	group by
		B_NAME
	order by
		2 DESC

	// row_count will frequently be zero near the start of a Test Run when
	// TRADE_REQUEST table is mostly empty.
	list_len = row_count
	commit transaction
*/

	scoped_str_arena s_arena(arena);

	txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);

	const sector::key k_sc_0( pIn->sector_name, string(cSC_ID_len, (char)0	));
	const sector::key k_sc_1( pIn->sector_name, string(cSC_ID_len, (char)255));
	table_scanner sc_scanner(s_arena.get());
	tbl_sector(1)->scan(txn, Encode(obj_key0, k_sc_0), &Encode(obj_key1, k_sc_1), sc_scanner, s_arena.get());
	ALWAYS_ASSERT(sc_scanner.output.size() == 1);

	// Industry scan
	const industry::key k_in_0( string(cIN_ID_len, (char)0	) );
	const industry::key k_in_1( string(cIN_ID_len, (char)255) );
	table_scanner in_scanner(s_arena.get());
	tbl_industry(1)->scan(txn, Encode(obj_key0, k_in_0), &Encode(obj_key1, k_in_1), in_scanner, s_arena.get());
	ALWAYS_ASSERT(in_scanner.output.size());

	// Company scan
	const company::key k_co_0( 0 );
	const company::key k_co_1( numeric_limits<int64_t>::max() );
	table_scanner co_scanner(s_arena.get());
	tbl_company(1)->scan(txn, Encode(obj_key0, k_co_0), &Encode(obj_key1, k_co_1), co_scanner, s_arena.get());
	ALWAYS_ASSERT(co_scanner.output.size());

	// Security scan
	const security::key k_s_0( string(cSYMBOL_len, (char)0	) );
	const security::key k_s_1( string(cSYMBOL_len, (char)255) );
	table_scanner s_scanner(s_arena.get());
	tbl_security(1)->scan(txn, Encode(obj_key0, k_s_0), &Encode(obj_key1, k_s_1), s_scanner, s_arena.get());
	ALWAYS_ASSERT(s_scanner.output.size());

	/*
	const broker::key k_b_0( 0 );
	const broker::key k_b_1( numeric_limits<int64_t>::max() );
	table_scanner b_scanner(s_arena.get());
	tbl_broker(1)->scan(txn, Encode(obj_key0, k_b_0), &Encode(obj_key1, k_b_1), b_scanner, s_arena.get());
	ALWAYS_ASSERT(b_scanner.output.size());

	*/
	std::vector<std::pair<std::string *, const std::string*>> brokers;

	// BrokerVolume query processing
	for( auto i = 0; i < max_broker_list_len and pIn->broker_list[i] ; i++ )
	{
		const b_name_index::key k_b_0( string(pIn->broker_list[i]), 0 );
		const b_name_index::key k_b_1( string(pIn->broker_list[i]), numeric_limits<int64_t>::max() );
		table_scanner b_scanner(s_arena.get());
		tbl_b_name_index(1)->scan(txn, Encode(obj_key0, k_b_0), &Encode(obj_key1, k_b_1), b_scanner, s_arena.get());
		if( not b_scanner.output.size())
			continue;

		for( auto &r_b : b_scanner.output )
			brokers.push_back( r_b );
	}

	// NLJ
	pOut->list_len = 0;
	for( auto &r_sc: sc_scanner.output )
	{
		sector::key k_sc_temp;
		const sector::key* k_sc = Decode(*r_sc.first, k_sc_temp );

		for( auto &r_in: in_scanner.output)
		{
			industry::key k_in_temp;
			industry::value v_in_temp;
			const industry::key* k_in = Decode(*r_in.first, k_in_temp );
			const industry::value* v_in = Decode(*r_in.second, v_in_temp );

			if( k_sc->sc_id != v_in->in_sc_id )
				continue;

			for( auto &r_co : co_scanner.output )
			{
				company::key k_co_temp;
				company::value v_co_temp;
				const company::key* k_co = Decode(*r_co.first, k_co_temp );
				const company::value* v_co = Decode(*r_co.second, v_co_temp );

				if( k_in->in_id != v_co->co_in_id )
					continue;

				for( auto &r_s : s_scanner.output )
				{
					security::key k_s_temp;
					security::value v_s_temp;
					const security::key* k_s = Decode( *r_s.first, k_s_temp );
					const security::value* v_s = Decode(*r_s.second, v_s_temp );

					if( v_s->s_co_id != k_co->co_id )
						continue;

					for( auto &r_b_idx : brokers )
					{
						b_name_index::key k_b_idx_temp;
						const b_name_index::key* k_b_idx = Decode( *r_b_idx.first, k_b_idx_temp );

						const trade_request::key k_tr( k_s->s_symb, k_b_idx->b_id );
						if(tbl_trade_request(1)->get(txn, Encode(obj_key0, k_tr), obj_v))
						{
							trade_request::value v_tr_temp;
							const trade_request::value *v_tr = Decode(obj_v, v_tr_temp);

							// TODO. group by
							// TODO. order by
							memcpy(pOut->broker_name[pOut->list_len],  k_b_idx->b_name.data(), k_b_idx->b_name.size());
							pOut->volume[pOut->list_len] = v_tr->tr_bid_price * v_tr->tr_qty; 
							pOut->list_len++;
						}
					}
				}
			}
		}
	}
	db->commit_txn(txn);
}

void tpce_worker::DoCustomerPositionFrame1(const TCustomerPositionFrame1Input *pIn, TCustomerPositionFrame1Output *pOut)
{
	scoped_str_arena s_arena(arena);
	txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);

	// Get c_id;
	const c_tax_id_index::key k_c_0( pIn->tax_id, 0 );
	const c_tax_id_index::key k_c_1( pIn->tax_id, numeric_limits<int64_t>::max() );
	table_scanner c_scanner(s_arena.get());

	if(pIn->cust_id)
		pOut->cust_id = pIn->cust_id;
	else
	{
		tbl_c_tax_id_index(1)->scan(txn, Encode(obj_key0, k_c_0), &Encode(obj_key1, k_c_1), c_scanner, s_arena.get());
		ALWAYS_ASSERT(c_scanner.output.size());
		c_tax_id_index::key k_c_temp;
		const c_tax_id_index::key* k_c = Decode( *(c_scanner.output.front().first), k_c_temp );
		pOut->cust_id = k_c->c_id;
	}
	ALWAYS_ASSERT( pOut->cust_id );

	// probe Customers
	const customers::key k_c(pOut->cust_id);
	ALWAYS_ASSERT(tbl_customers(1)->get(txn, Encode(obj_key0, k_c), obj_v));
    customers::value v_c_temp;
    const customers::value *v_c = Decode(obj_v, v_c_temp);

    memcpy(pOut->c_st_id, v_c->c_st_id.data(), v_c->c_st_id.size() );
    memcpy(pOut->c_l_name, v_c->c_l_name.data(), v_c->c_l_name.size());
	memcpy(pOut->c_f_name, v_c->c_f_name.data(), v_c->c_f_name.size());
    memcpy(pOut->c_m_name, v_c->c_m_name.data(), v_c->c_m_name.size());
    pOut->c_gndr[0] = v_c->c_gndr; pOut->c_gndr[1] = 0;
    pOut->c_tier = v_c->c_tier;
    pOut->c_dob = TimeTToTimeStamp((time_t)v_c->c_dob);
    pOut->c_ad_id = v_c->c_ad_id;
    memcpy(pOut->c_ctry_1, v_c->c_ctry_1.data(), v_c->c_ctry_1.size());
    memcpy(pOut->c_area_1, v_c->c_area_1.data(), v_c->c_area_1.size());
    memcpy(pOut->c_local_1, v_c->c_local_1.data(), v_c->c_local_1.size());
    memcpy(pOut->c_ext_1, v_c->c_ext_1.data(), v_c->c_ext_1.size());
    memcpy(pOut->c_ctry_2, v_c->c_ctry_2.data(), v_c->c_ctry_2.size());
    memcpy(pOut->c_area_2, v_c->c_area_2.data(), v_c->c_area_2.size());
    memcpy(pOut->c_local_2, v_c->c_local_2.data(), v_c->c_local_2.size());
    memcpy(pOut->c_ext_2, v_c->c_ext_2.data(), v_c->c_ext_2.size());
    memcpy(pOut->c_ctry_3, v_c->c_ctry_3.data(), v_c->c_ctry_3.size());
    memcpy(pOut->c_area_3, v_c->c_area_3.data(), v_c->c_area_3.size());
    memcpy(pOut->c_local_3, v_c->c_local_3.data(), v_c->c_local_3.size());
    memcpy(pOut->c_ext_3, v_c->c_ext_3.data(), v_c->c_ext_3.size());
    memcpy(pOut->c_email_1, v_c->c_email_1.data(), v_c->c_email_1.size());
    memcpy(pOut->c_email_2, v_c->c_email_2.data(), v_c->c_email_2.size());

/*
	const customer_account::key k_ca_0( 0 );
	const customer_account::key k_ca_1( numeric_limits<int64_t>::max() );
	table_scanner ca_scanner(s_arena.get());
	tbl_customer_account(1)->scan(txn, Encode(obj_key0, k_ca_0), &Encode(obj_key1, k_ca_1), ca_scanner, s_arena.get());
	cout << ca_scanner.output.size() << endl;
	//ALWAYS_ASSERT( ca_scanner.output.size() );

	*/

	// CustomerAccount scan
	const ca_id_index::key k_ca_0( pOut->cust_id, 0 );
	const ca_id_index::key k_ca_1( pOut->cust_id, numeric_limits<int64_t>::max() );
	table_scanner ca_scanner(s_arena.get());
	tbl_ca_id_index(1)->scan(txn, Encode(obj_key0, k_ca_0), &Encode(obj_key1, k_ca_1), ca_scanner, s_arena.get());
	cout << ca_scanner.output.size() << endl;
	ALWAYS_ASSERT( ca_scanner.output.size() );
	// XXX. input customer ID doesn't exist?

	// HoldingSummary scan
	const holding_summary::key k_hs_0( 0, 							   string(cSYMBOL_len, (char)0	) );
	const holding_summary::key k_hs_1( numeric_limits<int64_t>::max(), string(cSYMBOL_len, (char)255) );
	table_scanner hs_scanner(s_arena.get());
	tbl_holding_summary(1)->scan(txn, Encode(obj_key0, k_hs_0), &Encode(obj_key1, k_hs_1), hs_scanner, s_arena.get());
	ALWAYS_ASSERT( hs_scanner.output.size() );

	for( auto& r_ca : ca_scanner.output )
	{
		ca_id_index::key k_ca_temp;
		ca_id_index::value v_ca_temp;
		const ca_id_index::key* k_ca = Decode( *r_ca.first, k_ca_temp );
		const ca_id_index::value* v_ca = Decode(*r_ca.second, v_ca_temp );
		
		auto asset = 0;
		for( auto& r_hs : hs_scanner.output )
		{
			holding_summary::key k_hs_temp;
			holding_summary::value v_hs_temp;
			const holding_summary::key* k_hs = Decode( *r_hs.first, k_hs_temp );
			const holding_summary::value* v_hs = Decode(*r_hs.second, v_hs_temp );

			if(  k_ca->ca_id == k_hs->hs_ca_id )
			{
				// LastTrade probe & equi-join
				const last_trade::key k_lt(k_hs->hs_s_symb);
				ALWAYS_ASSERT(tbl_last_trade(1)->get(txn, Encode(obj_key0, k_lt), obj_v));
				last_trade::value v_lt_temp;
				const last_trade::value *v_lt = Decode(obj_v, v_lt_temp);

				asset += v_hs->hs_qty * v_lt->lt_price;
			}
		}

		// TODO.  aggregation( if <CA_ID,CA_BAL> is not unique ) and sorting.
		pOut->acct_id[pOut->acct_len] = k_ca->ca_id;
		pOut->cash_bal[pOut->acct_len] = v_ca->ca_bal;
		pOut->asset_total[pOut->acct_len] = asset;
		pOut->acct_len++;
	}
}

void tpce_worker::DoCustomerPositionFrame2(const TCustomerPositionFrame2Input *pIn, TCustomerPositionFrame2Output *pOut)
{
	scoped_str_arena s_arena(arena);
	// Trade scan and collect 10 TID
	const t_ca_id_index::key k_t_0( pIn->acct_id, 0, 0 );
	const t_ca_id_index::key k_t_1( pIn->acct_id, numeric_limits<int64_t>::max(), numeric_limits<int64_t>::max() );
	table_scanner t_scanner(s_arena.get());
	tbl_t_ca_id_index(1)->scan(txn, Encode(obj_key0, k_t_0), &Encode(obj_key1, k_t_1), t_scanner, s_arena.get());
	ALWAYS_ASSERT( t_scanner.output.size() );

	std::vector<std::pair<std::string *, const std::string*>> tids;
	for( auto &r_t : t_scanner.output )
	{
		t_ca_id_index::value v_t_temp;
		const t_ca_id_index::value* v_t = Decode(*r_t.second, v_t_temp );

		tids.push_back( r_t );
		if( tids.size() >= 10 )
			break;
	}
	reverse(tids.begin(), tids.end());

	// Join
	const trade_history::key k_th_0( 0, 0 );
	const trade_history::key k_th_1(numeric_limits<int64_t>::max(), numeric_limits<int64_t>::max() );
	table_scanner th_scanner(s_arena.get());
	tbl_trade_history(1)->scan(txn, Encode(obj_key0, k_th_0), &Encode(obj_key1, k_th_1), th_scanner, s_arena.get());
	ALWAYS_ASSERT( th_scanner.output.size() );

	const status_type::key k_st_0( string(cST_ID_len, (char)0	) );
	const status_type::key k_st_1( string(cST_ID_len, (char)255) );
	table_scanner st_scanner(s_arena.get());
	tbl_status_type(1)->scan(txn, Encode(obj_key0, k_st_0), &Encode(obj_key1, k_st_1), st_scanner, s_arena.get());
	ALWAYS_ASSERT( st_scanner.output.size() );

	for( auto &r_t : tids )
	{
		trade::key k_t_temp;
		trade::value v_t_temp;
		const trade::key* k_t = Decode( *r_t.first, k_t_temp );
		const trade::value* v_t = Decode(*r_t.second, v_t_temp );

		for( auto &r_th : th_scanner.output )
		{
			trade_history::key k_th_temp;
			trade_history::value v_th_temp;
			const trade_history::key* k_th = Decode( *r_th.first, k_th_temp );
			const trade_history::value* v_th = Decode(*r_th.second, v_th_temp );

			if( k_t->t_id != k_th->th_t_id )
				continue;

			for( auto &r_st : st_scanner.output )
			{
				status_type::key k_st_temp;
				status_type::value v_st_temp;
				const status_type::key* k_st = Decode( *r_st.first, k_st_temp );
				const status_type::value* v_st = Decode(*r_st.second, v_st_temp );

				if( v_th->th_st_id != k_st->st_id )
					continue;

				// TODO. order by and grab 30 rows
				pOut->trade_id[pOut->hist_len] 	= k_t->t_id;
				pOut->qty[pOut->hist_len] 		= v_t->t_qty;
				pOut->hist_dts[pOut->hist_len] = TimeTToTimeStamp((time_t)k_th->th_dts);
				memcpy(pOut->symbol[pOut->hist_len], v_t->t_s_symb.data(), v_t->t_s_symb.size());
				memcpy(pOut->trade_status[pOut->hist_len], v_st->st_name.data(), v_st->st_name.size());

				pOut->hist_len++;
				if( pOut->hist_len >= max_hist_len )
					goto commit;
			}

		}

	}
commit:
	db->commit_txn(txn);
}

void tpce_worker::DoCustomerPositionFrame3(void)
{
	db->commit_txn(txn);
}

void tpce_worker::DoMarketFeedFrame1(const TMarketFeedFrame1Input *pIn, TMarketFeedFrame1Output *pOut, CSendToMarketInterface *pSendToMarket)
{
	scoped_str_arena s_arena(arena);

	auto now_dts = time(0);	
	auto rows_updated = 0;
	vector<TTradeRequest> TradeRequestBuffer;
	double req_price_quote = 0; 
	uint64_t req_trade_id = 0; 
	int32_t req_trade_qty = 0; 
    inline_str_fixed<cTT_ID_len> req_trade_type;
	auto rows_sent = 0;

	TStatusAndTradeType type = pIn->StatusAndTradeType;
	for( int i = 0; i < max_feed_len; i++ )
	{
		txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);
		TTickerEntry ticker = pIn->Entries[i];

		last_trade::key k_lt(ticker.symbol);
		ALWAYS_ASSERT(tbl_last_trade(1)->get(txn, Encode(obj_key0, k_lt), obj_v));
		last_trade::value v_lt_temp;
		const last_trade::value *v_lt = Decode(obj_v, v_lt_temp);
		last_trade::value v_lt_new(*v_lt);
		v_lt_new.lt_dts = now_dts;
		v_lt_new.lt_price = v_lt->lt_price + ticker.price_quote;
		v_lt_new.lt_vol = ticker.price_quote;
		tbl_last_trade(1)->put(txn, Encode(obj_key0, k_lt), Encode(obj_v, v_lt_new));

		rows_updated++;

		const trade_request::key k_tr_0( string(ticker.symbol), 0 );
		const trade_request::key k_tr_1( string(ticker.symbol), numeric_limits<uint64_t>::max() );
		table_scanner tr_scanner(s_arena.get());
		tbl_trade_request(1)->scan(txn, Encode(obj_key0, k_tr_0), &Encode(obj_key1, k_tr_1), tr_scanner, s_arena.get());
		//ALWAYS_ASSERT( tr_scanner.output.size() );			// XXX. If there's no previous trade, this can happen

		std::vector<std::pair<std::string *, const std::string*>> request_list_cursor;
		for( auto &r_tr : tr_scanner.output )
		{
			trade_request::key k_tr_temp;
			trade_request::value v_tr_temp;
			const trade_request::key* k_tr = Decode( *r_tr.first, k_tr_temp );
			const trade_request::value* v_tr = Decode(*r_tr.second, v_tr_temp );


			//cout << "MF : " << v_tr->tr_tt_id << "," << v_tr->tr_bid_price << endl;
			if( (v_tr->tr_tt_id == string(type.type_stop_loss) and v_tr->tr_bid_price >= ticker.price_quote) or
				(v_tr->tr_tt_id == string(type.type_limit_sell) and v_tr->tr_bid_price <= ticker.price_quote) or
				(v_tr->tr_tt_id == string(type.type_limit_buy) and v_tr->tr_bid_price >= ticker.price_quote) )
			{
				request_list_cursor.push_back( r_tr );
			}
		}

		for( auto &r_tr : request_list_cursor )
		{
			trade_request::key k_tr_temp;
			trade_request::value v_tr_temp;
			const trade_request::key* k_tr = Decode( *r_tr.first, k_tr_temp );
			const trade_request::value* v_tr = Decode(*r_tr.second, v_tr_temp );

			req_trade_id = v_tr->tr_t_id;
			req_price_quote = v_tr->tr_bid_price;
			req_trade_type = v_tr->tr_tt_id;
			req_trade_qty = v_tr->tr_qty;

			const trade::key k_t(req_trade_id);
			ALWAYS_ASSERT(tbl_trade(1)->get(txn, Encode(obj_key0, k_t), obj_v));
			trade::value v_t_temp;
			const trade::value *v_t = Decode(obj_v, v_t_temp);
			trade::value v_t_new(*v_t);
			v_t_new.t_dts = now_dts;
			v_t_new.t_st_id = string(type.status_submitted);
			tbl_trade(1)->put(txn, Encode(k_t), Encode(obj_v, v_t_new));

			t_ca_id_index::key k_t_idx1;
			t_ca_id_index::value v_t_idx1;
			k_t_idx1.t_ca_id 		= v_t_new.t_ca_id;
			k_t_idx1.t_dts 			= v_t_new.t_dts;
			k_t_idx1.t_id 			= k_t.t_id;
			v_t_idx1.t_st_id 		= v_t_new.t_st_id ;
			v_t_idx1.t_tt_id 		= v_t_new.t_tt_id ;
			v_t_idx1.t_is_cash 		= v_t_new.t_is_cash ;
			v_t_idx1.t_s_symb 		= v_t_new.t_s_symb ;
			v_t_idx1.t_qty 			= v_t_new.t_qty ;
			v_t_idx1.t_bid_price 	= v_t_new.t_bid_price ;
			v_t_idx1.t_exec_name 	= v_t_new.t_exec_name ;
			v_t_idx1.t_trade_price 	= v_t_new.t_trade_price ;
			v_t_idx1.t_chrg 		= v_t_new.t_chrg ;
			tbl_t_ca_id_index(1)->put(txn, Encode(k_t_idx1), Encode(obj_v, v_t_idx1));

			t_s_symb_index::key k_t_idx2;
			t_s_symb_index::value v_t_idx2;
			k_t_idx2.t_s_symb 		= v_t_new.t_s_symb ;
			k_t_idx2.t_dts 			= v_t_new.t_dts;
			k_t_idx2.t_id 			= k_t.t_id;
			v_t_idx2.t_ca_id 		= v_t_new.t_ca_id;
			v_t_idx2.t_st_id 		= v_t_new.t_st_id ;
			v_t_idx2.t_tt_id 		= v_t_new.t_tt_id ;
			v_t_idx2.t_is_cash 		= v_t_new.t_is_cash ;
			v_t_idx2.t_qty 			= v_t_new.t_qty ;
			v_t_idx2.t_exec_name 	= v_t_new.t_exec_name ;
			v_t_idx2.t_trade_price 	= v_t_new.t_trade_price ;
			tbl_t_s_symb_index(1)->put(txn, Encode(k_t_idx2), Encode(obj_v, v_t_idx2));

			trade_request::key k_tr_new(*k_tr);
			tbl_trade_request(1)->remove(txn, Encode(k_tr_new));

			trade_history::key k_th;
			trade_history::value v_th;
			k_th.th_t_id = req_trade_id;
			k_th.th_dts = now_dts;
			v_th.th_st_id = string(type.status_submitted);
			tbl_trade_history(1)->insert(txn, Encode(k_th), Encode(obj_v, v_th));

			TTradeRequest request;
			memcpy(request.symbol, ticker.symbol, cSYMBOL_len+1);
			request.trade_id = req_trade_id;
			request.price_quote = req_price_quote;
			request.trade_qty = req_trade_qty;
			memcpy(request.trade_type_id, req_trade_type.data(), req_trade_type.size());
			TradeRequestBuffer.emplace_back( request );
			rows_sent = rows_sent + 1;
		}

		db->commit_txn(txn);

		pOut->send_len += rows_sent;
		for( auto i = 0; i < rows_sent; i++ )
		{
			SendToMarketFromFrame(TradeRequestBuffer[i]);
/*			cout << "MF: " << 
					TradeRequestBuffer[i].symbol 				<< "," <<
					TradeRequestBuffer[i].trade_id				<< "," <<
					TradeRequestBuffer[i].price_quote			<< "," <<
					TradeRequestBuffer[i].trade_qty				<< "," <<
					TradeRequestBuffer[i].trade_type_id			<< endl;
					*/
		}
	}
}

void tpce_worker::DoMarketWatchFrame1 (const TMarketWatchFrame1Input *pIn, TMarketWatchFrame1Output *pOut)
{
	scoped_str_arena s_arena(arena);
	txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);

	std::vector<inline_str_fixed<cSYMBOL_len>> stock_list_cursor;

	if( pIn->c_id )
	{
		/*
		const watch_item::key k_wi_0( 0,  							 string(cSYMBOL_len, (char)0  ) );
		const watch_item::key k_wi_1( numeric_limits<int64_t>::max(),string(cSYMBOL_len, (char)255) );
		table_scanner wi_scanner(s_arena.get());
		tbl_watch_item(1)->scan(txn, Encode(obj_key0, k_wi_0), &Encode(obj_key1, k_wi_1), wi_scanner, s_arena.get());
		ALWAYS_ASSERT( wi_scanner.output.size() );
	*/	
		const watch_list::key k_wl_0( pIn->c_id, 0 );
		const watch_list::key k_wl_1( pIn->c_id, numeric_limits<int64_t>::max() );
		table_scanner wl_scanner(s_arena.get());
		tbl_watch_list(1)->scan(txn, Encode(obj_key0, k_wl_0), &Encode(obj_key1, k_wl_1), wl_scanner, s_arena.get());
		ALWAYS_ASSERT( wl_scanner.output.size() );

		for( auto &r_wl: wl_scanner.output )
		{
			watch_list::key k_wl_temp;
			const watch_list::key* k_wl = Decode( *r_wl.first, k_wl_temp );

			const watch_item::key k_wi(k_wl->wl_id);
			if( not tbl_watch_item(1)->get(txn, Encode(obj_key0, k_wi), obj_v))
				continue;

			watch_item::value v_wi_temp;
			const watch_item::value *v_wi = Decode(obj_v, v_wi_temp);
			stock_list_cursor.push_back( v_wi->wi_s_symb );

		/*
			for( auto &r_wi : wi_scanner.output )
			{
				watch_item::key k_wi_temp;
				const watch_item::key* k_wi = Decode( *r_wi.first, k_wi_temp );


				if( k_wi->wi_wl_id == k_wl->wl_id )
			}
			*/
		}
	}
	else if ( pIn->industry_name[0] )
	{
		const in_name_index::key k_in_0( string(pIn->industry_name), string(cIN_ID_len, (char)0  )  );
		const in_name_index::key k_in_1( string(pIn->industry_name), string(cIN_ID_len, (char)255)  );
		table_scanner in_scanner(s_arena.get());
		tbl_in_name_index(1)->scan(txn, Encode(obj_key0, k_in_0), &Encode(obj_key1, k_in_1), in_scanner, s_arena.get());
		ALWAYS_ASSERT( in_scanner.output.size() );
		
		const company::key k_co_0( pIn->starting_co_id );
		const company::key k_co_1( pIn->ending_co_id );
		table_scanner co_scanner(s_arena.get());
		tbl_company(1)->scan(txn, Encode(obj_key0, k_co_0), &Encode(obj_key1, k_co_1), co_scanner, s_arena.get());
		ALWAYS_ASSERT( co_scanner.output.size() );

		const security::key k_s_0( string(cSYMBOL_len, (char)0  ));
		const security::key k_s_1( string(cSYMBOL_len, (char)255));
		table_scanner s_scanner(s_arena.get());
		tbl_security(1)->scan(txn, Encode(obj_key0, k_s_0), &Encode(obj_key1, k_s_1), s_scanner, s_arena.get());
		ALWAYS_ASSERT( s_scanner.output.size() );

		for( auto &r_in : in_scanner.output )
		{
			in_name_index::key k_in_temp;
			in_name_index::value v_in_temp;
			const in_name_index::key* k_in = Decode( *r_in.first, k_in_temp );
			const in_name_index::value* v_in = Decode( *r_in.second, v_in_temp );

			for( auto &r_co: co_scanner.output )
			{
				company::key k_co_temp;
				company::value v_co_temp;
				const company::key* k_co = Decode( *r_co.first, k_co_temp );
				const company::value* v_co = Decode( *r_co.second, v_co_temp );

				if( v_co->co_in_id != k_in->in_id )
					continue;
				
				for( auto &r_s : s_scanner.output )
				{
					security::key k_s_temp;
					security::value v_s_temp;
					const security::key* k_s = Decode( *r_s.first, k_s_temp );
					const security::value* v_s = Decode( *r_s.second, v_s_temp );

					if( v_s->s_co_id == k_co->co_id )
					{
						stock_list_cursor.push_back( k_s->s_symb );
					}
				}
			}
		}
	}
	else if( pIn->acct_id )
	{
		const holding_summary::key k_hs_0( pIn->acct_id, string(cSYMBOL_len, (char)0  ) );
		const holding_summary::key k_hs_1( pIn->acct_id, string(cSYMBOL_len, (char)255) );
		table_scanner hs_scanner(s_arena.get());
		tbl_holding_summary(1)->scan(txn, Encode(obj_key0, k_hs_0), &Encode(obj_key1, k_hs_1), hs_scanner, s_arena.get());
		ALWAYS_ASSERT( hs_scanner.output.size() );

		for( auto& r_hs : hs_scanner.output )
		{
			holding_summary::key k_hs_temp;
			const holding_summary::key* k_hs = Decode( *r_hs.first, k_hs_temp );

			stock_list_cursor.push_back( k_hs->hs_s_symb );
		}
	}
	else
		ALWAYS_ASSERT(false);

    double old_mkt_cap = 0;
    double new_mkt_cap = 0;

	for( auto &s : stock_list_cursor )
	{
		const last_trade::key k_lt(s);
		ALWAYS_ASSERT(tbl_last_trade(1)->get(txn, Encode(obj_key0, k_lt), obj_v));
		last_trade::value v_lt_temp;
		const last_trade::value *v_lt = Decode(obj_v, v_lt_temp);

		const security::key k_s(s);
		ALWAYS_ASSERT(tbl_security(1)->get(txn, Encode(obj_key0, k_s), obj_v));
		security::value v_s_temp;
		const security::value *v_s = Decode(obj_v, v_s_temp);

		const daily_market::key k_dm(s, TimeStampToTimeT(pIn->start_day) );
		ALWAYS_ASSERT(tbl_daily_market(1)->get(txn, Encode(obj_key0, k_dm), obj_v));
		daily_market::value v_dm_temp;
		const daily_market::value *v_dm = Decode(obj_v, v_dm_temp);

		auto s_num_out = v_s->s_num_out;
		auto old_price = v_dm->dm_close;
		auto new_price = v_lt->lt_price;

		old_mkt_cap += s_num_out * old_price;
		new_mkt_cap += s_num_out * new_price;
	}

	if( old_mkt_cap != 0 )
		pOut->pct_change = 100 * (new_mkt_cap / old_mkt_cap - 1);
	else
		pOut->pct_change = 0;

	db->commit_txn(txn);
}

void tpce_worker::DoSecurityDetailFrame1(const TSecurityDetailFrame1Input *pIn, TSecurityDetailFrame1Output *pOut)
{
	scoped_str_arena s_arena(arena);
	txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);

	int64_t co_id;

	const security::key k_s(pIn->symbol);
	ALWAYS_ASSERT(tbl_security(1)->get(txn, Encode(obj_key0, k_s), obj_v));
	security::value v_s_temp;
	const security::value *v_s = Decode(obj_v, v_s_temp);
	co_id = v_s->s_co_id;

	const company::key k_co(co_id);
	ALWAYS_ASSERT(tbl_company(1)->get(txn, Encode(obj_key0, k_co), obj_v));
	company::value v_co_temp;
	const company::value *v_co = Decode(obj_v, v_co_temp);

	const address::key k_ca(v_co->co_ad_id);
	ALWAYS_ASSERT(tbl_address(1)->get(txn, Encode(obj_key0, k_ca), obj_v));
	address::value v_ca_temp;
	const address::value *v_ca = Decode(obj_v, v_ca_temp);

	const zip_code::key k_zca(v_ca->ad_zc_code);
	ALWAYS_ASSERT(tbl_zip_code(1)->get(txn, Encode(obj_key0, k_zca), obj_v));
	zip_code::value v_zca_temp;
	const zip_code::value *v_zca = Decode(obj_v, v_zca_temp);

	const exchange::key k_ex(v_s->s_ex_id);
	ALWAYS_ASSERT(tbl_exchange(1)->get(txn, Encode(obj_key0, k_ex), obj_v));
	exchange::value v_ex_temp;
	const exchange::value *v_ex = Decode(obj_v, v_ex_temp);

	const address::key k_ea(v_ex->ex_ad_id);
	ALWAYS_ASSERT(tbl_address(1)->get(txn, Encode(obj_key0, k_ea), obj_v));
	address::value v_ea_temp;
	const address::value *v_ea = Decode(obj_v, v_ea_temp);

	const zip_code::key k_zea(v_ea->ad_zc_code);
	ALWAYS_ASSERT(tbl_zip_code(1)->get(txn, Encode(obj_key0, k_zea), obj_v));
	zip_code::value v_zea_temp;
	const zip_code::value *v_zea = Decode(obj_v, v_zea_temp);
	
	memcpy(pOut->s_name,  v_s->s_name.data(), v_s->s_name.size());
	pOut->num_out = v_s->s_num_out;
	pOut->start_date = TimeTToTimeStamp((time_t)v_s->s_start_date);
	pOut->ex_date= TimeTToTimeStamp((time_t)v_s->s_exch_date);
	pOut->pe_ratio = v_s->s_pe;
	pOut->s52_wk_high = v_s->s_52wk_high;
	pOut->s52_wk_high_date = TimeTToTimeStamp((time_t)v_s->s_52wk_high_date);
	pOut->s52_wk_low = v_s->s_52wk_low;
	pOut->s52_wk_low_date = TimeTToTimeStamp((time_t)v_s->s_52wk_low_date);
	pOut->divid = v_s->s_dividend;
	pOut->yield = v_s->s_yield;
	memcpy(pOut->co_name,  v_co->co_name.data(), v_co->co_name.size());
	memcpy(pOut->sp_rate,  v_co->co_sp_rate.data(), v_co->co_sp_rate.size());
	memcpy(pOut->ceo_name,  v_co->co_ceo.data(), v_co->co_ceo.size());
	memcpy(pOut->co_desc,  v_co->co_desc.data(), v_co->co_desc.size());
	pOut->open_date = TimeTToTimeStamp((time_t)v_co->co_open_date);
	memcpy(pOut->co_st_id,  v_co->co_st_id.data(), v_co->co_st_id.size());
	memcpy(pOut->co_ad_line1,  v_ca->ad_line1.data(), v_ca->ad_line1.size());
	memcpy(pOut->co_ad_line2,  v_ca->ad_line2.data(), v_ca->ad_line2.size());
	memcpy(pOut->co_ad_zip,  v_ca->ad_zc_code.data(), v_ca->ad_zc_code.size());
	memcpy(pOut->co_ad_cty,  v_ca->ad_ctry.data(), v_ca->ad_ctry.size());
	memcpy(pOut->ex_ad_line1,  v_ea->ad_line1.data(), v_ea->ad_line1.size());
	memcpy(pOut->ex_ad_line2,  v_ea->ad_line2.data(), v_ea->ad_line2.size());
	memcpy(pOut->ex_ad_zip,  v_ea->ad_zc_code.data(), v_ea->ad_zc_code.size());
	memcpy(pOut->ex_ad_cty,  v_ea->ad_ctry.data(), v_ea->ad_ctry.size());
	pOut->ex_open = v_ex->ex_open;
	pOut->ex_close = v_ex->ex_close;
	pOut->ex_num_symb = v_ex->ex_num_symb;
	memcpy(pOut->ex_name,  v_ex->ex_name.data(), v_ex->ex_name.size());
	memcpy(pOut->ex_desc,  v_ex->ex_desc.data(), v_ex->ex_desc.size());
	memcpy(pOut->co_ad_town, v_zca->zc_town.data(), v_zca->zc_town.size());
	memcpy(pOut->co_ad_div,  v_zca->zc_div.data(), v_zca->zc_div.size());
	memcpy(pOut->ex_ad_town, v_zea->zc_town.data(), v_zea->zc_town.size());
	memcpy(pOut->ex_ad_div,  v_zea->zc_div.data(),  v_zea->zc_div.size());


	const company_competitor::key k_cp_0( co_id, 0, 							 string(cIN_ID_len, (char)0  ));
	const company_competitor::key k_cp_1( co_id, numeric_limits<int64_t>::max(), string(cIN_ID_len, (char)255));
	table_scanner cp_scanner(s_arena.get());
	tbl_company_competitor(1)->scan(txn, Encode(obj_key0, k_cp_0), &Encode(obj_key1, k_cp_1), cp_scanner, s_arena.get());
	ALWAYS_ASSERT( cp_scanner.output.size() );

	for(auto i = 0; i < max_comp_len; i++ )
	{
		auto &r_cp = cp_scanner.output[i];
		company_competitor::key k_cp_temp;
		const company_competitor::key* k_cp = Decode( *r_cp.first, k_cp_temp );

		const company::key k_co3(k_cp->cp_comp_co_id);
		ALWAYS_ASSERT(tbl_company(1)->get(txn, Encode(obj_key0, k_co3), obj_v));
		company::value v_co3_temp;
		const company::value *v_co3 = Decode(obj_v, v_co3_temp);
		 
		const industry::key k_in(k_cp->cp_in_id);
		ALWAYS_ASSERT(tbl_industry(1)->get(txn, Encode(obj_key0, k_in), obj_v));
		industry::value v_in_temp;
		const industry::value *v_in = Decode(obj_v, v_in_temp);

		memcpy( pOut->cp_co_name[i], v_co3->co_name.data(), v_co3->co_name.size() );
		memcpy( pOut->cp_in_name[i], v_in->in_name.data(), v_in->in_name.size() );
	}

	const financial::key k_fi_0( co_id, 0, 0 );
	const financial::key k_fi_1( co_id, numeric_limits<int32_t>::max(), numeric_limits<int32_t>::max() );
	table_scanner fi_scanner(s_arena.get());
	tbl_financial(1)->scan(txn, Encode(obj_key0, k_fi_0), &Encode(obj_key1, k_fi_1), fi_scanner, s_arena.get());
	ALWAYS_ASSERT( fi_scanner.output.size() );
	for( uint64_t i = 0; i < max_fin_len; i++ )
	{
		auto &r_fi = fi_scanner.output[i];
		financial::key k_fi_temp;
		financial::value v_fi_temp;
		const financial::key* k_fi = Decode( *r_fi.first, k_fi_temp );
		const financial::value* v_fi = Decode( *r_fi.second, v_fi_temp );

		// TODO. order by

		pOut->fin[i].year = k_fi->fi_year;
		pOut->fin[i].qtr = k_fi->fi_qtr;
		pOut->fin[i].start_date = TimeTToTimeStamp((time_t)v_fi->fi_qtr_start_date);
		pOut->fin[i].rev = v_fi->fi_revenue;
		pOut->fin[i].net_earn = v_fi->fi_net_earn;
		pOut->fin[i].basic_eps = v_fi->fi_basic_eps;
		pOut->fin[i].dilut_eps = v_fi->fi_dilut_eps;
		pOut->fin[i].margin = v_fi->fi_margin;
		pOut->fin[i].invent = v_fi->fi_inventory;
		pOut->fin[i].assets = v_fi->fi_assets;
		pOut->fin[i].liab = v_fi->fi_liability;
		pOut->fin[i].out_basic = v_fi->fi_out_basic;
		pOut->fin[i].out_dilut = v_fi->fi_out_dilut;

	}
	pOut->fin_len = max_fin_len; 

	const daily_market::key k_dm_0(pIn->symbol,TimeStampToTimeT(pIn->start_day) );
	const daily_market::key k_dm_1(pIn->symbol,numeric_limits<uint64_t>::max());
	table_scanner dm_scanner(s_arena.get());
	tbl_daily_market(1)->scan(txn, Encode(obj_key0, k_dm_0), &Encode(obj_key1, k_dm_1), dm_scanner, s_arena.get());
	ALWAYS_ASSERT( dm_scanner.output.size() );
	for(auto i=0; i < pIn->max_rows_to_return and i< dm_scanner.output.size(); i++ )
	{
		auto &r_dm = dm_scanner.output[i];

		daily_market::key k_dm_temp;
		daily_market::value v_dm_temp;
		const daily_market::key* k_dm = Decode( *r_dm.first, k_dm_temp );
		const daily_market::value* v_dm = Decode( *r_dm.second, v_dm_temp );

		pOut->day[i].date = TimeTToTimeStamp((time_t)k_dm->dm_date);
		pOut->day[i].close = v_dm->dm_close;
		pOut->day[i].high = v_dm->dm_high;
		pOut->day[i].low = v_dm->dm_low;
		pOut->day[i].vol = v_dm->dm_vol;

	}
	// TODO. order by
	pOut->day_len = min( pIn->max_rows_to_return, dm_scanner.output.size()); 

	const last_trade::key k_lt(pIn->symbol);
	ALWAYS_ASSERT(tbl_last_trade(1)->get(txn, Encode(obj_key0, k_lt), obj_v));
	last_trade::value v_lt_temp;
	const last_trade::value *v_lt = Decode(obj_v, v_lt_temp);

	pOut->last_price = v_lt->lt_price;
	pOut->last_open = v_lt->lt_open_price;
	pOut->last_vol = v_lt->lt_vol;

	const news_xref::key k_nx_0( co_id , 0 );
	const news_xref::key k_nx_1( co_id , numeric_limits<int64_t>::max() );
	table_scanner nx_scanner(s_arena.get());
	tbl_news_xref(1)->scan(txn, Encode(obj_key0, k_nx_0), &Encode(obj_key1, k_nx_1), nx_scanner, s_arena.get());
	ALWAYS_ASSERT( nx_scanner.output.size() );

	for(int i = 0; i < max_news_len; i++ )
	{
		auto &r_nx = nx_scanner.output[i];
		news_xref::key k_nx_temp;
		const news_xref::key* k_nx = Decode( *r_nx.first, k_nx_temp );

		const news_item::key k_ni(k_nx->nx_ni_id);
		ALWAYS_ASSERT(tbl_news_item(1)->get(txn, Encode(obj_key0, k_ni), obj_v));
		news_item::value v_ni_temp;
		const news_item::value *v_ni = Decode(obj_v, v_ni_temp);

		if( pIn->access_lob_flag )
		{
			memcpy(pOut->news[i].item, v_ni->ni_item.data(), v_ni->ni_item.size());
			pOut->news[i].dts = TimeTToTimeStamp((time_t)v_ni->ni_dts);
			memcpy(pOut->news[i].src , v_ni->ni_source.data(), v_ni->ni_source.size());
			memcpy(pOut->news[i].auth , v_ni->ni_author.data(), v_ni->ni_author.size());
			pOut->news[i].headline[0] = 0;
			pOut->news[i].summary[0] = 0;
		}
		else
		{
			pOut->news[i].item[0] = 0;
			pOut->news[i].dts = TimeTToTimeStamp((time_t)v_ni->ni_dts);
			memcpy(pOut->news[i].src , v_ni->ni_source.data(), v_ni->ni_source.size());
			memcpy(pOut->news[i].auth , v_ni->ni_author.data(), v_ni->ni_author.size());
			memcpy(pOut->news[i].headline , v_ni->ni_headline.data(), v_ni->ni_headline.size());
			memcpy(pOut->news[i].summary , v_ni->ni_summary.data(), v_ni->ni_summary.size());
		}
	}
	pOut->news_len = min( max_news_len, nx_scanner.output.size() );

	db->commit_txn(txn);
}

void tpce_worker::DoTradeLookupFrame1(const TTradeLookupFrame1Input *pIn, TTradeLookupFrame1Output *pOut)
{
	scoped_str_arena s_arena(arena);
	int i;

	txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);

	pOut->num_found = 0;
	for( i = 0; i < pIn->max_trades; i++ )
	{
		const trade::key k_t(pIn->trade_id[i]);
		ALWAYS_ASSERT(tbl_trade(1)->get(txn, Encode(obj_key0, k_t), obj_v));
		trade::value v_t_temp;
		const trade::value *v_t = Decode(obj_v, v_t_temp);

		const trade_type::key k_tt(v_t->t_tt_id);
		ALWAYS_ASSERT(tbl_trade_type(1)->get(txn, Encode(obj_key0, k_tt), obj_v));
		trade_type::value v_tt_temp;
		const trade_type::value *v_tt = Decode(obj_v, v_tt_temp);

		pOut->trade_info[i].bid_price = v_t->t_bid_price;
		memcpy(pOut->trade_info[i].exec_name, v_t->t_exec_name.data(), v_t->t_exec_name.size() );
		pOut->trade_info[i].is_cash= v_t->t_is_cash;
		pOut->trade_info[i].is_market= v_tt->tt_is_mrkt;
		pOut->trade_info[i].trade_price = v_t->t_trade_price;

		pOut->num_found++; 

		const settlement::key k_se(pIn->trade_id[i]);
		ALWAYS_ASSERT(tbl_settlement(1)->get(txn, Encode(obj_key0, k_se), obj_v));
		settlement::value v_se_temp;
		const settlement::value *v_se = Decode(obj_v, v_se_temp);

		pOut->trade_info[i].settlement_amount = v_se->se_amt;
		pOut->trade_info[i].settlement_cash_due_date = TimeTToTimeStamp((time_t)v_se->se_cash_due_date);
		memcpy(pOut->trade_info[i].settlement_cash_type, v_se->se_cash_type.data(), v_se->se_cash_type.size() );

		if( pOut->trade_info[i].is_cash )
		{
			const cash_transaction::key k_ct(pIn->trade_id[i]);
			ALWAYS_ASSERT(tbl_cash_transaction(1)->get(txn, Encode(obj_key0, k_ct), obj_v));
			cash_transaction::value v_ct_temp;
			const cash_transaction::value *v_ct = Decode(obj_v, v_ct_temp);
			
			pOut->trade_info[i].cash_transaction_amount = v_ct->ct_amt;
			pOut->trade_info[i].cash_transaction_dts = TimeTToTimeStamp((time_t)v_ct->ct_dts);
			memcpy(pOut->trade_info[i].cash_transaction_name,  v_ct->ct_name.data(), v_ct->ct_name.size() );
		}

		// Scan
		const trade_history::key k_th_0( pIn->trade_id[i], 0);
		const trade_history::key k_th_1( pIn->trade_id[i], numeric_limits<int64_t>::max());
		table_scanner th_scanner(s_arena.get());
		tbl_trade_history(1)->scan(txn, Encode(obj_key0, k_th_0), &Encode(obj_key1, k_th_1), th_scanner, s_arena.get());
		ALWAYS_ASSERT( th_scanner.output.size() );

		int th_cursor= 0;
		for( auto &r_th : th_scanner.output )
		{
			trade_history::key k_th_temp;
			trade_history::value v_th_temp;
			const trade_history::key* k_th = Decode( *r_th.first, k_th_temp );
			const trade_history::value* v_th = Decode( *r_th.second, v_th_temp );
			
			memcpy( pOut->trade_info[i].trade_history_status_id[th_cursor], v_th->th_st_id.data(), v_th->th_st_id.size() );
			pOut->trade_info[i].trade_history_dts[th_cursor] = TimeTToTimeStamp((time_t)k_th->th_dts);
			th_cursor++;

			if( th_cursor >= TradeLookupMaxTradeHistoryRowsReturned )
				break;
		}
	}
	db->commit_txn(txn);
}

void tpce_worker::DoTradeLookupFrame2(const TTradeLookupFrame2Input *pIn, TTradeLookupFrame2Output *pOut)
{
	scoped_str_arena s_arena(arena);
	txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);

	const t_ca_id_index::key k_t_0( pIn->acct_id, TimeStampToTimeT(pIn->start_trade_dts), 0 );
	const t_ca_id_index::key k_t_1( pIn->acct_id, TimeStampToTimeT(pIn->end_trade_dts), numeric_limits<int64_t>::max() );
	table_scanner t_scanner(s_arena.get());
	tbl_t_ca_id_index(1)->scan(txn, Encode(obj_key0, k_t_0), &Encode(obj_key1, k_t_1), t_scanner, s_arena.get());
	ALWAYS_ASSERT( t_scanner.output.size() );

	auto num_found = 0;
	for( auto &r_t : t_scanner.output )
	{
		t_ca_id_index::key k_t_temp;
		t_ca_id_index::value v_t_temp;
		const t_ca_id_index::key* k_t = Decode( *r_t.first, k_t_temp );
		const t_ca_id_index::value* v_t = Decode( *r_t.second, v_t_temp );

		pOut->trade_info[num_found].bid_price = v_t->t_bid_price;
		memcpy(pOut->trade_info[num_found].exec_name, v_t->t_exec_name.data(), v_t->t_exec_name.size() );
		pOut->trade_info[num_found].is_cash = v_t->t_is_cash;
		pOut->trade_info[num_found].trade_id= k_t->t_id;
		pOut->trade_info[num_found].trade_price= v_t->t_trade_price;
		num_found++;
		if( num_found > pIn->max_trades )
			break;
	}

	pOut->num_found = num_found;

	for( auto i = 0; i < num_found; i++ )
	{
		const settlement::key k_se(pOut->trade_info[i].trade_id);
		ALWAYS_ASSERT(tbl_settlement(1)->get(txn, Encode(obj_key0, k_se), obj_v));
		settlement::value v_se_temp;
		const settlement::value *v_se = Decode(obj_v, v_se_temp);

		pOut->trade_info[i].settlement_amount = v_se->se_amt;
		pOut->trade_info[i].settlement_cash_due_date = TimeTToTimeStamp((time_t)v_se->se_cash_due_date);
		memcpy(pOut->trade_info[i].settlement_cash_type, v_se->se_cash_type.data(), v_se->se_cash_type.size() );

		if( pOut->trade_info[i].is_cash )
		{
			const cash_transaction::key k_ct(pOut->trade_info[i].trade_id);
			ALWAYS_ASSERT(tbl_cash_transaction(1)->get(txn, Encode(obj_key0, k_ct), obj_v));
			cash_transaction::value v_ct_temp;
			const cash_transaction::value *v_ct = Decode(obj_v, v_ct_temp);
			
			pOut->trade_info[i].cash_transaction_amount = v_ct->ct_amt;
			pOut->trade_info[i].cash_transaction_dts = TimeTToTimeStamp((time_t)v_ct->ct_dts);
			memcpy(pOut->trade_info[i].cash_transaction_name,  v_ct->ct_name.data(), v_ct->ct_name.size() );
		}

		const trade_history::key k_th_0( pOut->trade_info[i].trade_id, 0);
		const trade_history::key k_th_1( pOut->trade_info[i].trade_id, numeric_limits<int64_t>::max());
		table_scanner th_scanner(s_arena.get());
		tbl_trade_history(1)->scan(txn, Encode(obj_key0, k_th_0), &Encode(obj_key1, k_th_1), th_scanner, s_arena.get());
		ALWAYS_ASSERT( th_scanner.output.size() );

		int th_cursor= 0;
		for( auto &r_th : th_scanner.output )
		{
			trade_history::key k_th_temp;
			trade_history::value v_th_temp;
			const trade_history::key* k_th = Decode( *r_th.first, k_th_temp );
			const trade_history::value* v_th = Decode( *r_th.second, v_th_temp );
			
			memcpy( pOut->trade_info[i].trade_history_status_id[th_cursor], v_th->th_st_id.data(), v_th->th_st_id.size() );
			pOut->trade_info[i].trade_history_dts[th_cursor] = TimeTToTimeStamp((time_t)k_th->th_dts);
			th_cursor++;

			if( th_cursor >= TradeLookupMaxTradeHistoryRowsReturned )
				break;
		}
	}

	db->commit_txn(txn);
}

void tpce_worker::DoTradeLookupFrame3(const TTradeLookupFrame3Input *pIn, TTradeLookupFrame3Output *pOut)
{
	scoped_str_arena s_arena(arena);
	txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);
	
	const t_s_symb_index::key k_t_0( string(pIn->symbol), TimeStampToTimeT(pIn->start_trade_dts), 0 );
	const t_s_symb_index::key k_t_1( string(pIn->symbol), TimeStampToTimeT(pIn->end_trade_dts), numeric_limits<uint64_t>::max() );
	table_scanner t_scanner(s_arena.get());
	tbl_t_s_symb_index(1)->scan(txn, Encode(obj_key0, k_t_0), &Encode(obj_key1, k_t_1), t_scanner, s_arena.get());
	ALWAYS_ASSERT( t_scanner.output.size() );

	auto num_found = 0;
	for( auto &r_t : t_scanner.output )
	{
		t_s_symb_index::key k_t_temp;
		t_s_symb_index::value v_t_temp;
		const t_s_symb_index::key* k_t = Decode( *r_t.first, k_t_temp );
		const t_s_symb_index::value* v_t = Decode( *r_t.second, v_t_temp );


		pOut->trade_info[num_found].acct_id = v_t->t_ca_id;
		memcpy(pOut->trade_info[num_found].exec_name, v_t->t_exec_name.data(), v_t->t_exec_name.size() );
		pOut->trade_info[num_found].is_cash = v_t->t_is_cash;
		pOut->trade_info[num_found].price= v_t->t_trade_price;
		pOut->trade_info[num_found].quantity = v_t->t_qty;
		pOut->trade_info[num_found].trade_dts = TimeTToTimeStamp((time_t)k_t->t_dts);
		pOut->trade_info[num_found].trade_id = k_t->t_id;
		memcpy(pOut->trade_info[num_found].trade_type, v_t->t_tt_id.data(), v_t->t_tt_id.size() );

		num_found++;
		if( num_found > pIn->max_trades )
			break;
	}

	pOut->num_found = num_found;

	for( int i = 0; i < num_found; i++ )
	{
		const settlement::key k_se(pOut->trade_info[i].trade_id);
		ALWAYS_ASSERT(tbl_settlement(1)->get(txn, Encode(obj_key0, k_se), obj_v));
		settlement::value v_se_temp;
		const settlement::value *v_se = Decode(obj_v, v_se_temp);

		pOut->trade_info[i].settlement_amount = v_se->se_amt;
		pOut->trade_info[i].settlement_cash_due_date = TimeTToTimeStamp((time_t)v_se->se_cash_due_date);
		memcpy(pOut->trade_info[i].settlement_cash_type, v_se->se_cash_type.data(), v_se->se_cash_type.size() );

		if( pOut->trade_info[i].is_cash )
		{
			const cash_transaction::key k_ct(pOut->trade_info[i].trade_id);
			ALWAYS_ASSERT(tbl_cash_transaction(1)->get(txn, Encode(obj_key0, k_ct), obj_v));
			cash_transaction::value v_ct_temp;
			const cash_transaction::value *v_ct = Decode(obj_v, v_ct_temp);
			
			pOut->trade_info[i].cash_transaction_amount = v_ct->ct_amt;
			pOut->trade_info[i].cash_transaction_dts = TimeTToTimeStamp((time_t)v_ct->ct_dts);
			memcpy(pOut->trade_info[i].cash_transaction_name,  v_ct->ct_name.data(), v_ct->ct_name.size() );
		}

		const trade_history::key k_th_0( pOut->trade_info[i].trade_id, 0);
		const trade_history::key k_th_1( pOut->trade_info[i].trade_id, numeric_limits<int64_t>::max());
		table_scanner th_scanner(s_arena.get());
		tbl_trade_history(1)->scan(txn, Encode(obj_key0, k_th_0), &Encode(obj_key1, k_th_1), th_scanner, s_arena.get());
		ALWAYS_ASSERT( th_scanner.output.size() );

		// TODO. order by
		int th_cursor= 0;
		for( auto &r_th : th_scanner.output )
		{
			trade_history::key k_th_temp;
			trade_history::value v_th_temp;
			const trade_history::key* k_th = Decode( *r_th.first, k_th_temp );
			const trade_history::value* v_th = Decode( *r_th.second, v_th_temp );
			
			memcpy( pOut->trade_info[i].trade_history_status_id[th_cursor], v_th->th_st_id.data(), v_th->th_st_id.size() );
			pOut->trade_info[i].trade_history_dts[th_cursor] = TimeTToTimeStamp((time_t)k_th->th_dts);
			th_cursor++;
			if( th_cursor >= TradeLookupMaxTradeHistoryRowsReturned )
				break;
		}
	}

	db->commit_txn(txn);
}

void tpce_worker::DoTradeLookupFrame4(const TTradeLookupFrame4Input *pIn, TTradeLookupFrame4Output *pOut)
{
	scoped_str_arena s_arena(arena);
	txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);

	const t_ca_id_index::key k_t_0( pIn->acct_id, TimeStampToTimeT(pIn->trade_dts), 0 );
	const t_ca_id_index::key k_t_1( pIn->acct_id, numeric_limits<uint64_t>::max(), numeric_limits<int64_t>::max() );
	table_scanner t_scanner(s_arena.get());
	tbl_t_ca_id_index(1)->scan(txn, Encode(obj_key0, k_t_0), &Encode(obj_key1, k_t_1), t_scanner, s_arena.get());
	ALWAYS_ASSERT( t_scanner.output.size() );

	for( auto &r_t : t_scanner.output )
	{
		t_ca_id_index::key k_t_temp;
		t_ca_id_index::value v_t_temp;
		const t_ca_id_index::key* k_t = Decode( *r_t.first, k_t_temp );
		const t_ca_id_index::value* v_t = Decode( *r_t.second, v_t_temp );

		pOut->trade_id = k_t->t_id;
		break;
	}
	pOut->num_trades_found = 1;

	// XXX. holding_history PK isn't unique. combine T_ID and row ID.
	const holding_history::key k_hh_0(pOut->trade_id);
	const holding_history::key k_hh_1(pOut->trade_id);
	table_scanner hh_scanner(s_arena.get());
	tbl_holding_history(1)->scan(txn, Encode(obj_key0, k_hh_0), &Encode(obj_key1, k_hh_1), hh_scanner, s_arena.get());
//	ALWAYS_ASSERT( t_scanner.output.size() );		// possible case. no holding for the customer

	auto hh_cursor = 0;
	for( auto& r_hh : hh_scanner.output )
	{
		holding_history::key k_hh_temp;
		holding_history::value v_hh_temp;
		const holding_history::key* k_hh = Decode( *r_hh.first, k_hh_temp );
		const holding_history::value* v_hh = Decode( *r_hh.second, v_hh_temp );

		pOut->trade_info[hh_cursor].holding_history_id = v_hh->hh_h_t_id;
		pOut->trade_info[hh_cursor].holding_history_trade_id = k_hh->hh_t_id;
		pOut->trade_info[hh_cursor].quantity_after = v_hh->hh_after_qty;
		pOut->trade_info[hh_cursor].quantity_before = v_hh->hh_before_qty;

		hh_cursor++;
		if( hh_cursor >= 20 )
			break;
	}

	pOut->num_found = hh_cursor;

	db->commit_txn(txn);
}

void tpce_worker::DoTradeOrderFrame1(const TTradeOrderFrame1Input *pIn, TTradeOrderFrame1Output *pOut)
{
	scoped_str_arena s_arena(arena);
	txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);

	const customer_account::key k_ca(pIn->acct_id);
	ALWAYS_ASSERT(tbl_customer_account(1)->get(txn, Encode(obj_key0, k_ca), obj_v));
	customer_account::value v_ca_temp;
	const customer_account::value *v_ca = Decode(obj_v, v_ca_temp);

	memcpy( pOut->acct_name, v_ca->ca_name.data(), v_ca->ca_name.size() );
	pOut->broker_id = v_ca->ca_b_id;
	pOut->cust_id = v_ca->ca_c_id;
	pOut->tax_status = v_ca->ca_tax_st;
	pOut->num_found = 1;

	const customers::key k_c(pOut->cust_id);
	ALWAYS_ASSERT(tbl_customers(1)->get(txn, Encode(obj_key0, k_c), obj_v));
	customers::value v_c_temp;
	const customers::value *v_c = Decode(obj_v, v_c_temp);

	memcpy( pOut->cust_f_name, v_c->c_f_name.data(), v_c->c_f_name.size() );
	memcpy( pOut->cust_l_name, v_c->c_l_name.data(), v_c->c_l_name.size() );
	pOut->cust_tier = v_c->c_tier;
	memcpy(pOut->tax_id, v_c->c_tax_id.data(), v_c->c_tax_id.size() );

	const broker::key k_b(pOut->broker_id);
	ALWAYS_ASSERT(tbl_broker(1)->get(txn, Encode(obj_key0, k_b), obj_v));
	broker::value v_b_temp;
	const broker::value *v_b = Decode(obj_v, v_b_temp);
	memcpy( pOut->broker_name, v_b->b_name.data(), v_b->b_name.size() );

//	cout << __FUNCTION__ << ","
//		<< pOut->cust_f_name << ","
//		<< pOut->cust_tier << ","
//		<< pOut->broker_name << endl;

}

void tpce_worker::DoTradeOrderFrame2(const TTradeOrderFrame2Input *pIn, TTradeOrderFrame2Output *pOut)
{
	scoped_str_arena s_arena(arena);

	const account_permission::key k_ap(pIn->acct_id, string(pIn->exec_tax_id) );
	if(tbl_account_permission(1)->get(txn, Encode(obj_key0, k_ap), obj_v))
	{
		account_permission::value v_ap_temp;
		const account_permission::value *v_ap = Decode(obj_v, v_ap_temp);
		if( v_ap->ap_f_name == pIn->exec_f_name and v_ap->ap_l_name == pIn->exec_l_name )
		{
			memcpy(pOut->ap_acl, v_ap->ap_acl.data(), v_ap->ap_acl.size() );
			return;
		}
	}
	pOut->ap_acl[0] = '\0';
//	cout << __FUNCTION__ << ","
//		<< pOut->ap_acl << endl;
}

void tpce_worker::DoTradeOrderFrame3(const TTradeOrderFrame3Input *pIn, TTradeOrderFrame3Output *pOut)
{
	scoped_str_arena s_arena(arena);

	int64_t co_id = 0;
	char exch_id[cEX_ID_len ];
	memset(exch_id, 0, cEX_ID_len );

	if( not pIn->symbol[0] )
	{
		const co_name_index::key k_co_0( string(pIn->co_name), 0 );
		const co_name_index::key k_co_1( string(pIn->co_name), numeric_limits<int64_t>::max() );
		table_scanner co_scanner(s_arena.get());
		tbl_co_name_index(1)->scan(txn, Encode(obj_key0, k_co_0), &Encode(obj_key1, k_co_1), co_scanner, s_arena.get());
		ALWAYS_ASSERT( co_scanner.output.size() );

		co_name_index::key k_co_temp;
		co_name_index::value v_co_temp;
		const co_name_index::key* k_co = Decode( *co_scanner.output.front().first, k_co_temp );
		const co_name_index::value* v_co = Decode( *co_scanner.output.front().second, v_co_temp );

		co_id = k_co->co_id;
		ALWAYS_ASSERT(co_id);

		/*
		const security::key k_s_0( string(cSYMBOL_len, (char)0  ) );
		const security::key k_s_1( string(cSYMBOL_len, (char)255) );
		table_scanner s_scanner(s_arena.get());
		tbl_security(1)->scan(txn, Encode(obj_key0, k_s_0), &Encode(obj_key1, k_s_1), s_scanner, s_arena.get());
		ALWAYS_ASSERT( s_scanner.output.size() );
		*/
		const security_index::key k_s(co_id, pIn->issue);
		ALWAYS_ASSERT(tbl_security_index(1)->get(txn, Encode(obj_key0, k_s), obj_v));
		security_index::value v_s_temp;
		const security_index::value *v_s = Decode(obj_v, v_s_temp);

		memcpy(exch_id, v_s->s_ex_id.data(), v_s->s_ex_id.size() );
		memcpy(pOut->s_name, v_s->s_name.data(), v_s->s_name.size() );
		memcpy(pOut->symbol, v_s->s_symb.data(), v_s->s_symb.size() );
	}

	else
	{
		memcpy(pOut->symbol, pIn->symbol, cSYMBOL_len);
		const security::key k_s(pIn->symbol);
		ALWAYS_ASSERT(tbl_security(1)->get(txn, Encode(obj_key0, k_s), obj_v));
		security::value v_s_temp;
		const security::value *v_s = Decode(obj_v, v_s_temp);

		co_id = v_s->s_co_id;
		memcpy(exch_id, v_s->s_ex_id.data(), v_s->s_ex_id.size() );
		memcpy(pOut->s_name, v_s->s_name.data(), v_s->s_name.size() );

		const company::key k_co(co_id);
		ALWAYS_ASSERT(tbl_company(1)->get(txn, Encode(obj_key0, k_co), obj_v));
		company::value v_co_temp;
		const company::value *v_co = Decode(obj_v, v_co_temp);
		memcpy(pOut->co_name, v_co->co_name.data(), v_co->co_name.size() );
	}
	const last_trade::key k_lt(pOut->symbol);
	ALWAYS_ASSERT(tbl_last_trade(1)->get(txn, Encode(obj_key0, k_lt), obj_v));
	last_trade::value v_lt_temp;
	const last_trade::value *v_lt = Decode(obj_v, v_lt_temp);

	pOut->market_price = v_lt->lt_price;

	const trade_type::key k_tt(pIn->trade_type_id);
	ALWAYS_ASSERT(tbl_trade_type(1)->get(txn, Encode(obj_key0, k_tt), obj_v));
	trade_type::value v_tt_temp;
	const trade_type::value *v_tt = Decode(obj_v, v_tt_temp);

	pOut->type_is_market = v_tt->tt_is_mrkt;
	pOut->type_is_sell = v_tt->tt_is_sell;

	if( pOut->type_is_market )
	{
		pOut->requested_price = pOut->market_price;
	}
	else
		pOut->requested_price = pIn->requested_price;

	auto hold_qty = 0;
	auto hold_price = 0.0;
	auto hs_qty = 0;
	auto buy_value = 0.0;
	auto sell_value = 0.0;
	auto needed_qty = pIn->trade_qty;

	const holding_summary::key k_hs(pIn->acct_id, string(pOut->symbol));
	if(tbl_holding_summary(1)->get(txn, Encode(obj_key0, k_hs), obj_v))
	{
		holding_summary::value v_hs_temp;
		const holding_summary::value *v_hs = Decode(obj_v, v_hs_temp);
		hs_qty = v_hs->hs_qty;
	}

	if( pOut->type_is_sell )
	{
		if( hs_qty > 0 )
		{
			vector<pair<int32_t, double>> hold_list;
			const holding::key k_h_0( pIn->acct_id, pOut->symbol ,0		   						 );
			const holding::key k_h_1( pIn->acct_id, pOut->symbol ,numeric_limits<int64_t>::max() );
			table_scanner h_scanner(s_arena.get());
			tbl_holding(1)->scan(txn, Encode(obj_key0, k_h_0), &Encode(obj_key1, k_h_1), h_scanner, s_arena.get());
			//ALWAYS_ASSERT( h_scanner.output.size() );		// this set could be empty

			for( auto &r_h : h_scanner.output )
			{
				holding::value v_h_temp;
				const holding::value* v_h = Decode( *r_h.second, v_h_temp );

				hold_list.push_back( make_pair(v_h->h_qty, v_h->h_price) );
			}

			if( pIn->is_lifo )
			{
				reverse(hold_list.begin(), hold_list.end());
			}

			for( auto& hold_list_cursor : hold_list )
			{
				if( not needed_qty )
					break;

				hold_qty = hold_list_cursor.first;
				hold_price = hold_list_cursor.second;

				if( hold_qty > needed_qty )
				{
					buy_value += needed_qty * hold_price;
					sell_value += needed_qty * pOut->requested_price;
					needed_qty = 0;
				}
				else
				{
					buy_value += hold_qty * hold_price;
					sell_value += hold_qty * pOut->requested_price;
					needed_qty -= hold_qty;
				}
			}
		}
	}
	else
	{
		if( hs_qty < 0 )
		{
			vector<pair<int32_t, double>> hold_list;
			const holding::key k_h_0( pIn->acct_id, pOut->symbol, 0 );
			const holding::key k_h_1( pIn->acct_id, pOut->symbol, numeric_limits<int64_t>::max() );
			table_scanner h_scanner(s_arena.get());
			tbl_holding(1)->scan(txn, Encode(obj_key0, k_h_0), &Encode(obj_key1, k_h_1), h_scanner, s_arena.get());
		//	ALWAYS_ASSERT( h_scanner.output.size() );		// this set could be empty

			for( auto &r_h : h_scanner.output )
			{
				holding::value v_h_temp;
				const holding::value* v_h = Decode( *r_h.second, v_h_temp );

				hold_list.push_back( make_pair(v_h->h_qty, v_h->h_price) );
			}
			if( pIn->is_lifo )
			{
				reverse(hold_list.begin(), hold_list.end());
			}

			for( auto& hold_list_cursor : hold_list )
			{
				if( not needed_qty )
					break;

				hold_qty = hold_list_cursor.first;
				hold_price = hold_list_cursor.second;

				if( hold_qty + needed_qty < 0 )
				{
					sell_value += needed_qty * hold_price;
					buy_value += needed_qty * pOut->requested_price;
					needed_qty = 0;
				}
				else
				{
					hold_qty -= hold_qty;
					sell_value += hold_qty * hold_price;
					buy_value += hold_qty * pOut->requested_price;
					needed_qty -= hold_qty;
				}
			}
		}
	}

	pOut->tax_amount = 0.0;
	if( sell_value > buy_value and (pIn->tax_status == 1 or pIn->tax_status == 2 ))
	{
		const customer_taxrate::key k_cx_0( pIn->cust_id, string(cTX_ID_len, (char)0  ));
		const customer_taxrate::key k_cx_1( pIn->cust_id, string(cTX_ID_len, (char)255));

		string kcx1 = Encode(obj_key0, k_cx_0);
		string kcx2 = Encode(obj_key1, k_cx_1);
		table_scanner cx_scanner(s_arena.get());
		tbl_customer_taxrate(1)->scan(txn, Encode(obj_key0, k_cx_0), &Encode(obj_key1, k_cx_1), cx_scanner, s_arena.get());
		ALWAYS_ASSERT( cx_scanner.output.size() );

		auto tax_rates = 0.0;
		for( auto &r_cx : cx_scanner.output )
		{
			customer_taxrate::key k_cx_temp;
			const customer_taxrate::key* k_cx = Decode( *r_cx.first, k_cx_temp );

			const tax_rate::key k_tx(k_cx->cx_tx_id);
			ALWAYS_ASSERT(tbl_tax_rate(1)->get(txn, Encode(obj_key0, k_tx), obj_v));
			tax_rate::value v_tx_temp;
			const tax_rate::value *v_tx = Decode(obj_v, v_tx_temp);

			tax_rates += v_tx->tx_rate;
		}
		pOut->tax_amount = (sell_value - buy_value) * tax_rates;
	}
	
	const commission_rate::key k_cr_0( pIn->cust_tier, string(pIn->trade_type_id), string(exch_id), 0 );
	const commission_rate::key k_cr_1( pIn->cust_tier, string(pIn->trade_type_id), string(exch_id), pIn->trade_qty );

	table_scanner cr_scanner(s_arena.get());
	string k_cr_s0 = Encode(obj_key0, k_cr_0);
	string k_cr_s1 = Encode(obj_key1, k_cr_1);
	tbl_commission_rate(1)->scan(txn, k_cr_s0, &k_cr_s1, cr_scanner, s_arena.get());
	ALWAYS_ASSERT(cr_scanner.output.size());

	for( auto &r_cr : cr_scanner.output )
	{
		commission_rate::value v_cr_temp;
		const commission_rate::value* v_cr = Decode( *r_cr.second, v_cr_temp );

		if( v_cr->cr_to_qty < pIn->trade_qty )
			continue;

		pOut->comm_rate = v_cr->cr_rate;
		break;
	}

	const charge::key k_ch(pIn->trade_type_id, pIn->cust_tier );
	ALWAYS_ASSERT(tbl_charge(1)->get(txn, Encode(obj_key0, k_ch), obj_v));
	charge::value v_ch_temp;
	const charge::value *v_ch = Decode(obj_v, v_ch_temp);
	pOut->charge_amount = v_ch->ch_chrg;


	double acct_bal = 0.0;
	double hold_assets = 0.0;
	pOut->acct_assets = 0.0;

	if( pIn->type_is_margin )
	{
		const customer_account::key k_ca(pIn->acct_id);
		ALWAYS_ASSERT(tbl_customer_account(1)->get(txn, Encode(obj_key0, k_ca), obj_v));
		customer_account::value v_ca_temp;
		const customer_account::value *v_ca = Decode(obj_v, v_ca_temp);
		acct_bal = v_ca->ca_bal;

		const holding_summary::key k_hs_0( pIn->acct_id, string(cSYMBOL_len, (char)0  ) );
		const holding_summary::key k_hs_1( pIn->acct_id, string(cSYMBOL_len, (char)255) );
		table_scanner hs_scanner(s_arena.get());
		tbl_holding_summary(1)->scan(txn, Encode(obj_key0, k_hs_0), &Encode(obj_key1, k_hs_1), hs_scanner, s_arena.get());
//		ALWAYS_ASSERT( hs_scanner.output.size() );				// XXX. allowed?

		for( auto &r_hs : hs_scanner.output )
		{
			holding_summary::key k_hs_temp;
			holding_summary::value v_hs_temp;
			const holding_summary::key* k_hs = Decode( *r_hs.first, k_hs_temp );
			const holding_summary::value* v_hs = Decode( *r_hs.second, v_hs_temp );

			const last_trade::key k_lt(k_hs->hs_s_symb);
			ALWAYS_ASSERT(tbl_last_trade(1)->get(txn, Encode(obj_key0, k_lt), obj_v));
			last_trade::value v_lt_temp;
			const last_trade::value *v_lt = Decode(obj_v, v_lt_temp);

			hold_assets += v_hs->hs_qty * v_lt->lt_price;
			
		}

		if( not hold_assets )
			pOut->acct_assets = acct_bal;
		else
			pOut->acct_assets = hold_assets + acct_bal;

	}
	if( pOut->type_is_market )
		memcpy(pOut->status_id, pIn->st_submitted_id, cST_ID_len );
	else
		memcpy(pOut->status_id, pIn->st_pending_id, cST_ID_len );

//	cout << __FUNCTION__ << ","
//		<< pOut->status_id<< ","
//		<< pOut->acct_assets<< ","
//		<< pOut->comm_rate<< ","
//		<< pOut->tax_amount<< ","
//		<< pOut->charge_amount << endl;
}

void tpce_worker::DoTradeOrderFrame4(const TTradeOrderFrame4Input *pIn, TTradeOrderFrame4Output *pOut)
{
	auto now_dts = time(0);
	pOut->trade_id = GetLastTradeID();
	trade::key k_t;
	trade::value v_t;
	k_t.t_id = pOut->trade_id;
	v_t.t_st_id = string(pIn->status_id);
	v_t.t_tt_id = string(pIn->trade_type_id);
	v_t.t_is_cash = pIn->is_cash;
	v_t.t_s_symb = string(pIn->symbol);
	v_t.t_qty = pIn->trade_qty;
	v_t.t_bid_price = pIn->requested_price;
	v_t.t_ca_id = pIn->acct_id;
	v_t.t_exec_name = string(pIn->exec_name);
	v_t.t_trade_price = 0;
	v_t.t_chrg = pIn->charge_amount;
	v_t.t_comm = pIn->comm_amount;
	v_t.t_tax = 0;
	v_t.t_lifo = pIn->is_lifo;
	tbl_trade(1)->insert(txn, Encode(k_t), Encode(obj_v, v_t));

	t_ca_id_index::key k_t_idx1;
	t_ca_id_index::value v_t_idx1;
	k_t_idx1.t_ca_id 		= v_t.t_ca_id;
	k_t_idx1.t_dts 			= v_t.t_dts;
	k_t_idx1.t_id 			= k_t.t_id;
	v_t_idx1.t_st_id 		= v_t.t_st_id ;
	v_t_idx1.t_tt_id 		= v_t.t_tt_id ;
	v_t_idx1.t_is_cash 		= v_t.t_is_cash ;
	v_t_idx1.t_s_symb 		= v_t.t_s_symb ;
	v_t_idx1.t_qty 			= v_t.t_qty ;
	v_t_idx1.t_bid_price 	= v_t.t_bid_price ;
	v_t_idx1.t_exec_name 	= v_t.t_exec_name ;
	v_t_idx1.t_trade_price 	= v_t.t_trade_price ;
	v_t_idx1.t_chrg 		= v_t.t_chrg ;
	tbl_t_ca_id_index(1)->put(txn, Encode(k_t_idx1), Encode(obj_v, v_t_idx1));

	t_s_symb_index::key k_t_idx2;
	t_s_symb_index::value v_t_idx2;
	k_t_idx2.t_s_symb 		= v_t.t_s_symb ;
	k_t_idx2.t_dts 			= v_t.t_dts;
	k_t_idx2.t_id 			= k_t.t_id;
	v_t_idx2.t_ca_id 		= v_t.t_ca_id;
	v_t_idx2.t_st_id 		= v_t.t_st_id ;
	v_t_idx2.t_tt_id 		= v_t.t_tt_id ;
	v_t_idx2.t_is_cash 		= v_t.t_is_cash ;
	v_t_idx2.t_qty 			= v_t.t_qty ;
	v_t_idx2.t_exec_name 	= v_t.t_exec_name ;
	v_t_idx2.t_trade_price 	= v_t.t_trade_price ;
	tbl_t_s_symb_index(1)->put(txn, Encode(k_t_idx2), Encode(obj_v, v_t_idx2));

	if( not pIn->type_is_market )
	{
		trade_request::key k_tr;
		trade_request::value v_tr;
		
		k_tr.tr_s_symb = string(pIn->symbol);
		k_tr.tr_b_id = pIn->broker_id;
		v_tr.tr_t_id = pOut->trade_id;
		v_tr.tr_tt_id = string(pIn->trade_type_id);
		v_tr.tr_qty = pIn->trade_qty;
		v_tr.tr_bid_price = pIn->requested_price;
		//cout << "TO : " << v_tr.tr_tt_id <<"," << v_tr.tr_bid_price << endl;
		tbl_trade_request(1)->insert(txn, Encode(k_tr), Encode(obj_v, v_tr));
	}

	trade_history::key k_th;
	trade_history::value v_th;

	k_th.th_t_id = pOut->trade_id;
	k_th.th_dts = now_dts;
	v_th.th_st_id = string(pIn->status_id);

	tbl_trade_history(1)->insert(txn, Encode(k_th), Encode(obj_v, v_th));

//	cout << __FUNCTION__ << ","
//		<< k_th.th_t_id << ","
//		<< k_th.th_dts << ","
//		<< v_t.t_tt_id << ","
//		<< v_t.t_is_cash << ","
//		<< v_t.t_s_symb<< "," 
//		<< v_t.t_qty << endl;
}

void tpce_worker::DoTradeOrderFrame5(void)
{
	db->abort_txn(txn);
}

void tpce_worker::DoTradeOrderFrame6(void)
{
	db->commit_txn(txn);
}

void tpce_worker::DoTradeResultFrame1(const TTradeResultFrame1Input *pIn, TTradeResultFrame1Output *pOut)
{
	scoped_str_arena s_arena(arena);
	txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);

	const trade::key k_t(pIn->trade_id);
	ALWAYS_ASSERT(tbl_trade(1)->get(txn, Encode(obj_key0, k_t), obj_v));
	trade::value v_t_temp;
	const trade::value *v_t = Decode(obj_v, v_t_temp);
	pOut->acct_id = v_t->t_ca_id;
	memcpy(pOut->type_id, v_t->t_tt_id.data(), v_t->t_tt_id.size());
	memcpy(pOut->symbol, v_t->t_s_symb.data(), v_t->t_s_symb.size());
	pOut->trade_qty = v_t->t_qty;
	pOut->charge = v_t->t_chrg;
	pOut->is_lifo = v_t->t_lifo;
	pOut->trade_is_cash = v_t->t_is_cash;
	pOut->num_found = 1;

	const trade_type::key k_tt(pOut->type_id);
	ALWAYS_ASSERT(tbl_trade_type(1)->get(txn, Encode(obj_key0, k_tt), obj_v));
	trade_type::value v_tt_temp;
	const trade_type::value *v_tt = Decode(obj_v, v_tt_temp);
	memcpy(pOut->type_name, v_tt->tt_name.data(), v_tt->tt_name.size());
	pOut->type_is_sell = v_tt->tt_is_sell;
	pOut->type_is_market = v_tt->tt_is_mrkt;

	pOut->hs_qty = 0;
	const holding_summary::key k_hs(pOut->acct_id, pOut->symbol);
	if(tbl_holding_summary(1)->get(txn, Encode(obj_key0, k_hs), obj_v))
	{
		holding_summary::value v_hs_temp;
		const holding_summary::value *v_hs = Decode(obj_v, v_hs_temp);
		pOut->hs_qty = v_hs->hs_qty;
	}
//	cout << __FUNCTION__ << ","
//		<< pOut->trade_qty<< ","
//		<< pOut->charge << ","
//		<< pOut->is_lifo << ","
//		<< pOut->trade_is_cash << ","
//		<< pOut->hs_qty << endl;
}

void tpce_worker::DoTradeResultFrame2(const TTradeResultFrame2Input *pIn, TTradeResultFrame2Output *pOut)
{
	scoped_str_arena s_arena(arena);
	auto buy_value = 0.0;
	auto sell_value = 0.0;
	auto needed_qty = pIn->trade_qty;
	auto trade_dts = time(0);
	auto hold_id=0;
	auto hold_price=0;
	auto hold_qty=0;

	pOut->trade_dts = TimeTToTimeStamp((time_t)trade_dts);

	const customer_account::key k_ca(pIn->acct_id);
	ALWAYS_ASSERT(tbl_customer_account(1)->get(txn, Encode(obj_key0, k_ca), obj_v));
	customer_account::value v_ca_temp;
	const customer_account::value *v_ca = Decode(obj_v, v_ca_temp);
	pOut->broker_id = v_ca->ca_b_id;
	pOut->cust_id = v_ca->ca_c_id;
	pOut->tax_status = v_ca->ca_tax_st;

	if( pIn->type_is_sell )
	{
		if( pIn->hs_qty == 0 )
		{
			holding_summary::key k_hs;
			holding_summary::value v_hs;
			k_hs.hs_ca_id		= pIn->acct_id;
			k_hs.hs_s_symb		= string(pIn->symbol);
			v_hs.hs_qty		= -1 * pIn->trade_qty;
			tbl_holding_summary(1)->insert(txn, Encode(k_hs), Encode(obj_v, v_hs));
		}

		else
		{
			if( pIn->hs_qty != pIn->trade_qty )
			{
				holding_summary::key k_hs;
				holding_summary::value v_hs;
				k_hs.hs_ca_id		= pIn->acct_id;
				k_hs.hs_s_symb		= string(pIn->symbol);
				v_hs.hs_qty		= pIn->hs_qty - pIn->trade_qty;
				tbl_holding_summary(1)->put(txn, Encode(k_hs), Encode(obj_v, v_hs));
			}
		}

		if( pIn->hs_qty > 0 )
		{
			const holding::key k_h_0( pIn->acct_id, pIn->symbol, 0 );
			const holding::key k_h_1( pIn->acct_id, pIn->symbol, numeric_limits<int64_t>::max() );
			table_scanner h_scanner(s_arena.get());
			tbl_holding(1)->scan(txn, Encode(obj_key0, k_h_0), &Encode(obj_key1, k_h_1), h_scanner, s_arena.get());
//			ALWAYS_ASSERT( h_scanner.output.size() );		// guessing this could be empty set

			if( pIn->is_lifo )
			{
				reverse(h_scanner.output.begin(), h_scanner.output.end());
			}
			
			for( auto& r_h : h_scanner.output )
			{
				if( needed_qty == 0 )
					break;
				holding::key k_h_temp;
				holding::value v_h_temp;
				const holding::key* k_h = Decode( *r_h.first, k_h_temp );
				const holding::value* v_h = Decode( *r_h.second, v_h_temp );

				hold_id = v_h->h_t_id;
				hold_qty = v_h->h_qty;
				hold_price = v_h->h_price;

				if( hold_qty > needed_qty )
				{
					holding_history::key k_hh;
					holding_history::value v_hh;
					k_hh.hh_t_id 		= pIn->trade_id;
					v_hh.hh_h_t_id 		= hold_id;
					v_hh.hh_before_qty 	= hold_qty;
					v_hh.hh_after_qty 	= hold_qty - needed_qty;
					tbl_holding_history(1)->insert(txn, Encode(k_hh), Encode(obj_v, v_hh));

					// update with current holding cursor. use the same key
					holding::key k_h_new(*k_h);
					holding::value v_h_new(*v_h);
					v_h_new.h_qty	= hold_qty - needed_qty;
					tbl_holding(1)->put(txn, Encode(k_h_new), Encode(obj_v, v_h_new));

					buy_value += needed_qty * hold_price;
					sell_value += needed_qty * pIn->trade_price;
					needed_qty = 0;
				}
				else
				{
					holding_history::key k_hh;
					holding_history::value v_hh;
					k_hh.hh_t_id 		= pIn->trade_id;
					v_hh.hh_h_t_id 		= hold_id;
					v_hh.hh_before_qty 	= hold_qty;
					v_hh.hh_after_qty 	= 0;
					tbl_holding_history(1)->insert(txn, Encode(k_hh), Encode(obj_v, v_hh));

					holding::key k_h_new(*k_h);
					tbl_holding(1)->remove(txn, Encode(k_h_new));

					buy_value += hold_qty * hold_price;
					sell_value += hold_qty * pIn->trade_price;
					needed_qty -= hold_qty;
				}
			}
		}


		if( needed_qty > 0)
		{
			holding_history::key k_hh;
			holding_history::value v_hh;
			k_hh.hh_t_id 		= pIn->trade_id;
			v_hh.hh_h_t_id 		= pIn->trade_id;
			v_hh.hh_before_qty 	= 0;
			v_hh.hh_after_qty 	= -1 * needed_qty;
			tbl_holding_history(1)->insert(txn, Encode(k_hh), Encode(obj_v, v_hh));

			holding::key k_h;
			holding::value v_h;
			k_h.h_ca_id 	= pIn->acct_id;
			k_h.h_s_symb 	= string(pIn->symbol);
			k_h.h_dts 		= trade_dts;
			v_h.h_t_id 		= pIn->trade_id;
			v_h.h_price 	= pIn->trade_price;
			v_h.h_qty 		= -1 * needed_qty;
			tbl_holding(1)->insert(txn, Encode(k_h), Encode(obj_v, v_h));

		}
		else
		{
			if( pIn->hs_qty == pIn->trade_qty )
			{
				holding_summary::key k_hs;
				k_hs.hs_ca_id		= pIn->acct_id;
				k_hs.hs_s_symb		= string(pIn->symbol);
				tbl_holding_summary(1)->remove(txn, Encode(k_hs));
			}
			
		}
	}
	else		// BUY
	{
		if( pIn->hs_qty == 0 )
		{
			// HS insert
			holding_summary::key k_hs;
			holding_summary::value v_hs;
			k_hs.hs_ca_id		= pIn->acct_id;
			k_hs.hs_s_symb		= string(pIn->symbol);
			v_hs.hs_qty		=  pIn->trade_qty;
			tbl_holding_summary(1)->insert(txn, Encode(k_hs), Encode(obj_v, v_hs));

		}
		else if ( -1*pIn->hs_qty != pIn->trade_qty )
		{
			// HS update
			holding_summary::key k_hs;
			holding_summary::value v_hs;
			k_hs.hs_ca_id		= pIn->acct_id;
			k_hs.hs_s_symb		= string(pIn->symbol);
			v_hs.hs_qty 		= pIn->trade_qty + pIn->hs_qty;
			tbl_holding_summary(1)->put(txn, Encode(k_hs), Encode(obj_v, v_hs));
		}

		if( pIn->hs_qty < 0 )
		{
			const holding::key k_h_0( pIn->acct_id, pIn->symbol, 0 );
			const holding::key k_h_1( pIn->acct_id, pIn->symbol, numeric_limits<int64_t>::max() );
			table_scanner h_scanner(s_arena.get());
			tbl_holding(1)->scan(txn, Encode(obj_key0, k_h_0), &Encode(obj_key1, k_h_1), h_scanner, s_arena.get());
//			ALWAYS_ASSERT( h_scanner.output.size() );			// XXX. guessing could be empty

			if( pIn->is_lifo )
			{
				reverse(h_scanner.output.begin(), h_scanner.output.end());
			}
			
			// hold list cursor 
			for( auto& r_h : h_scanner.output )
			{
				if( needed_qty == 0 )
					break;
				holding::key k_h_temp;
				holding::value v_h_temp;
				const holding::key* k_h = Decode( *r_h.first, k_h_temp );
				const holding::value* v_h = Decode( *r_h.second, v_h_temp );

				hold_id = v_h->h_t_id;
				hold_qty = v_h->h_qty;
				hold_price = v_h->h_price;
				
				if( hold_qty + needed_qty < 0 )
				{
					// HH insert
					// H update
					holding_history::key k_hh;
					holding_history::value v_hh;
					k_hh.hh_t_id 		= pIn->trade_id;
					v_hh.hh_h_t_id 		= hold_id;
					v_hh.hh_before_qty 	= hold_qty;
					v_hh.hh_after_qty 	= hold_qty + needed_qty;
					tbl_holding_history(1)->insert(txn, Encode(k_hh), Encode(obj_v, v_hh));

					// update with current holding cursor. use the same key
					holding::key k_h_new(*k_h);
					holding::value v_h_new(*v_h);
					v_h_new.h_qty	= hold_qty + needed_qty;
					tbl_holding(1)->put(txn, Encode(k_h_new), Encode(obj_v, v_h_new));

					sell_value += needed_qty * hold_price;
					buy_value += needed_qty * pIn->trade_price;
					needed_qty = 0;
				}
				else
				{
					// HH insert
					holding_history::key k_hh;
					holding_history::value v_hh;
					k_hh.hh_t_id 		= pIn->trade_id;
					v_hh.hh_h_t_id 		= hold_id;
					v_hh.hh_before_qty 	= hold_qty;
					v_hh.hh_after_qty 	= 0;
					tbl_holding_history(1)->insert(txn, Encode(k_hh), Encode(obj_v, v_hh));

					// H delete
					holding::key k_h_new(*k_h);
					tbl_holding(1)->remove(txn, Encode(k_h_new));

					hold_qty *= -1;
					sell_value += hold_qty * hold_price;
					buy_value += hold_qty * pIn->trade_price;
					needed_qty -= hold_qty;
				}
			}
		}
		if( needed_qty > 0 ) 
		{
			holding_history::key k_hh;
			holding_history::value v_hh;
			k_hh.hh_t_id 		= pIn->trade_id;
			v_hh.hh_h_t_id 		= pIn->trade_id;
			v_hh.hh_before_qty 	= 0;
			v_hh.hh_after_qty 	= needed_qty;
			tbl_holding_history(1)->insert(txn, Encode(k_hh), Encode(obj_v, v_hh));

			holding::key k_h;
			holding::value v_h;
			k_h.h_ca_id 	= pIn->acct_id;
			k_h.h_s_symb 	= string(pIn->symbol);
			k_h.h_dts 		= trade_dts;
			v_h.h_t_id 		= pIn->trade_id;
			v_h.h_price 	= pIn->trade_price;
			v_h.h_qty 		= needed_qty;
			tbl_holding(1)->insert(txn, Encode(k_h), Encode(obj_v, v_h));
		}
		else if ( -1*pIn->hs_qty == pIn->trade_qty )
		{
			holding_summary::key k_hs;
			k_hs.hs_ca_id		= pIn->acct_id;
			k_hs.hs_s_symb		= string(pIn->symbol);
			tbl_holding_summary(1)->remove(txn, Encode(k_hs));
		}
	}

//	cout << __FUNCTION__ << ","
//		<< pOut->broker_id << ","
//		<< pOut->cust_id   << ","
//		<< pOut->tax_status << endl;
}

void tpce_worker::DoTradeResultFrame3(const TTradeResultFrame3Input *pIn, TTradeResultFrame3Output *pOut)
{
	scoped_str_arena s_arena(arena);
	const customer_taxrate::key k_cx_0( pIn->cust_id, string(cTX_ID_len, (char)0  ) );
	const customer_taxrate::key k_cx_1( pIn->cust_id, string(cTX_ID_len, (char)255) );
	table_scanner cx_scanner(s_arena.get());
	tbl_customer_taxrate(1)->scan(txn, Encode(obj_key0, k_cx_0), &Encode(obj_key1, k_cx_1), cx_scanner, s_arena.get());
	ALWAYS_ASSERT( cx_scanner.output.size() );

	double tax_rates = 0.0;
	for( auto &r_cx : cx_scanner.output )
	{
		customer_taxrate::key k_cx_temp;
		customer_taxrate::value v_cx_temp;
		const customer_taxrate::key* k_cx = Decode( *r_cx.first, k_cx_temp );

		const tax_rate::key k_tx(k_cx->cx_tx_id);
		ALWAYS_ASSERT(tbl_tax_rate(1)->get(txn, Encode(obj_key0, k_tx), obj_v));
		tax_rate::value v_tx_temp;
		const tax_rate::value *v_tx = Decode(obj_v, v_tx_temp);

		tax_rates += v_tx->tx_rate;
	}

	pOut->tax_amount = (pIn->sell_value - pIn->buy_value) * tax_rates;

	const trade::key k_t(pIn->trade_id);
	ALWAYS_ASSERT(tbl_trade(1)->get(txn, Encode(obj_key0, k_t), obj_v));
	trade::value v_t_temp;
	const trade::value *v_t = Decode(obj_v, v_t_temp);
	trade::value v_t_new(*v_t);
	v_t_new.t_tax = pOut->tax_amount;
	tbl_trade(1)->put(txn, Encode(k_t), Encode(obj_v, v_t_new));
	
	t_ca_id_index::key k_t_idx1;
	t_ca_id_index::value v_t_idx1;
	k_t_idx1.t_ca_id 		= v_t_new.t_ca_id;
	k_t_idx1.t_dts 			= v_t_new.t_dts;
	k_t_idx1.t_id 			= k_t.t_id;
	v_t_idx1.t_st_id 		= v_t_new.t_st_id ;
	v_t_idx1.t_tt_id 		= v_t_new.t_tt_id ;
	v_t_idx1.t_is_cash 		= v_t_new.t_is_cash ;
	v_t_idx1.t_s_symb 		= v_t_new.t_s_symb ;
	v_t_idx1.t_qty 			= v_t_new.t_qty ;
	v_t_idx1.t_bid_price 	= v_t_new.t_bid_price ;
	v_t_idx1.t_exec_name 	= v_t_new.t_exec_name ;
	v_t_idx1.t_trade_price 	= v_t_new.t_trade_price ;
	v_t_idx1.t_chrg 		= v_t_new.t_chrg ;
	tbl_t_ca_id_index(1)->put(txn, Encode(k_t_idx1), Encode(obj_v, v_t_idx1));

	t_s_symb_index::key k_t_idx2;
	t_s_symb_index::value v_t_idx2;
	k_t_idx2.t_s_symb 		= v_t_new.t_s_symb ;
	k_t_idx2.t_dts 			= v_t_new.t_dts;
	k_t_idx2.t_id 			= k_t.t_id;
	v_t_idx2.t_ca_id 		= v_t_new.t_ca_id;
	v_t_idx2.t_st_id 		= v_t_new.t_st_id ;
	v_t_idx2.t_tt_id 		= v_t_new.t_tt_id ;
	v_t_idx2.t_is_cash 		= v_t_new.t_is_cash ;
	v_t_idx2.t_qty 			= v_t_new.t_qty ;
	v_t_idx2.t_exec_name 	= v_t_new.t_exec_name ;
	v_t_idx2.t_trade_price 	= v_t_new.t_trade_price ;
	tbl_t_s_symb_index(1)->put(txn, Encode(k_t_idx2), Encode(obj_v, v_t_idx2));
//	cout << __FUNCTION__ << ","
//		<< pOut->tax_amount << endl;
}

void tpce_worker::DoTradeResultFrame4(const TTradeResultFrame4Input *pIn, TTradeResultFrame4Output *pOut)
{
	scoped_str_arena s_arena(arena);

	const security::key k_s(pIn->symbol);
	ALWAYS_ASSERT(tbl_security(1)->get(txn, Encode(obj_key0, k_s), obj_v));
	security::value v_s_temp;
	const security::value *v_s = Decode(obj_v, v_s_temp);
	memcpy(pOut->s_name, v_s->s_name.data(), v_s->s_name.size() );

	const customers::key k_c(pIn->cust_id);
	ALWAYS_ASSERT(tbl_customers(1)->get(txn, Encode(obj_key0, k_c), obj_v));
	customers::value v_c_temp;
	const customers::value *v_c = Decode(obj_v, v_c_temp);
	
	const commission_rate::key k_cr_0( v_c->c_tier, string(pIn->type_id), v_s->s_ex_id, 0);
	const commission_rate::key k_cr_1( v_c->c_tier, string(pIn->type_id), v_s->s_ex_id, pIn->trade_qty );

	table_scanner cr_scanner(s_arena.get());
	string k_cr_s0 = Encode(obj_key0, k_cr_0);
	string k_cr_s1 = Encode(obj_key1, k_cr_1);
	tbl_commission_rate(1)->scan(txn, k_cr_s0, &k_cr_s1, cr_scanner, s_arena.get());
	ALWAYS_ASSERT(cr_scanner.output.size());

	for( auto &r_cr : cr_scanner.output )
	{
		commission_rate::value v_cr_temp;
		const commission_rate::value* v_cr = Decode( *r_cr.second, v_cr_temp );

		if( v_cr->cr_to_qty < pIn->trade_qty )
			continue;
		pOut->comm_rate = v_cr->cr_rate;
		break;
	}
}

void tpce_worker::DoTradeResultFrame5(const TTradeResultFrame5Input *pIn                                )
{
	const trade::key k_t(pIn->trade_id);
	ALWAYS_ASSERT(tbl_trade(1)->get(txn, Encode(obj_key0, k_t), obj_v));
	trade::value v_t_temp;
	const trade::value *v_t = Decode(obj_v, v_t_temp);
	trade::value v_t_new(*v_t);
	v_t_new.t_comm = pIn->comm_amount;
	v_t_new.t_dts = TimeStampToTimeT(pIn->trade_dts);
	v_t_new.t_st_id = string(pIn->st_completed_id);
	v_t_new.t_trade_price = pIn->trade_price;
	tbl_trade(1)->put(txn, Encode(k_t), Encode(obj_v, v_t_new));

	t_ca_id_index::key k_t_idx1;
	t_ca_id_index::value v_t_idx1;
	k_t_idx1.t_ca_id 		= v_t_new.t_ca_id;
	k_t_idx1.t_dts 			= v_t_new.t_dts;
	k_t_idx1.t_id 			= k_t.t_id;
	v_t_idx1.t_st_id 		= v_t_new.t_st_id ;
	v_t_idx1.t_tt_id 		= v_t_new.t_tt_id ;
	v_t_idx1.t_is_cash 		= v_t_new.t_is_cash ;
	v_t_idx1.t_s_symb 		= v_t_new.t_s_symb ;
	v_t_idx1.t_qty 			= v_t_new.t_qty ;
	v_t_idx1.t_bid_price 	= v_t_new.t_bid_price ;
	v_t_idx1.t_exec_name 	= v_t_new.t_exec_name ;
	v_t_idx1.t_trade_price 	= v_t_new.t_trade_price ;
	v_t_idx1.t_chrg 		= v_t_new.t_chrg ;
	tbl_t_ca_id_index(1)->put(txn, Encode(k_t_idx1), Encode(obj_v, v_t_idx1));

	t_s_symb_index::key k_t_idx2;
	t_s_symb_index::value v_t_idx2;
	k_t_idx2.t_s_symb 		= v_t_new.t_s_symb ;
	k_t_idx2.t_dts 			= v_t_new.t_dts;
	k_t_idx2.t_id 			= k_t.t_id;
	v_t_idx2.t_ca_id 		= v_t_new.t_ca_id;
	v_t_idx2.t_st_id 		= v_t_new.t_st_id ;
	v_t_idx2.t_tt_id 		= v_t_new.t_tt_id ;
	v_t_idx2.t_is_cash 		= v_t_new.t_is_cash ;
	v_t_idx2.t_qty 			= v_t_new.t_qty ;
	v_t_idx2.t_exec_name 	= v_t_new.t_exec_name ;
	v_t_idx2.t_trade_price 	= v_t_new.t_trade_price ;
	tbl_t_s_symb_index(1)->put(txn, Encode(k_t_idx2), Encode(obj_v, v_t_idx2));

	trade_history::key k_th;
	trade_history::value v_th;
	k_th.th_t_id = pIn->trade_id;
	k_th.th_dts = TimeStampToTimeT(pIn->trade_dts);	
	v_th.th_st_id = string(pIn->st_completed_id);
	tbl_trade_history(1)->insert(txn, Encode(k_th), Encode(obj_v, v_th));

	const broker::key k_b(pIn->broker_id);
	ALWAYS_ASSERT(tbl_broker(1)->get(txn, Encode(obj_key0, k_b), obj_v));
	broker::value v_b_temp;
	const broker::value *v_b = Decode(obj_v, v_b_temp);
	broker::value v_b_new(*v_b);
	v_b_new.b_comm_total += pIn->comm_amount;
	v_b_new.b_num_trades += 1;
	tbl_broker(1)->put(txn, Encode(k_b), Encode(obj_v, v_b_new));
}

void tpce_worker::DoTradeResultFrame6(const TTradeResultFrame6Input *pIn, TTradeResultFrame6Output *pOut)
{
	string cash_type;

	if( pIn->trade_is_cash )
		cash_type = "Cash Account";
	else
		cash_type = "Margin";

	settlement::key k_se;
	settlement::value v_se;
	k_se.se_t_id = pIn->trade_id;
	v_se.se_cash_type = cash_type;
	v_se.se_cash_due_date = TimeStampToTimeT(pIn->due_date);
	v_se.se_amt = pIn->se_amount;
	tbl_settlement(1)->insert(txn, Encode(k_se), Encode(obj_v, v_se));

	if( pIn->trade_is_cash )
	{
		const customer_account::key k_ca(pIn->acct_id);
		ALWAYS_ASSERT(tbl_customer_account(1)->get(txn, Encode(obj_key0, k_ca), obj_v));
		customer_account::value v_ca_temp;
		const customer_account::value *v_ca = Decode(obj_v, v_ca_temp);
		customer_account::value v_ca_new(*v_ca);
		v_ca_new.ca_bal += pIn->se_amount;
		tbl_customer_account(1)->put(txn, Encode(k_ca), Encode(obj_v, v_ca_new));

		ca_id_index::key k_idx_ca;
		ca_id_index::value v_idx_ca;
		k_idx_ca.ca_id = k_ca.ca_id;
		k_idx_ca.ca_c_id = v_ca_new.ca_c_id;
		v_idx_ca.ca_bal = v_ca_new.ca_bal;
		tbl_ca_id_index(1)->put(txn, Encode(k_idx_ca), Encode(obj_v, v_idx_ca));

		cash_transaction::key k_ct;
		cash_transaction::value v_ct;
		k_ct.ct_t_id = pIn->trade_id;
		v_ct.ct_dts = TimeStampToTimeT(pIn->trade_dts);
		v_ct.ct_amt = pIn->se_amount;
		v_ct.ct_name = string(pIn->type_name) + " " + to_string(pIn->trade_qty) + " shares of " + string(pIn->s_name);
		tbl_cash_transaction(1)->insert(txn, Encode(k_ct), Encode(obj_v, v_ct));
	}

	const customer_account::key k_ca(pIn->acct_id);
	ALWAYS_ASSERT(tbl_customer_account(1)->get(txn, Encode(obj_key0, k_ca), obj_v));
	customer_account::value v_ca_temp;
	const customer_account::value *v_ca = Decode(obj_v, v_ca_temp);
	pOut->acct_bal = v_ca->ca_bal;
	
	db->commit_txn(txn);
}

void tpce_worker::DoTradeStatusFrame1(const TTradeStatusFrame1Input *pIn, TTradeStatusFrame1Output *pOut)
{
	scoped_str_arena s_arena(arena);
	txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);

	const t_ca_id_index::key k_t_0( pIn->acct_id, 0, 0 );
	const t_ca_id_index::key k_t_1( pIn->acct_id, numeric_limits<uint64_t>::max(), numeric_limits<int64_t>::max() );
	table_scanner t_scanner(s_arena.get());
	tbl_t_ca_id_index(1)->scan(txn, Encode(obj_key0, k_t_0), &Encode(obj_key1, k_t_1), t_scanner, s_arena.get());
	ALWAYS_ASSERT( t_scanner.output.size() );

	int t_cursor = 0;
	for( auto &r_t : t_scanner.output )
	{
		t_ca_id_index::key k_t_temp;
		t_ca_id_index::value v_t_temp;
		const t_ca_id_index::key* k_t = Decode( *r_t.first, k_t_temp );
		const t_ca_id_index::value* v_t = Decode( *r_t.second, v_t_temp );

		const status_type::key k_st(v_t->t_st_id);
		ALWAYS_ASSERT(tbl_status_type(1)->get(txn, Encode(obj_key0, k_st), obj_v));
		status_type::value v_st_temp;
		const status_type::value *v_st = Decode(obj_v, v_st_temp);

		const trade_type::key k_tt(v_t->t_tt_id);
		ALWAYS_ASSERT(tbl_trade_type(1)->get(txn, Encode(obj_key0, k_tt), obj_v));
		trade_type::value v_tt_temp;
		const trade_type::value *v_tt = Decode(obj_v, v_tt_temp);

		const security::key k_s(v_t->t_s_symb);
		ALWAYS_ASSERT(tbl_security(1)->get(txn, Encode(obj_key0, k_s), obj_v));
		security::value v_s_temp;
		const security::value *v_s = Decode(obj_v, v_s_temp);

		const exchange::key k_ex(v_s->s_ex_id);
		ALWAYS_ASSERT(tbl_exchange(1)->get(txn, Encode(obj_key0, k_ex), obj_v));
		exchange::value v_ex_temp;
		const exchange::value *v_ex = Decode(obj_v, v_ex_temp);

		pOut->trade_id[t_cursor] = k_t->t_id;
		pOut->trade_dts[t_cursor] = TimeTToTimeStamp((time_t)k_t->t_dts);
		memcpy(pOut->status_name[t_cursor], v_st->st_name.data(), v_st->st_name.size() );
		memcpy(pOut->type_name[t_cursor], v_tt->tt_name.data(), v_tt->tt_name.size());
		memcpy(pOut->symbol[t_cursor], v_t->t_s_symb.data(), v_t->t_s_symb.size() );
		pOut->trade_qty[t_cursor] = v_t->t_qty;
		memcpy(pOut->exec_name[t_cursor], v_t->t_exec_name.data(), v_t->t_exec_name.size() );
		pOut->charge[t_cursor] = v_t->t_chrg;
		memcpy(pOut->s_name[t_cursor], v_s->s_name.data(), v_s->s_name.size());
		memcpy(pOut->ex_name[t_cursor], v_ex->ex_name.data(), v_ex->ex_name.size() );
		
		t_cursor++;
		if( t_cursor >= max_trade_status_len )
			break;
	}

	pOut->num_found = t_cursor;

	const customer_account::key k_ca(pIn->acct_id);
	ALWAYS_ASSERT(tbl_customer_account(1)->get(txn, Encode(obj_key0, k_ca), obj_v));
	customer_account::value v_ca_temp;
	const customer_account::value *v_ca = Decode(obj_v, v_ca_temp);

	const customers::key k_c(v_ca->ca_c_id);
	ALWAYS_ASSERT(tbl_customers(1)->get(txn, Encode(obj_key0, k_c), obj_v));
	customers::value v_c_temp;
	const customers::value *v_c = Decode(obj_v, v_c_temp);

	const broker::key k_b(v_ca->ca_b_id);
	ALWAYS_ASSERT(tbl_broker(1)->get(txn, Encode(obj_key0, k_b), obj_v));
	broker::value v_b_temp;
	const broker::value *v_b = Decode(obj_v, v_b_temp);

	memcpy(pOut->cust_f_name, v_c->c_f_name.data(), v_c->c_f_name.size() );
	memcpy(pOut->cust_l_name, v_c->c_l_name.data(), v_c->c_l_name.size() );
	memcpy(pOut->broker_name, v_b->b_name.data(), v_b->b_name.size() );

	db->commit_txn(txn);
}

void tpce_worker::DoTradeUpdateFrame1(const TTradeUpdateFrame1Input *pIn, TTradeUpdateFrame1Output *pOut)
{
	scoped_str_arena s_arena(arena);
	txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);

	for( auto i = 0; i < pIn->max_trades; i++ )
	{
		const trade::key k_t(pIn->trade_id[i]);
		ALWAYS_ASSERT(tbl_trade(1)->get(txn, Encode(obj_key0, k_t), obj_v));
		trade::value v_t_temp;
		const trade::value *v_t = Decode(obj_v, v_t_temp);
		pOut->num_found++;
		
		const trade_type::key k_tt(v_t->t_tt_id);
		ALWAYS_ASSERT(tbl_trade_type(1)->get(txn, Encode(obj_key0, k_tt), obj_v));
		trade_type::value v_tt_temp;
		const trade_type::value *v_tt = Decode(obj_v, v_tt_temp);

		pOut->trade_info[i].bid_price = v_t->t_bid_price;
		pOut->trade_info[i].is_cash = v_t->t_is_cash;
		pOut->trade_info[i].is_market = v_tt->tt_is_mrkt;
		pOut->trade_info[i].trade_price = v_t->t_trade_price;

		if( pOut->num_updated < pIn->max_updates )
		{
			string temp_exec_name = v_t->t_exec_name.str();
			size_t index = temp_exec_name.find(" X ");
			if(index != string::npos){
			temp_exec_name.replace(index, 3, "   ");
			} else {
			index = temp_exec_name.find("   ");
			temp_exec_name.replace(index, 3, " X ");
			}
			
			trade::value v_t_new(*v_t);
			v_t_new.t_exec_name = temp_exec_name;
			tbl_trade(1)->put(txn, Encode(k_t), Encode(obj_v, v_t_new));
			t_ca_id_index::key k_t_idx1;
			t_ca_id_index::value v_t_idx1;
			k_t_idx1.t_ca_id 		= v_t_new.t_ca_id;
			k_t_idx1.t_dts 			= v_t_new.t_dts;
			k_t_idx1.t_id 			= k_t.t_id;
			v_t_idx1.t_st_id 		= v_t_new.t_st_id ;
			v_t_idx1.t_tt_id 		= v_t_new.t_tt_id ;
			v_t_idx1.t_is_cash 		= v_t_new.t_is_cash ;
			v_t_idx1.t_s_symb 		= v_t_new.t_s_symb ;
			v_t_idx1.t_qty 			= v_t_new.t_qty ;
			v_t_idx1.t_bid_price 	= v_t_new.t_bid_price ;
			v_t_idx1.t_exec_name 	= v_t_new.t_exec_name ;
			v_t_idx1.t_trade_price 	= v_t_new.t_trade_price ;
			v_t_idx1.t_chrg 		= v_t_new.t_chrg ;
			tbl_t_ca_id_index(1)->put(txn, Encode(k_t_idx1), Encode(obj_v, v_t_idx1));

			t_s_symb_index::key k_t_idx2;
			t_s_symb_index::value v_t_idx2;
			k_t_idx2.t_s_symb 		= v_t_new.t_s_symb ;
			k_t_idx2.t_dts 			= v_t_new.t_dts;
			k_t_idx2.t_id 			= k_t.t_id;
			v_t_idx2.t_ca_id 		= v_t_new.t_ca_id;
			v_t_idx2.t_st_id 		= v_t_new.t_st_id ;
			v_t_idx2.t_tt_id 		= v_t_new.t_tt_id ;
			v_t_idx2.t_is_cash 		= v_t_new.t_is_cash ;
			v_t_idx2.t_qty 			= v_t_new.t_qty ;
			v_t_idx2.t_exec_name 	= v_t_new.t_exec_name ;
			v_t_idx2.t_trade_price 	= v_t_new.t_trade_price ;
			tbl_t_s_symb_index(1)->put(txn, Encode(k_t_idx2), Encode(obj_v, v_t_idx2));

			pOut->num_updated++;
			memcpy(pOut->trade_info[i].exec_name, temp_exec_name.data(), temp_exec_name.size());
		}
		else
		{
			memcpy(pOut->trade_info[i].exec_name, v_t->t_exec_name.data(), v_t->t_exec_name.size());
		}
		
		const settlement::key k_se(pIn->trade_id[i]);
		ALWAYS_ASSERT(tbl_settlement(1)->get(txn, Encode(obj_key0, k_se), obj_v));
		settlement::value v_se_temp;
		const settlement::value *v_se = Decode(obj_v, v_se_temp);
		pOut->trade_info[i].settlement_amount = v_se->se_amt;
		pOut->trade_info[i].settlement_cash_due_date = TimeTToTimeStamp(v_se->se_cash_due_date);
		memcpy(pOut->trade_info[i].settlement_cash_type, v_se->se_cash_type.data(), v_se->se_cash_type.size());
		
		if( pOut->trade_info[i].is_cash )
		{
			const cash_transaction::key k_ct(pIn->trade_id[i]);
			ALWAYS_ASSERT(tbl_cash_transaction(1)->get(txn, Encode(obj_key0, k_ct), obj_v));
			cash_transaction::value v_ct_temp;
			const cash_transaction::value *v_ct = Decode(obj_v, v_ct_temp);
			pOut->trade_info[i].cash_transaction_amount = v_ct->ct_amt;
			pOut->trade_info[i].cash_transaction_dts = TimeTToTimeStamp(v_ct->ct_dts);
			memcpy(pOut->trade_info[i].cash_transaction_name, v_ct->ct_name.data(), v_ct->ct_name.size());
		}

		const trade_history::key k_th_0( pIn->trade_id[i], 0 );
		const trade_history::key k_th_1( pIn->trade_id[i], numeric_limits<int64_t>::max() );
		table_scanner th_scanner(s_arena.get());
		tbl_trade_history(1)->scan(txn, Encode(obj_key0, k_th_0), &Encode(obj_key1, k_th_1), th_scanner, s_arena.get());
		ALWAYS_ASSERT( th_scanner.output.size() );

		for( auto th_cursor = 0; th_cursor < 3 and th_cursor < th_scanner.output.size(); th_cursor++ )
		{
			auto& r_th = th_scanner.output[th_cursor];
			trade_history::key k_th_temp;
			trade_history::value v_th_temp;
			const trade_history::key* k_th = Decode( *r_th.first, k_th_temp );
			const trade_history::value* v_th = Decode( *r_th.second, v_th_temp );

			pOut->trade_info[i].trade_history_dts[th_cursor] = TimeTToTimeStamp(k_th->th_dts);
			memcpy( pOut->trade_info[i].trade_history_status_id[th_cursor], v_th->th_st_id.data(), v_th->th_st_id.size());
		}
	}

	db->commit_txn(txn);
}

void tpce_worker::DoTradeUpdateFrame2(const TTradeUpdateFrame2Input *pIn, TTradeUpdateFrame2Output *pOut)
{
	scoped_str_arena s_arena(arena);
	txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);

	const t_ca_id_index::key k_t_0( pIn->acct_id, TimeStampToTimeT(pIn->start_trade_dts), 0 );
	const t_ca_id_index::key k_t_1( pIn->acct_id, TimeStampToTimeT(pIn->end_trade_dts), numeric_limits<int64_t>::max() );
	table_scanner t_scanner(s_arena.get());
	tbl_t_ca_id_index(1)->scan(txn, Encode(obj_key0, k_t_0), &Encode(obj_key1, k_t_1), t_scanner, s_arena.get());
	ALWAYS_ASSERT( t_scanner.output.size() );

	for( int i = 0; i < pIn->max_trades and i < t_scanner.output.size(); i++ )
	{
		auto &r_t = t_scanner.output[i];
		trade::key k_t_temp;
		trade::value v_t_temp;
		const trade::key* k_t = Decode( *r_t.first, k_t_temp );
		const trade::value* v_t = Decode( *r_t.second, v_t_temp );

		pOut->trade_info[i].bid_price = v_t->t_bid_price;
		memcpy(pOut->trade_info[i].exec_name, v_t->t_exec_name.data(), v_t->t_exec_name.size());
		pOut->trade_info[i].is_cash = v_t->t_is_cash;
		pOut->trade_info[i].trade_id= k_t->t_id;
		pOut->trade_info[i].trade_price = v_t->t_trade_price;

		pOut->num_found = i;
	}
	pOut->num_updated = 0;

	for( int i = 0; i < pOut->num_found; i++ )
	{
		const settlement::key k_se(pOut->trade_info[i].trade_id);
		ALWAYS_ASSERT(tbl_settlement(1)->get(txn, Encode(obj_key0, k_se), obj_v));
		settlement::value v_se_temp;
		const settlement::value *v_se = Decode(obj_v, v_se_temp);

		if( pOut->num_updated < pIn->max_updates )
		{
			settlement::value v_se_new(*v_se);

			if( pOut->trade_info[i].is_cash ){
				if( v_se_new.se_cash_type == "Cash Account" )
					v_se_new.se_cash_type = "Cash";
				else
					v_se_new.se_cash_type = "Cash Account";
			}
			else{
				if( v_se_new.se_cash_type == "Margin Account" )
					v_se_new.se_cash_type = "Margin";
				else
					v_se_new.se_cash_type = "Margin Account";
			}
			tbl_settlement(1)->put(txn, Encode(k_se), Encode(obj_v, v_se_new));

			pOut->num_updated++;
		}

		pOut->trade_info[i].settlement_amount = v_se->se_amt;
		pOut->trade_info[i].settlement_cash_due_date = TimeTToTimeStamp(v_se->se_cash_due_date);
		memcpy(pOut->trade_info[i].settlement_cash_type, v_se->se_cash_type.data(), v_se->se_cash_type.size());

		if( pOut->trade_info[i].is_cash )
		{
			const cash_transaction::key k_ct(pOut->trade_info[i].trade_id);
			ALWAYS_ASSERT(tbl_cash_transaction(1)->get(txn, Encode(obj_key0, k_ct), obj_v));
			cash_transaction::value v_ct_temp;
			const cash_transaction::value *v_ct = Decode(obj_v, v_ct_temp);
			pOut->trade_info[i].cash_transaction_amount = v_ct->ct_amt;
			pOut->trade_info[i].cash_transaction_dts = TimeTToTimeStamp(v_ct->ct_dts);
			memcpy(pOut->trade_info[i].cash_transaction_name, v_ct->ct_name.data(), v_ct->ct_name.size());
		}

		const trade_history::key k_th_0( pOut->trade_info[i].trade_id, 0 );
		const trade_history::key k_th_1( pOut->trade_info[i].trade_id, numeric_limits<int64_t>::max() );
		table_scanner th_scanner(s_arena.get());
		tbl_trade_history(1)->scan(txn, Encode(obj_key0, k_th_0), &Encode(obj_key1, k_th_1), th_scanner, s_arena.get());
		ALWAYS_ASSERT( th_scanner.output.size() );

		for( auto th_cursor = 0; th_cursor < 3 and th_cursor < th_scanner.output.size(); th_cursor++ )
		{
			auto& r_th = th_scanner.output[th_cursor];
			trade_history::key k_th_temp;
			trade_history::value v_th_temp;
			const trade_history::key* k_th = Decode( *r_th.first, k_th_temp );
			const trade_history::value* v_th = Decode( *r_th.second, v_th_temp );

			pOut->trade_info[i].trade_history_dts[th_cursor] = TimeTToTimeStamp(k_th->th_dts);
			memcpy( pOut->trade_info[i].trade_history_status_id[th_cursor], v_th->th_st_id.data(), v_th->th_st_id.size());
		}
	}

	db->commit_txn(txn);
}

void tpce_worker::DoTradeUpdateFrame3(const TTradeUpdateFrame3Input *pIn, TTradeUpdateFrame3Output *pOut)
{
	scoped_str_arena s_arena(arena);
	txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);

	const t_s_symb_index::key k_t_0( string(pIn->symbol), TimeStampToTimeT(pIn->start_trade_dts), 0 );
	const t_s_symb_index::key k_t_1( string(pIn->symbol), TimeStampToTimeT(pIn->end_trade_dts), numeric_limits<uint64_t>::max() );
	table_scanner t_scanner(s_arena.get());
	tbl_t_s_symb_index(1)->scan(txn, Encode(obj_key0, k_t_0), &Encode(obj_key1, k_t_1), t_scanner, s_arena.get());
//	ALWAYS_ASSERT( t_scanner.output.size() );		// XXX. short innitial trading day can make this case happening?

	for( int i = 0; i < pIn->max_trades and i < t_scanner.output.size() ; i++ )
	{
		auto &r_t = t_scanner.output[i];
		t_s_symb_index::key k_t_temp;
		t_s_symb_index::value v_t_temp;
		const t_s_symb_index::key* k_t = Decode( *r_t.first, k_t_temp );
		const t_s_symb_index::value* v_t = Decode( *r_t.second, v_t_temp );


		const trade_type::key k_tt(v_t->t_tt_id);
		ALWAYS_ASSERT(tbl_trade_type(1)->get(txn, Encode(obj_key0, k_tt), obj_v));
		trade_type::value v_tt_temp;
		const trade_type::value *v_tt = Decode(obj_v, v_tt_temp);

		const security::key k_s(k_t->t_s_symb);
		ALWAYS_ASSERT(tbl_security(1)->get(txn, Encode(obj_key0, k_s), obj_v));
		security::value v_s_temp;
		const security::value *v_s = Decode(obj_v, v_s_temp);

		/*
		   acct_id[]    = T_CA_ID, 
		   exec_name[]  = T_EXEC_NAME, 
		   is_cash[]    = T_IS_CASH, 
		   price[]      = T_TRADE_PRICE, 
		   quantity[]   = T_QTY, 
		   s_name[]     = S_NAME, 
		   trade_dts[]  = T_DTS, 
		   trade_list[] = T_ID, 
		   trade_type[] = T_TT_ID, 
		   type_name[]  = TT_NAME
		   */
		pOut->trade_info[i].acct_id = v_t->t_ca_id;
		memcpy(pOut->trade_info[i].exec_name, v_t->t_exec_name.data(), v_t->t_exec_name.size());
		pOut->trade_info[i].is_cash = v_t->t_is_cash;
		pOut->trade_info[i].price = v_t->t_trade_price;
		pOut->trade_info[i].quantity = v_t->t_qty;
		memcpy(pOut->trade_info[i].s_name, v_s->s_name.data(), v_s->s_name.size());
		pOut->trade_info[i].trade_dts = TimeTToTimeStamp(k_t->t_dts);
		pOut->trade_info[i].trade_id = k_t->t_id;
		memcpy(pOut->trade_info[i].trade_type, v_t->t_tt_id.data(), v_t->t_tt_id.size());
		memcpy(pOut->trade_info[i].type_name, v_tt->tt_name.data(), v_tt->tt_name.size());

		pOut->num_found = i;
	}
	pOut->num_updated = 0;

	for( int i = 0; i < pOut->num_found; i++ )
	{
		const settlement::key k_se(pOut->trade_info[i].trade_id);
		ALWAYS_ASSERT(tbl_settlement(1)->get(txn, Encode(obj_key0, k_se), obj_v));
		settlement::value v_se_temp;
		const settlement::value *v_se = Decode(obj_v, v_se_temp);

		if( pOut->trade_info[i].is_cash )
		{
			const cash_transaction::key k_ct(pOut->trade_info[i].trade_id);
			ALWAYS_ASSERT(tbl_cash_transaction(1)->get(txn, Encode(obj_key0, k_ct), obj_v));
			cash_transaction::value v_ct_temp;
			const cash_transaction::value *v_ct = Decode(obj_v, v_ct_temp);

			if( pOut->num_updated < pIn->max_updates )
			{
				string temp_ct_name = v_ct->ct_name.str();
				size_t index = temp_ct_name.find(" shares of ");
				if(index != string::npos){
					stringstream ss;
					ss << pOut->trade_info[i].type_name <<  " " << pOut->trade_info[i].quantity
						<< " Shares of " << pOut->trade_info[i].s_name;
					temp_ct_name = ss.str();
				} else {
					stringstream ss;
					ss << pOut->trade_info[i].type_name <<  " " << pOut->trade_info[i].quantity
						<< " shares of " << pOut->trade_info[i].s_name;
					temp_ct_name = ss.str();
				}

				cash_transaction::value v_ct_new(*v_ct);
				v_ct_new.ct_name = temp_ct_name;

				tbl_cash_transaction(1)->put(txn, Encode(k_ct), Encode(obj_v, v_ct_new));
				pOut->num_updated++;

				memcpy(pOut->trade_info[i].cash_transaction_name, v_ct_new.ct_name.data(), v_ct_new.ct_name.size());
			}
			else
			{
				memcpy(pOut->trade_info[i].cash_transaction_name, v_ct->ct_name.data(), v_ct->ct_name.size());
			}
			
			pOut->trade_info[i].cash_transaction_amount = v_ct->ct_amt;
			pOut->trade_info[i].cash_transaction_dts = TimeTToTimeStamp(v_ct->ct_dts);
		}

		const trade_history::key k_th_0( pOut->trade_info[i].trade_id, 0 );
		const trade_history::key k_th_1( pOut->trade_info[i].trade_id, numeric_limits<int64_t>::max() );
		table_scanner th_scanner(s_arena.get());
		tbl_trade_history(1)->scan(txn, Encode(obj_key0, k_th_0), &Encode(obj_key1, k_th_1), th_scanner, s_arena.get());
		ALWAYS_ASSERT( th_scanner.output.size() );

		for( auto th_cursor = 0; th_cursor < 3 and th_cursor < th_scanner.output.size(); th_cursor++ )
		{
			auto& r_th = th_scanner.output[th_cursor];
			trade_history::key k_th_temp;
			trade_history::value v_th_temp;
			const trade_history::key* k_th = Decode( *r_th.first, k_th_temp );
			const trade_history::value* v_th = Decode( *r_th.second, v_th_temp );

			pOut->trade_info[i].trade_history_dts[th_cursor] = TimeTToTimeStamp(k_th->th_dts);
			memcpy( pOut->trade_info[i].trade_history_status_id[th_cursor], v_th->th_st_id.data(), v_th->th_st_id.size());
		}
	}

	db->commit_txn(txn);
}

void tpce_worker::DoDataMaintenanceFrame1(const TDataMaintenanceFrame1Input *pIn){}
void tpce_worker::DoTradeCleanupFrame1(const TTradeCleanupFrame1Input *pIn){}

class tpce_charge_loader : public bench_loader, public tpce_worker_mixin {
	public:
		tpce_charge_loader(unsigned long seed,
				abstract_db *db,
				const map<string, abstract_ordered_index *> &open_tables,
				const map<string, vector<abstract_ordered_index *>> &partitions,
				ssize_t partition_id)
			: bench_loader(seed, db, open_tables),
			tpce_worker_mixin(partitions),
			partition_id(partition_id)
	{
		ALWAYS_ASSERT(partition_id == -1 ||
				(partition_id >= 1 &&
				 static_cast<size_t>(partition_id) <= NumPartitions()));
	}

	protected:
		virtual void
			load()
			{
				try{
					pGenerateAndLoad->InitCharge();
					bool isLast = pGenerateAndLoad->isLastCharge();
					while(!isLast) {
						PCHARGE_ROW record = pGenerateAndLoad->getChargeRow();
						chargeBuffer.append(record);
						isLast= pGenerateAndLoad->isLastCharge();
					}
					chargeBuffer.setMoreToRead(false);
					int rows=chargeBuffer.getSize();
					for(int i=0; i<rows; i++){
						PCHARGE_ROW record = chargeBuffer.get(i);
						charge::key k;
						charge::value v;
						string obj_buf;

						k.ch_tt_id = string( record->CH_TT_ID );
						k.ch_c_tier = record->CH_C_TIER;
						v.ch_chrg = record->CH_CHRG;

						void *txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);
						tbl_charge(1)->insert(txn, Encode(k), Encode(obj_buf, v));
						db->commit_txn(txn);
						// TODO. sanity check

						// Partitioning by customer?
					}
					pGenerateAndLoad->ReleaseCharge();
					chargeBuffer.release();
				} catch (abstract_db::abstract_abort_exception &ex) {
					// shouldn't abort on loading!
					ALWAYS_ASSERT(false);
				}
			}
	private:
		ssize_t partition_id;
};

class tpce_commission_rate_loader : public bench_loader, public tpce_worker_mixin {
	public:
		tpce_commission_rate_loader(unsigned long seed,
				abstract_db *db,
				const map<string, abstract_ordered_index *> &open_tables,
				const map<string, vector<abstract_ordered_index *>> &partitions,
				ssize_t partition_id)
			: bench_loader(seed, db, open_tables),
			tpce_worker_mixin(partitions),
			partition_id(partition_id)
	{
		ALWAYS_ASSERT(partition_id == -1 ||
				(partition_id >= 1 &&
				 static_cast<size_t>(partition_id) <= NumPartitions()));
	}

	protected:
		virtual void
			load()
			{
				try{
					pGenerateAndLoad->InitCommissionRate();
					bool isLast = pGenerateAndLoad->isLastCommissionRate();
					while(!isLast) {
						PCOMMISSION_RATE_ROW record = pGenerateAndLoad->getCommissionRateRow();
						commissionRateBuffer.append(record);
						isLast= pGenerateAndLoad->isLastCommissionRate();
					}
					commissionRateBuffer.setMoreToRead(false);
					int rows=commissionRateBuffer.getSize();
					for(int i=0; i<rows; i++){
						PCOMMISSION_RATE_ROW record = commissionRateBuffer.get(i);
						commission_rate::key k;
						commission_rate::value v;
						string obj_buf;

						k.cr_c_tier = record->CR_C_TIER;
						k.cr_tt_id = string(record->CR_TT_ID);
						k.cr_ex_id = string(record->CR_EX_ID);
						k.cr_from_qty = record->CR_FROM_QTY;
						v.cr_to_qty = record->CR_TO_QTY;
						v.cr_rate = record->CR_RATE;

						void *txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);
						tbl_commission_rate(1)->insert(txn, Encode(k), Encode(obj_buf, v));
						db->commit_txn(txn);
					}
					pGenerateAndLoad->ReleaseCommissionRate();
					commissionRateBuffer.release();
				} catch (abstract_db::abstract_abort_exception &ex) {
					// shouldn't abort on loading!
					ALWAYS_ASSERT(false);
				}
			}

	private:
		ssize_t partition_id;
};

class tpce_exchange_loader : public bench_loader, public tpce_worker_mixin {
	public:
		tpce_exchange_loader(unsigned long seed,
				abstract_db *db,
				const map<string, abstract_ordered_index *> &open_tables,
				const map<string, vector<abstract_ordered_index *>> &partitions,
				ssize_t partition_id)
			: bench_loader(seed, db, open_tables),
			tpce_worker_mixin(partitions)
	{}

	protected:
		virtual void
			load()
			{
				try{
					pGenerateAndLoad->InitExchange();
					bool isLast = pGenerateAndLoad->isLastExchange();
					while(!isLast) {
						PEXCHANGE_ROW record = pGenerateAndLoad->getExchangeRow();
						exchangeBuffer.append(record);
						isLast= pGenerateAndLoad->isLastExchange();
					}
					exchangeBuffer.setMoreToRead(false);
					int rows=exchangeBuffer.getSize();
					for(int i=0; i<rows; i++){
						PEXCHANGE_ROW record = exchangeBuffer.get(i);
						exchange::key k;
						exchange::value v;
						string obj_buf;

						k.ex_id = string(record->EX_ID);
						v.ex_name = string(record->EX_NAME);
						v.ex_num_symb = record->EX_NUM_SYMB;
						v.ex_open= record->EX_OPEN;
						v.ex_close= record->EX_CLOSE;
						v.ex_desc = string(record->EX_DESC);
						v.ex_ad_id= record->EX_AD_ID;

						void *txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);
						tbl_exchange(1)->insert(txn, Encode(k), Encode(obj_buf, v));
						db->commit_txn(txn);
					}
					pGenerateAndLoad->ReleaseExchange();
					exchangeBuffer.release();
				} catch (abstract_db::abstract_abort_exception &ex) {
					// shouldn't abort on loading!
					ALWAYS_ASSERT(false);
				}
			}
};

class tpce_industry_loader : public bench_loader, public tpce_worker_mixin {
	public:
		tpce_industry_loader(unsigned long seed,
				abstract_db *db,
				const map<string, abstract_ordered_index *> &open_tables,
				const map<string, vector<abstract_ordered_index *>> &partitions,
				ssize_t partition_id)
			: bench_loader(seed, db, open_tables),
			tpce_worker_mixin(partitions),
			partition_id(partition_id)
	{
		ALWAYS_ASSERT(partition_id == -1 ||
				(partition_id >= 1 &&
				 static_cast<size_t>(partition_id) <= NumPartitions()));
	}

	protected:
		virtual void
			load()
			{
				try{
					pGenerateAndLoad->InitIndustry();
					bool isLast = pGenerateAndLoad->isLastIndustry();
					while(!isLast) {
						PINDUSTRY_ROW record = pGenerateAndLoad->getIndustryRow();
						industryBuffer.append(record);
						isLast= pGenerateAndLoad->isLastIndustry();
					}
					industryBuffer.setMoreToRead(false);
					int rows=industryBuffer.getSize();
					for(int i=0; i<rows; i++){
						PINDUSTRY_ROW record = industryBuffer.get(i);
						industry::key k_in;
						industry::value v_in;
						in_name_index::key k_in_idx;
						in_name_index::value v_in_idx;
						string obj_buf;

						k_in.in_id = string(record->IN_ID);
						v_in.in_name = string(record->IN_NAME);
						v_in.in_sc_id = string(record->IN_SC_ID);

						k_in_idx.in_name = string(record->IN_NAME);
						k_in_idx.in_id = string(record->IN_ID);

						void *txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);
						tbl_industry(1)->insert(txn, Encode(k_in), Encode(obj_buf, v_in));
						tbl_in_name_index(1)->insert(txn, Encode(k_in_idx), Encode(obj_buf, v_in_idx));
						db->commit_txn(txn);
					}
					pGenerateAndLoad->ReleaseIndustry();
					industryBuffer.release();
				} catch (abstract_db::abstract_abort_exception &ex) {
					// shouldn't abort on loading!
					ALWAYS_ASSERT(false);
				}
			}

	private:
		ssize_t partition_id;
};

class tpce_sector_loader : public bench_loader, public tpce_worker_mixin {
	public:
		tpce_sector_loader(unsigned long seed,
				abstract_db *db,
				const map<string, abstract_ordered_index *> &open_tables,
				const map<string, vector<abstract_ordered_index *>> &partitions,
				ssize_t partition_id)
			: bench_loader(seed, db, open_tables),
			tpce_worker_mixin(partitions),
			partition_id(partition_id)
	{
		ALWAYS_ASSERT(partition_id == -1 ||
				(partition_id >= 1 &&
				 static_cast<size_t>(partition_id) <= NumPartitions()));
	}

	protected:
		size_t
			NumOrderLinesPerCustomer()
			{
				return RandomNumber(r, 5, 15);
			}

		virtual void
			load()
			{
				try{
					pGenerateAndLoad->InitSector();
					bool isLast = pGenerateAndLoad->isLastSector();
					while(!isLast) {
						PSECTOR_ROW record = pGenerateAndLoad->getSectorRow();
						sectorBuffer.append(record);
						isLast= pGenerateAndLoad->isLastSector();
					}
					sectorBuffer.setMoreToRead(false);
					int rows=sectorBuffer.getSize();
					for(int i=0; i<rows; i++){
						PSECTOR_ROW record = sectorBuffer.get(i);
						sector::key k;
						sector::value v;
						string obj_buf;

						k.sc_name= string(record->SC_NAME);
						k.sc_id= string(record->SC_ID);
						v.dummy = true;

						void *txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);
						tbl_sector(1)->insert(txn, Encode(k), Encode(obj_buf, v));
						db->commit_txn(txn);
					}
					pGenerateAndLoad->ReleaseSector();
					sectorBuffer.release();
				} catch (abstract_db::abstract_abort_exception &ex) {
					// shouldn't abort on loading!
					ALWAYS_ASSERT(false);
				}
			}

	private:
		ssize_t partition_id;
};

class tpce_status_type_loader : public bench_loader, public tpce_worker_mixin {
	public:
		tpce_status_type_loader(unsigned long seed,
				abstract_db *db,
				const map<string, abstract_ordered_index *> &open_tables,
				const map<string, vector<abstract_ordered_index *>> &partitions,
				ssize_t partition_id)
			: bench_loader(seed, db, open_tables),
			tpce_worker_mixin(partitions),
			partition_id(partition_id)
	{
		ALWAYS_ASSERT(partition_id == -1 ||
				(partition_id >= 1 &&
				 static_cast<size_t>(partition_id) <= NumPartitions()));
	}

	protected:
		size_t
			NumOrderLinesPerCustomer()
			{
				return RandomNumber(r, 5, 15);
			}

		virtual void
			load()
			{
				try{
					pGenerateAndLoad->InitStatusType();
					bool isLast = pGenerateAndLoad->isLastStatusType();
					while(!isLast) {
						PSTATUS_TYPE_ROW record = pGenerateAndLoad->getStatusTypeRow();
						statusTypeBuffer.append(record);
						isLast= pGenerateAndLoad->isLastStatusType();
					}
					statusTypeBuffer.setMoreToRead(false);
					int rows=statusTypeBuffer.getSize();
					for(int i=0; i<rows; i++){
						PSTATUS_TYPE_ROW record = statusTypeBuffer.get(i);
						status_type::key k;
						status_type::value v;
						string obj_buf;

						k.st_id = string(record->ST_ID);
						v.st_name = string(record->ST_NAME );

						void *txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);
						tbl_status_type(1)->insert(txn, Encode(k), Encode(obj_buf, v));
						db->commit_txn(txn);
					}
					pGenerateAndLoad->ReleaseStatusType();
					statusTypeBuffer.release();
				} catch (abstract_db::abstract_abort_exception &ex) {
					// shouldn't abort on loading!
					ALWAYS_ASSERT(false);
				}
			}

	private:
		ssize_t partition_id;
};

class tpce_tax_rate_loader : public bench_loader, public tpce_worker_mixin {
	public:
		tpce_tax_rate_loader(unsigned long seed,
				abstract_db *db,
				const map<string, abstract_ordered_index *> &open_tables,
				const map<string, vector<abstract_ordered_index *>> &partitions,
				ssize_t partition_id)
			: bench_loader(seed, db, open_tables),
			tpce_worker_mixin(partitions),
			partition_id(partition_id)
	{
		ALWAYS_ASSERT(partition_id == -1 ||
				(partition_id >= 1 &&
				 static_cast<size_t>(partition_id) <= NumPartitions()));
	}

	protected:
		size_t
			NumOrderLinesPerCustomer()
			{
				return RandomNumber(r, 5, 15);
			}

		virtual void
			load()
			{
				try{
					pGenerateAndLoad->InitTaxrate();
					bool hasNext;
					do{
						hasNext= pGenerateAndLoad->hasNextTaxrate();
						PTAXRATE_ROW record = pGenerateAndLoad->getTaxrateRow();
						taxrateBuffer.append(record);
					} while(hasNext);
					taxrateBuffer.setMoreToRead(false);
					int rows=taxrateBuffer.getSize();
					for(int i=0; i<rows; i++){
						PTAXRATE_ROW record = taxrateBuffer.get(i);
						tax_rate::key k;
						tax_rate::value v;
						string obj_buf;

						k.tx_id = string(record->TX_ID);
						v.tx_name = string(record->TX_NAME );
						v.tx_rate = record->TX_RATE;

						void *txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);
						tbl_tax_rate(1)->insert(txn, Encode(k), Encode(obj_buf, v));
						db->commit_txn(txn);
					}
					pGenerateAndLoad->ReleaseTaxrate();
					taxrateBuffer.release();
				} catch (abstract_db::abstract_abort_exception &ex) {
					// shouldn't abort on loading!
					ALWAYS_ASSERT(false);
				}
			}

	private:
		ssize_t partition_id;
};

class tpce_trade_type_loader : public bench_loader, public tpce_worker_mixin {
	public:
		tpce_trade_type_loader(unsigned long seed,
				abstract_db *db,
				const map<string, abstract_ordered_index *> &open_tables,
				const map<string, vector<abstract_ordered_index *>> &partitions,
				ssize_t partition_id)
			: bench_loader(seed, db, open_tables),
			tpce_worker_mixin(partitions),
			partition_id(partition_id)
	{
		ALWAYS_ASSERT(partition_id == -1 ||
				(partition_id >= 1 &&
				 static_cast<size_t>(partition_id) <= NumPartitions()));
	}

	protected:
		size_t
			NumOrderLinesPerCustomer()
			{
				return RandomNumber(r, 5, 15);
			}

		virtual void
			load()
			{
				try{
					pGenerateAndLoad->InitTradeType();
					bool isLast = pGenerateAndLoad->isLastTradeType();
					while(!isLast) {
						PTRADE_TYPE_ROW record = pGenerateAndLoad->getTradeTypeRow();
						tradeTypeBuffer.append(record);
						isLast= pGenerateAndLoad->isLastTradeType();
					}
					tradeTypeBuffer.setMoreToRead(false);
					int rows=tradeTypeBuffer.getSize();
					for(int i=0; i<rows; i++){
						PTRADE_TYPE_ROW record = tradeTypeBuffer.get(i);
						trade_type::key k;
						trade_type::value v;
						string obj_buf;

						k.tt_id = string(record->TT_ID);
						v.tt_name = string(record->TT_NAME );
						v.tt_is_sell = record->TT_IS_SELL;
						v.tt_is_mrkt = record->TT_IS_MRKT;

						void *txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);
						tbl_trade_type(1)->insert(txn, Encode(k), Encode(obj_buf, v));
						db->commit_txn(txn);
					}
					pGenerateAndLoad->ReleaseTradeType();
					tradeTypeBuffer.release();
				} catch (abstract_db::abstract_abort_exception &ex) {
					// shouldn't abort on loading!
					ALWAYS_ASSERT(false);
				}
			}

	private:
		ssize_t partition_id;
};

class tpce_zip_code_loader : public bench_loader, public tpce_worker_mixin {
	public:
		tpce_zip_code_loader(unsigned long seed,
				abstract_db *db,
				const map<string, abstract_ordered_index *> &open_tables,
				const map<string, vector<abstract_ordered_index *>> &partitions,
				ssize_t partition_id)
			: bench_loader(seed, db, open_tables),
			tpce_worker_mixin(partitions),
			partition_id(partition_id)
	{
		ALWAYS_ASSERT(partition_id == -1 ||
				(partition_id >= 1 &&
				 static_cast<size_t>(partition_id) <= NumPartitions()));
	}

	protected:

		virtual void
			load()
			{
				try{
					pGenerateAndLoad->InitZipCode();
					bool hasNext = pGenerateAndLoad->hasNextZipCode();
					while(hasNext) {
						PZIP_CODE_ROW record = pGenerateAndLoad->getZipCodeRow();
						zipCodeBuffer.append(record);
						hasNext= pGenerateAndLoad->hasNextZipCode();
					}
					zipCodeBuffer.setMoreToRead(false);
					int rows=zipCodeBuffer.getSize();
					for(int i=0; i<rows; i++){
						PZIP_CODE_ROW record = zipCodeBuffer.get(i);
						zip_code::key k;
						zip_code::value v;
						string obj_buf;

						k.zc_code = string(record->ZC_CODE);
						v.zc_town = string(record->ZC_TOWN);
						v.zc_div = string(record->ZC_DIV);

						void *txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);
						tbl_zip_code(1)->insert(txn, Encode(k), Encode(obj_buf, v));
						db->commit_txn(txn);
					}
					pGenerateAndLoad->ReleaseZipCode();
					zipCodeBuffer.release();
				} catch (abstract_db::abstract_abort_exception &ex) {
					// shouldn't abort on loading!
					ALWAYS_ASSERT(false);
				}
			}

	private:
		ssize_t partition_id;
};

class tpce_address_loader : public bench_loader, public tpce_worker_mixin {
	public:
		tpce_address_loader(unsigned long seed,
				abstract_db *db,
				const map<string, abstract_ordered_index *> &open_tables,
				const map<string, vector<abstract_ordered_index *>> &partitions,
				ssize_t partition_id)
			: bench_loader(seed, db, open_tables),
			tpce_worker_mixin(partitions),
			partition_id(partition_id)
	{
		ALWAYS_ASSERT(partition_id == -1 ||
				(partition_id >= 1 &&
				 static_cast<size_t>(partition_id) <= NumPartitions()));
	}

	protected:

		virtual void
			load()
			{
				try{
					pGenerateAndLoad->InitAddress();
					while(addressBuffer.hasMoreToRead()){
						addressBuffer.reset();
						bool hasNext;
						do {
							hasNext= pGenerateAndLoad->hasNextAddress();
							PADDRESS_ROW record = pGenerateAndLoad->getAddressRow();
							addressBuffer.append(record);
						} while((hasNext && addressBuffer.hasSpace()));
						addressBuffer.setMoreToRead(hasNext);

						int rows=addressBuffer.getSize();
						for(int i=0; i<rows; i++){
							PADDRESS_ROW record = addressBuffer.get(i);
							address::key k;
							address::value v;
							string obj_buf;

							k.ad_id = record->AD_ID;
							v.ad_line1 = string(record->AD_LINE1);
							v.ad_line2 = string(record->AD_LINE2);
							v.ad_zc_code = string(record->AD_ZC_CODE );
							v.ad_ctry = string(record->AD_CTRY );

							void *txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);
							tbl_address(1)->insert(txn, Encode(k), Encode(obj_buf, v));
							db->commit_txn(txn);
						}
					}
					pGenerateAndLoad->ReleaseAddress();
					addressBuffer.release();
				} catch (abstract_db::abstract_abort_exception &ex) {
					// shouldn't abort on loading!
					ALWAYS_ASSERT(false);
				}
			}

	private:
		ssize_t partition_id;
};

class tpce_customer_loader : public bench_loader, public tpce_worker_mixin {
	public:
		tpce_customer_loader(unsigned long seed,
				abstract_db *db,
				const map<string, abstract_ordered_index *> &open_tables,
				const map<string, vector<abstract_ordered_index *>> &partitions,
				ssize_t partition_id)
			: bench_loader(seed, db, open_tables),
			tpce_worker_mixin(partitions),
			partition_id(partition_id)
	{
		ALWAYS_ASSERT(partition_id == -1 ||
				(partition_id >= 1 &&
				 static_cast<size_t>(partition_id) <= NumPartitions()));
	}

	protected:

		virtual void
			load()
			{
				try{
					pGenerateAndLoad->InitCustomer();
					while(customerBuffer.hasMoreToRead()){
						customerBuffer.reset();
						bool hasNext;
						do {
							hasNext= pGenerateAndLoad->hasNextCustomer();
							PCUSTOMER_ROW record = pGenerateAndLoad->getCustomerRow();
							customerBuffer.append(record);
						} while((hasNext && customerBuffer.hasSpace()));
						customerBuffer.setMoreToRead(hasNext);

						int rows=customerBuffer.getSize();
						for(int i=0; i<rows; i++){
							PCUSTOMER_ROW record = customerBuffer.get(i);
							customers::key k;
							customers::value v;
							string obj_buf;

							k.c_id			= record->C_ID;
							v.c_tax_id		= string(record->C_TAX_ID);
							v.c_st_id		= string(record->C_ST_ID);
							v.c_l_name		= string(record->C_L_NAME);
							v.c_f_name		= string(record->C_F_NAME);
							v.c_m_name		= string(record->C_M_NAME);
							v.c_gndr		= record->C_GNDR;
							v.c_tier		= record->C_TIER;
							v.c_dob			= CDateTimeToTimeT(record->C_DOB);
							v.c_ad_id		= record->C_AD_ID;
							v.c_ctry_1		= string(record->C_CTRY_1);
							v.c_area_1		= string(record->C_AREA_1);
							v.c_local_1		= string(record->C_LOCAL_1);
							v.c_ext_1		= string(record->C_EXT_1);
							v.c_ctry_2		= string(record->C_CTRY_2);
							v.c_area_2		= string(record->C_AREA_2);
							v.c_local_2		= string(record->C_LOCAL_2);
							v.c_ext_2		= string(record->C_EXT_2);
							v.c_ctry_3		= string(record->C_CTRY_3);
							v.c_area_3		= string(record->C_AREA_3);
							v.c_local_3		= string(record->C_LOCAL_3);
							v.c_ext_3		= string(record->C_EXT_3);
							v.c_email_1		= string(record->C_EMAIL_1);
							v.c_email_2		= string(record->C_EMAIL_2);

							c_tax_id_index::key k_idx_tax_id;
							c_tax_id_index::value v_idx_tax_id;

							k_idx_tax_id.c_id			= record->C_ID;
							k_idx_tax_id.c_tax_id		= string(record->C_TAX_ID);

							void *txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);
							tbl_customers(1)->insert(txn, Encode(k), Encode(obj_buf, v));
							tbl_c_tax_id_index(1)->insert(txn, Encode(k_idx_tax_id), Encode(obj_buf, v_idx_tax_id));
							db->commit_txn(txn);
						}
					}
					pGenerateAndLoad->ReleaseCustomer();
					customerBuffer.release();
				} catch (abstract_db::abstract_abort_exception &ex) {
					// shouldn't abort on loading!
					ALWAYS_ASSERT(false);
				}
			}

	private:
		ssize_t partition_id;
};

class tpce_ca_and_ap_loader : public bench_loader, public tpce_worker_mixin {
	public:
		tpce_ca_and_ap_loader(unsigned long seed,
				abstract_db *db,
				const map<string, abstract_ordered_index *> &open_tables,
				const map<string, vector<abstract_ordered_index *>> &partitions,
				ssize_t partition_id)
			: bench_loader(seed, db, open_tables),
			tpce_worker_mixin(partitions),
			partition_id(partition_id)
	{
		ALWAYS_ASSERT(partition_id == -1 ||
				(partition_id >= 1 &&
				 static_cast<size_t>(partition_id) <= NumPartitions()));
	}

	protected:

		virtual void
			load()
			{
				try{
					pGenerateAndLoad->InitCustomerAccountAndAccountPermission();
					while(customerAccountBuffer.hasMoreToRead()){
						customerAccountBuffer.reset();
						accountPermissionBuffer.reset();
						bool hasNext;
						do {
							hasNext= pGenerateAndLoad->hasNextCustomerAccount();
							PCUSTOMER_ACCOUNT_ROW record = pGenerateAndLoad->getCustomerAccountRow();
							customerAccountBuffer.append(record);
							int perms = pGenerateAndLoad->PermissionsPerCustomer();
							for(int i=0; i<perms; i++) {
								PACCOUNT_PERMISSION_ROW row =
									pGenerateAndLoad->getAccountPermissionRow(i);
								accountPermissionBuffer.append(row);
							}
						} while((hasNext && customerAccountBuffer.hasSpace()));
						customerAccountBuffer.setMoreToRead(hasNext);

						int rows=customerAccountBuffer.getSize();
						for(int i=0; i<rows; i++){
							PCUSTOMER_ACCOUNT_ROW record = customerAccountBuffer.get(i);
							customer_account::key k;
							customer_account::value v;
							ca_id_index::key k_idx1;
							ca_id_index::value v_idx1;
							string obj_buf;

							k.ca_id 		= record->CA_ID;
							v.ca_b_id 	= record->CA_B_ID;
							v.ca_c_id 	= record->CA_C_ID;
							v.ca_name 	= string(record->CA_NAME);
							v.ca_tax_st 	= record->CA_TAX_ST;
							v.ca_bal 		= record->CA_BAL;

							k_idx1.ca_id = record->CA_ID;
							k_idx1.ca_c_id = record->CA_C_ID;
							v_idx1.ca_bal	= record->CA_BAL;

							void *txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);
							tbl_customer_account(1)->insert(txn, Encode(k), Encode(obj_buf, v));
							tbl_ca_id_index(1)->insert(txn, Encode(k_idx1), Encode(obj_buf, v_idx1));
							db->commit_txn(txn);
						}
						rows=customerAccountBuffer.getSize();
						for(int i=0; i<rows; i++){
							PACCOUNT_PERMISSION_ROW record = accountPermissionBuffer.get(i);
							account_permission::key k;
							account_permission::value v;
							string obj_buf;

							k.ap_ca_id 	= record->AP_CA_ID;
							k.ap_tax_id 	= string(record->AP_TAX_ID);
							v.ap_acl		= string(record->AP_ACL);
							v.ap_l_name	= string(record->AP_L_NAME);
							v.ap_f_name	= string(record->AP_F_NAME);

							void *txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);
							tbl_account_permission(1)->insert(txn, Encode(k), Encode(obj_buf, v));
							db->commit_txn(txn);
						}
					}
					pGenerateAndLoad->ReleaseCustomerAccountAndAccountPermission();
					customerAccountBuffer.release();
					accountPermissionBuffer.release();
				} catch (abstract_db::abstract_abort_exception &ex) {
					// shouldn't abort on loading!
					ALWAYS_ASSERT(false);
				}
			}

	private:
		ssize_t partition_id;
};

class tpce_customer_taxrate_loader : public bench_loader, public tpce_worker_mixin {
	public:
		tpce_customer_taxrate_loader(unsigned long seed,
				abstract_db *db,
				const map<string, abstract_ordered_index *> &open_tables,
				const map<string, vector<abstract_ordered_index *>> &partitions,
				ssize_t partition_id)
			: bench_loader(seed, db, open_tables),
			tpce_worker_mixin(partitions),
			partition_id(partition_id)
	{
		ALWAYS_ASSERT(partition_id == -1 ||
				(partition_id >= 1 &&
				 static_cast<size_t>(partition_id) <= NumPartitions()));
	}

	protected:

		virtual void
			load()
			{
				try{
					pGenerateAndLoad->InitCustomerTaxrate();
					while(customerTaxrateBuffer.hasMoreToRead()){
						customerTaxrateBuffer.reset();
						bool hasNext;
						int taxrates=pGenerateAndLoad->getTaxratesCount();
						do {
							hasNext= pGenerateAndLoad->hasNextCustomerTaxrate();
							for(int i=0; i<taxrates; i++) {
								PCUSTOMER_TAXRATE_ROW record = pGenerateAndLoad->getCustomerTaxrateRow(i);
								customerTaxrateBuffer.append(record);
							}
						} while((hasNext && customerTaxrateBuffer.hasSpace()));
						customerTaxrateBuffer.setMoreToRead(hasNext);

						int rows=customerTaxrateBuffer.getSize();
						for(int i=0; i<rows; i++){
							PCUSTOMER_TAXRATE_ROW record = customerTaxrateBuffer.get(i);
							customer_taxrate::key k;
							customer_taxrate::value v;
							string obj_buf;

							k.cx_c_id			= record->CX_C_ID;
							k.cx_tx_id		= string(record->CX_TX_ID);
							v.dummy 			= true;

							void *txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);
							tbl_customer_taxrate(1)->insert(txn, Encode(k), Encode(obj_buf, v));
							db->commit_txn(txn);
						}
					}
					pGenerateAndLoad->ReleaseCustomerTaxrate();
					customerTaxrateBuffer.release();
				} catch (abstract_db::abstract_abort_exception &ex) {
					// shouldn't abort on loading!
					ALWAYS_ASSERT(false);
				}
			}

	private:
		ssize_t partition_id;
};

class tpce_wl_and_wi_loader : public bench_loader, public tpce_worker_mixin {
	public:
		tpce_wl_and_wi_loader(unsigned long seed,
				abstract_db *db,
				const map<string, abstract_ordered_index *> &open_tables,
				const map<string, vector<abstract_ordered_index *>> &partitions,
				ssize_t partition_id)
			: bench_loader(seed, db, open_tables),
			tpce_worker_mixin(partitions),
			partition_id(partition_id)
	{
		ALWAYS_ASSERT(partition_id == -1 ||
				(partition_id >= 1 &&
				 static_cast<size_t>(partition_id) <= NumPartitions()));
	}

	protected:

		virtual void
			load()
			{
				try{
					pGenerateAndLoad->InitWatchListAndWatchItem();
					while(watchListBuffer.hasMoreToRead()){
						watchItemBuffer.reset();
						watchListBuffer.reset();
						bool hasNext;
						do {
							hasNext= pGenerateAndLoad->hasNextWatchList();
							PWATCH_LIST_ROW record = pGenerateAndLoad->getWatchListRow();
							watchListBuffer.append(record);
							int items = pGenerateAndLoad->ItemsPerWatchList();
							for(int i=0; i<items; i++) {
								PWATCH_ITEM_ROW row = pGenerateAndLoad->getWatchItemRow(i);
								watchItemBuffer.append(row);
							}
						} while(hasNext && watchListBuffer.hasSpace());
						watchListBuffer.setMoreToRead(hasNext);

						int rows=watchListBuffer.getSize();
						for(int i=0; i<rows; i++){
							PWATCH_LIST_ROW record = watchListBuffer.get(i);
							watch_list::key k;
							watch_list::value v;
							string obj_buf;

							k.wl_c_id	= record->WL_C_ID;
							k.wl_id = record->WL_ID;
							v.dummy = true;

							void *txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);
							tbl_watch_list(1)->insert(txn, Encode(k), Encode(obj_buf, v));
							db->commit_txn(txn);
						}
						rows=watchItemBuffer.getSize();
						for(int i=0; i<rows; i++){
							PWATCH_ITEM_ROW record = watchItemBuffer.get(i);
							watch_item::key k;
							watch_item::value v;
							string obj_buf;

							k.wi_wl_id	= record->WI_WL_ID;
							v.wi_s_symb   = record->WI_S_SYMB;

							void *txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);
							tbl_watch_item(1)->insert(txn, Encode(k), Encode(obj_buf, v));
							db->commit_txn(txn);
						}
					}
					pGenerateAndLoad->ReleaseWatchListAndWatchItem();
					watchItemBuffer.release();
					watchListBuffer.release();
				} catch (abstract_db::abstract_abort_exception &ex) {
					// shouldn't abort on loading!
					ALWAYS_ASSERT(false);
				}
			}

	private:
		ssize_t partition_id;
};

class tpce_company_loader : public bench_loader, public tpce_worker_mixin {
	public:
		tpce_company_loader(unsigned long seed,
				abstract_db *db,
				const map<string, abstract_ordered_index *> &open_tables,
				const map<string, vector<abstract_ordered_index *>> &partitions,
				ssize_t partition_id)
			: bench_loader(seed, db, open_tables),
			tpce_worker_mixin(partitions),
			partition_id(partition_id)
	{
		ALWAYS_ASSERT(partition_id == -1 ||
				(partition_id >= 1 &&
				 static_cast<size_t>(partition_id) <= NumPartitions()));
	}

	protected:

		virtual void
			load()
			{
				try{
					pGenerateAndLoad->InitCompany();
					while(companyBuffer.hasMoreToRead()){
						companyBuffer.reset();
						bool hasNext;
						do {
							hasNext= pGenerateAndLoad->hasNextCompany();
							PCOMPANY_ROW record = pGenerateAndLoad->getCompanyRow();
							companyBuffer.append(record);
						} while((hasNext && companyBuffer.hasSpace()));
						companyBuffer.setMoreToRead(hasNext);

						int rows=companyBuffer.getSize();
						for(int i=0; i<rows; i++){
							PCOMPANY_ROW record = companyBuffer.get(i);
							company::key k;
							company::value v;
							co_name_index::key k_idx1;
							co_name_index::value v_idx1;
							string obj_buf;

							k.co_id			= record->CO_ID;
							v.co_st_id		= string(record->CO_ST_ID);
							v.co_name		= string(record->CO_NAME);
							v.co_in_id		= string(record->CO_IN_ID);
							v.co_sp_rate	= string(record->CO_SP_RATE);
							v.co_ceo		= string(record->CO_CEO);
							v.co_ad_id		= record->CO_AD_ID;
							v.co_open_date	= CDateTimeToTimeT(record->CO_OPEN_DATE);

							k_idx1.co_name	= string(record->CO_NAME);
							k_idx1.co_id	= record->CO_ID;

							void *txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);
							tbl_company(1)->insert(txn, Encode(k), Encode(obj_buf, v));
							tbl_co_name_index(1)->insert(txn, Encode(k_idx1), Encode(obj_buf, v_idx1));
							db->commit_txn(txn);
						}
					}
					pGenerateAndLoad->ReleaseCompany();
					companyBuffer.release();
				} catch (abstract_db::abstract_abort_exception &ex) {
					// shouldn't abort on loading!
					ALWAYS_ASSERT(false);
				}
			}

	private:
		ssize_t partition_id;
};

class tpce_company_competitor_loader : public bench_loader, public tpce_worker_mixin {
	public:
		tpce_company_competitor_loader(unsigned long seed,
				abstract_db *db,
				const map<string, abstract_ordered_index *> &open_tables,
				const map<string, vector<abstract_ordered_index *>> &partitions,
				ssize_t partition_id)
			: bench_loader(seed, db, open_tables),
			tpce_worker_mixin(partitions),
			partition_id(partition_id)
	{
		ALWAYS_ASSERT(partition_id == -1 ||
				(partition_id >= 1 &&
				 static_cast<size_t>(partition_id) <= NumPartitions()));
	}

	protected:

		virtual void
			load()
			{
				try{
					pGenerateAndLoad->InitCompanyCompetitor();
					while(companyCompetitorBuffer.hasMoreToRead()){
						companyCompetitorBuffer.reset();
						bool hasNext;
						do {
							hasNext= pGenerateAndLoad->hasNextCompanyCompetitor();
							PCOMPANY_COMPETITOR_ROW record = pGenerateAndLoad->getCompanyCompetitorRow();
							companyCompetitorBuffer.append(record);
						} while((hasNext && companyCompetitorBuffer.hasSpace()));
						companyCompetitorBuffer.setMoreToRead(hasNext);

						int rows=companyCompetitorBuffer.getSize();
						for(int i=0; i<rows; i++){
							PCOMPANY_COMPETITOR_ROW record = companyCompetitorBuffer.get(i);
							company_competitor::key k;
							company_competitor::value v;
							string obj_buf;

							k.cp_co_id			= record->CP_CO_ID;
							k.cp_comp_co_id		= record->CP_COMP_CO_ID;
							k.cp_in_id			= string(record->CP_IN_ID);
							v.dummy				= true;

							void *txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);
							tbl_company_competitor(1)->insert(txn, Encode(k), Encode(obj_buf, v));
							db->commit_txn(txn);
						}
					}
					pGenerateAndLoad->ReleaseCompanyCompetitor();
					companyCompetitorBuffer.release();
				} catch (abstract_db::abstract_abort_exception &ex) {
					// shouldn't abort on loading!
					ALWAYS_ASSERT(false);
				}
			}

	private:
		ssize_t partition_id;
};

class tpce_daily_market_loader : public bench_loader, public tpce_worker_mixin {
	public:
		tpce_daily_market_loader(unsigned long seed,
				abstract_db *db,
				const map<string, abstract_ordered_index *> &open_tables,
				const map<string, vector<abstract_ordered_index *>> &partitions,
				ssize_t partition_id)
			: bench_loader(seed, db, open_tables),
			tpce_worker_mixin(partitions),
			partition_id(partition_id)
	{
		ALWAYS_ASSERT(partition_id == -1 ||
				(partition_id >= 1 &&
				 static_cast<size_t>(partition_id) <= NumPartitions()));
	}

	protected:

		virtual void
			load()
			{
				try{
					pGenerateAndLoad->InitDailyMarket();
					while(dailyMarketBuffer.hasMoreToRead()){
						dailyMarketBuffer.reset();
						bool hasNext;
						do {
							hasNext= pGenerateAndLoad->hasNextDailyMarket();
							PDAILY_MARKET_ROW record = pGenerateAndLoad->getDailyMarketRow();
							dailyMarketBuffer.append(record);
						} while((hasNext && dailyMarketBuffer.hasSpace()));
						dailyMarketBuffer.setMoreToRead(hasNext);

						int rows=dailyMarketBuffer.getSize();
						for(int i=0; i<rows; i++){
							PDAILY_MARKET_ROW record = dailyMarketBuffer.get(i);
							daily_market::key k;
							daily_market::value v;
							string obj_buf;

							k.dm_s_symb			= string(record->DM_S_SYMB);
							k.dm_date				= CDateTimeToTimeT(record->DM_DATE);
							v.dm_close			= record->DM_CLOSE;
							v.dm_high				= record->DM_HIGH;
							v.dm_low				= record->DM_HIGH;
							v.dm_vol				= record->DM_VOL;

							void *txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);
							tbl_daily_market(1)->insert(txn, Encode(k), Encode(obj_buf, v));
							db->commit_txn(txn);
						}
					}
					pGenerateAndLoad->ReleaseDailyMarket();
					dailyMarketBuffer.release();
				} catch (abstract_db::abstract_abort_exception &ex) {
					// shouldn't abort on loading!
					ALWAYS_ASSERT(false);
				}
			}

	private:
		ssize_t partition_id;
};

class tpce_financial_loader : public bench_loader, public tpce_worker_mixin {
	public:
		tpce_financial_loader(unsigned long seed,
				abstract_db *db,
				const map<string, abstract_ordered_index *> &open_tables,
				const map<string, vector<abstract_ordered_index *>> &partitions,
				ssize_t partition_id)
			: bench_loader(seed, db, open_tables),
			tpce_worker_mixin(partitions),
			partition_id(partition_id)
	{
		ALWAYS_ASSERT(partition_id == -1 ||
				(partition_id >= 1 &&
				 static_cast<size_t>(partition_id) <= NumPartitions()));
	}

	protected:

		virtual void
			load()
			{
				try{
					pGenerateAndLoad->InitFinancial();
					while(financialBuffer.hasMoreToRead()){
						financialBuffer.reset();
						bool hasNext;
						do {
							hasNext= pGenerateAndLoad->hasNextFinancial();
							PFINANCIAL_ROW record = pGenerateAndLoad->getFinancialRow();
							financialBuffer.append(record);
						} while((hasNext && financialBuffer.hasSpace()));
						financialBuffer.setMoreToRead(hasNext);

						int rows=financialBuffer.getSize();
						for(int i=0; i<rows; i++){
							PFINANCIAL_ROW record = financialBuffer.get(i);
							financial::key k;
							financial::value v;
							string obj_buf;

							k.fi_co_id	= record->FI_CO_ID;
							k.fi_year		= record->FI_YEAR;
							k.fi_qtr 		= record->FI_QTR;

							v.fi_qtr_start_date	=	CDateTimeToTimeT(record->FI_QTR_START_DATE);
							v.fi_revenue			=	record->FI_REVENUE;
							v.fi_net_earn			=	record->FI_NET_EARN;
							v.fi_basic_eps		=	record->FI_BASIC_EPS;
							v.fi_dilut_eps		=	record->FI_DILUT_EPS;
							v.fi_margin			=	record->FI_MARGIN;
							v.fi_inventory		=	record->FI_INVENTORY;
							v.fi_assets			=	record->FI_ASSETS;
							v.fi_liability		=	record->FI_LIABILITY;
							v.fi_out_basic		=	record->FI_OUT_BASIC;
							v.fi_out_dilut		=	record->FI_OUT_DILUT;

							void *txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);
							tbl_financial(1)->insert(txn, Encode(k), Encode(obj_buf, v));
							db->commit_txn(txn);
						}
					}
					pGenerateAndLoad->ReleaseFinancial();
					dailyMarketBuffer.release();
				} catch (abstract_db::abstract_abort_exception &ex) {
					// shouldn't abort on loading!
					ALWAYS_ASSERT(false);
				}
			}

	private:
		ssize_t partition_id;
};

class tpce_last_trade_loader : public bench_loader, public tpce_worker_mixin {
	public:
		tpce_last_trade_loader(unsigned long seed,
				abstract_db *db,
				const map<string, abstract_ordered_index *> &open_tables,
				const map<string, vector<abstract_ordered_index *>> &partitions,
				ssize_t partition_id)
			: bench_loader(seed, db, open_tables),
			tpce_worker_mixin(partitions),
			partition_id(partition_id)
	{
		ALWAYS_ASSERT(partition_id == -1 ||
				(partition_id >= 1 &&
				 static_cast<size_t>(partition_id) <= NumPartitions()));
	}

	protected:

		virtual void
			load()
			{
				try{
					pGenerateAndLoad->InitLastTrade();
					while(lastTradeBuffer.hasMoreToRead()){
						lastTradeBuffer.reset();
						bool hasNext;
						do {
							hasNext= pGenerateAndLoad->hasNextLastTrade();
							PLAST_TRADE_ROW record = pGenerateAndLoad->getLastTradeRow();
							lastTradeBuffer.append(record);
						} while((hasNext && lastTradeBuffer.hasSpace()));
						lastTradeBuffer.setMoreToRead(hasNext);

						int rows=lastTradeBuffer.getSize();
						for(int i=0; i<rows; i++){
							PLAST_TRADE_ROW record = lastTradeBuffer.get(i);
							last_trade::key k;
							last_trade::value v;
							string obj_buf;

							k.lt_s_symb = string( record->LT_S_SYMB );
							v.lt_dts 			= CDateTimeToTimeT(record->LT_DTS);
							v.lt_price 		= record->LT_PRICE;
							v.lt_open_price 	= record->LT_OPEN_PRICE;
							v.lt_vol 			= record->LT_VOL;

							void *txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);
							tbl_last_trade(1)->insert(txn, Encode(k), Encode(obj_buf, v));
							db->commit_txn(txn);
						}
					}
					pGenerateAndLoad->ReleaseLastTrade();
					lastTradeBuffer.release();
				} catch (abstract_db::abstract_abort_exception &ex) {
					// shouldn't abort on loading!
					ALWAYS_ASSERT(false);
				}
			}

	private:
		ssize_t partition_id;
};

class tpce_ni_and_nx_loader : public bench_loader, public tpce_worker_mixin {
	public:
		tpce_ni_and_nx_loader(unsigned long seed,
				abstract_db *db,
				const map<string, abstract_ordered_index *> &open_tables,
				const map<string, vector<abstract_ordered_index *>> &partitions,
				ssize_t partition_id)
			: bench_loader(seed, db, open_tables),
			tpce_worker_mixin(partitions),
			partition_id(partition_id)
	{
		ALWAYS_ASSERT(partition_id == -1 ||
				(partition_id >= 1 &&
				 static_cast<size_t>(partition_id) <= NumPartitions()));
	}

	protected:

		virtual void
			load()
			{
				try{
					pGenerateAndLoad->InitNewsItemAndNewsXRef();
					while(newsItemBuffer.hasMoreToRead()){
						newsItemBuffer.reset();
						newsXRefBuffer.reset();
						bool hasNext;
						do {
							hasNext= pGenerateAndLoad->hasNextNewsItemAndNewsXRef();
							PNEWS_ITEM_ROW record1 = pGenerateAndLoad->getNewsItemRow();
							PNEWS_XREF_ROW record2 = pGenerateAndLoad->getNewsXRefRow();
							newsItemBuffer.append(record1);
							newsXRefBuffer.append(record2);
						} while((hasNext && newsItemBuffer.hasSpace()));
						newsItemBuffer.setMoreToRead(hasNext);
						newsXRefBuffer.setMoreToRead(hasNext);

						int rows=newsXRefBuffer.getSize();
						for(int i=0; i<rows; i++){
							PNEWS_XREF_ROW record = newsXRefBuffer.get(i);
							news_xref::key k;
							news_xref::value v;
							string obj_buf;

							k.nx_co_id = record->NX_CO_ID;
							k.nx_ni_id = record->NX_NI_ID;

							v.dummy = true;

							void *txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);
							tbl_news_xref(1)->insert(txn, Encode(k), Encode(obj_buf, v));
							db->commit_txn(txn);
						}
						rows=newsItemBuffer.getSize();
						for(int i=0; i<rows; i++){
							PNEWS_ITEM_ROW record = newsItemBuffer.get(i);
							news_item::key k;
							news_item::value v;
							string obj_buf;

							k.ni_id		= record->NI_ID;

							v.ni_headline	= string(record->NI_HEADLINE);
							v.ni_summary	= string(record->NI_SUMMARY);
							v.ni_item		= string(record->NI_ITEM);
							v.ni_dts		= CDateTimeToTimeT(record->NI_DTS);
							v.ni_source	= string(record->NI_SOURCE);
							v.ni_author	= string(record->NI_AUTHOR);

							void *txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);
							tbl_news_item(1)->insert(txn, Encode(k), Encode(obj_buf, v));
							db->commit_txn(txn);
						}
					}
					pGenerateAndLoad->ReleaseNewsItemAndNewsXRef();
					newsItemBuffer.release();
					newsXRefBuffer.release();
				} catch (abstract_db::abstract_abort_exception &ex) {
					// shouldn't abort on loading!
					ALWAYS_ASSERT(false);
				}
			}

	private:
		ssize_t partition_id;
};

class tpce_security_loader : public bench_loader, public tpce_worker_mixin {
	public:
		tpce_security_loader(unsigned long seed,
				abstract_db *db,
				const map<string, abstract_ordered_index *> &open_tables,
				const map<string, vector<abstract_ordered_index *>> &partitions,
				ssize_t partition_id)
			: bench_loader(seed, db, open_tables),
			tpce_worker_mixin(partitions),
			partition_id(partition_id)
	{
		ALWAYS_ASSERT(partition_id == -1 ||
				(partition_id >= 1 &&
				 static_cast<size_t>(partition_id) <= NumPartitions()));
	}

	protected:

		virtual void
			load()
			{
				try{
					pGenerateAndLoad->InitSecurity();
					while(securityBuffer.hasMoreToRead()){
						securityBuffer.reset();
						bool hasNext;
						do {
							hasNext= pGenerateAndLoad->hasNextSecurity();
							PSECURITY_ROW record = pGenerateAndLoad->getSecurityRow();
							securityBuffer.append(record);
						} while((hasNext && securityBuffer.hasSpace()));
						securityBuffer.setMoreToRead(hasNext);

						int rows=securityBuffer.getSize();
						for(int i=0; i<rows; i++){
							PSECURITY_ROW record = securityBuffer.get(i);
							security::key k;
							security::value v;
							security_index::key k_idx;
							security_index::value v_idx;
							string obj_buf;

							k.s_symb			= string(record->S_SYMB);

							v.s_issue			= string(record->S_ISSUE);
							v.s_st_id			= string(record->S_ST_ID);
							v.s_name			= string(record->S_NAME);
							v.s_ex_id			= string(record->S_EX_ID);
							v.s_co_id			= record->S_CO_ID;
							v.s_num_out		= record->S_NUM_OUT;
							v.s_start_date	= CDateTimeToTimeT(record->S_START_DATE);	
							v.s_exch_date		= CDateTimeToTimeT(record->S_EXCH_DATE);	
							v.s_pe			= record->S_PE;	
							v.s_52wk_high		= record->S_52WK_HIGH;
							v.s_52wk_high_date= CDateTimeToTimeT(record->S_52WK_HIGH_DATE);
							v.s_52wk_low		= record->S_52WK_LOW;
							v.s_52wk_low_date	= CDateTimeToTimeT(record->S_52WK_LOW_DATE);
							v.s_dividend		= record->S_DIVIDEND;
							v.s_yield			= record->S_YIELD;

							k_idx.s_co_id		= record->S_CO_ID;
							k_idx.s_issue		= string(record->S_ISSUE);
							v_idx.s_symb		= string(record->S_SYMB);
							v_idx.s_name		= string(record->S_NAME);
							v_idx.s_ex_id		= string(record->S_EX_ID);

							void *txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);
							tbl_security(1)->insert(txn, Encode(k), Encode(obj_buf, v));
							tbl_security_index(1)->insert(txn, Encode(k_idx), Encode(obj_buf, v_idx));
							db->commit_txn(txn);
						}
					}
					pGenerateAndLoad->ReleaseSecurity();
					securityBuffer.release();
				} catch (abstract_db::abstract_abort_exception &ex) {
					// shouldn't abort on loading!
					ALWAYS_ASSERT(false);
				}
			}

	private:
		ssize_t partition_id;
};

class tpce_growing_loader : public bench_loader, public tpce_worker_mixin {
	public:
		tpce_growing_loader(unsigned long seed,
				abstract_db *db,
				const map<string, abstract_ordered_index *> &open_tables,
				const map<string, vector<abstract_ordered_index *>> &partitions,
				ssize_t partition_id)
			: bench_loader(seed, db, open_tables),
			tpce_worker_mixin(partitions),
			partition_id(partition_id)
	{
		ALWAYS_ASSERT(partition_id == -1 ||
				(partition_id >= 1 &&
				 static_cast<size_t>(partition_id) <= NumPartitions()));
	}

	protected:

		virtual void
			load()
			{
				try{
					pGenerateAndLoad->InitHoldingAndTrade();
					do {
						populate_unit_trade();
						populate_broker();
						populate_holding_summary();
						populate_holding();

						tradeBuffer.newLoadUnit();
						tradeHistoryBuffer.newLoadUnit();
						settlementBuffer.newLoadUnit();
						cashTransactionBuffer.newLoadUnit();
						holdingHistoryBuffer.newLoadUnit();
						brokerBuffer.newLoadUnit();
						holdingSummaryBuffer.newLoadUnit();
						holdingBuffer.newLoadUnit();	
					} while(pGenerateAndLoad->hasNextLoadUnit());

					pGenerateAndLoad->ReleaseHoldingAndTrade();
					tradeBuffer.release();
					tradeHistoryBuffer.release();
					settlementBuffer.release();
					cashTransactionBuffer.release();
					holdingHistoryBuffer.release();
					brokerBuffer.release();
					holdingSummaryBuffer.release();
					holdingBuffer.release();
				} catch (abstract_db::abstract_abort_exception &ex) {
					// shouldn't abort on loading!
					ALWAYS_ASSERT(false);
				}
			}

	private:
		void populate_unit_trade()
		{
			while(tradeBuffer.hasMoreToRead()){
				tradeBuffer.reset();
				tradeHistoryBuffer.reset();
				settlementBuffer.reset();
				cashTransactionBuffer.reset();
				holdingHistoryBuffer.reset();

				bool hasNext;
				do {
					hasNext= pGenerateAndLoad->hasNextTrade();
					PTRADE_ROW row = pGenerateAndLoad->getTradeRow();
					tradeBuffer.append(row);
					int hist = pGenerateAndLoad->getTradeHistoryRowCount();
					for(int i=0; i<hist; i++) {
						PTRADE_HISTORY_ROW record = pGenerateAndLoad->getTradeHistoryRow(i);
						tradeHistoryBuffer.append(record);
					}
					if(pGenerateAndLoad->shouldProcessSettlementRow()) {
						PSETTLEMENT_ROW record = pGenerateAndLoad->getSettlementRow();
						settlementBuffer.append(record);
					}
					if(pGenerateAndLoad->shouldProcessCashTransactionRow()) {
						PCASH_TRANSACTION_ROW record=pGenerateAndLoad->getCashTransactionRow();
						cashTransactionBuffer.append(record);
					}
					hist = pGenerateAndLoad->getHoldingHistoryRowCount();
					for(int i=0; i<hist; i++) {
						PHOLDING_HISTORY_ROW record=pGenerateAndLoad->getHoldingHistoryRow(i);
						holdingHistoryBuffer.append(record);
					}
				} while((hasNext && tradeBuffer.hasSpace()));
				tradeBuffer.setMoreToRead(hasNext);

				int rows=tradeBuffer.getSize();
				for(int i=0; i<rows; i++){
					PTRADE_ROW record = tradeBuffer.get(i);
					trade::key k;
					trade::value v;
					string obj_buf;

					k.t_id 			=	record->T_ID 			;
					lastTradeId		= record->T_ID;
					v.t_dts 			=	CDateTimeToTimeT(record->T_DTS);
					v.t_st_id			=	string(record->T_ST_ID)	;
					v.t_tt_id			=	string(record->T_TT_ID)	;
					v.t_is_cash 		=	record->T_IS_CASH 		;
					v.t_s_symb		=	string(record->T_S_SYMB);
					v.t_qty			=	record->T_QTY			;
					v.t_bid_price		=	record->T_BID_PRICE		;
					v.t_ca_id			=	record->T_CA_ID			;
					v.t_exec_name		=	string(record->T_EXEC_NAME);
					v.t_trade_price	=	record->T_TRADE_PRICE	;
					v.t_chrg			=	record->T_CHRG			;
					v.t_comm			=	record->T_COMM			;
					v.t_tax			=	record->T_TAX			;
					v.t_lifo			=	record->T_LIFO			;

					t_ca_id_index::key k_idx1;
					t_ca_id_index::value v_idx1;
					k_idx1.t_ca_id			=	record->T_CA_ID			;
					k_idx1.t_dts 			=	CDateTimeToTimeT(record->T_DTS);
					k_idx1.t_id 			=	record->T_ID 			;
					v_idx1.t_st_id			=	string(record->T_ST_ID)	;
					v_idx1.t_tt_id			=	string(record->T_TT_ID)	;
					v_idx1.t_is_cash 		=	record->T_IS_CASH 		;
					v_idx1.t_s_symb			=	string(record->T_S_SYMB);
					v_idx1.t_qty			=	record->T_QTY			;
					v_idx1.t_bid_price		=	record->T_BID_PRICE		;
					v_idx1.t_exec_name		=	string(record->T_EXEC_NAME);
					v_idx1.t_trade_price	=	record->T_TRADE_PRICE	;
					v_idx1.t_chrg			=	record->T_CHRG			;

					t_s_symb_index::key k_idx2;
					t_s_symb_index::value v_idx2;
					k_idx2.t_s_symb			=	string(record->T_S_SYMB);
					k_idx2.t_dts 			=	CDateTimeToTimeT(record->T_DTS);
					k_idx2.t_id 			=	record->T_ID 			;
					v_idx2.t_ca_id			=	record->T_CA_ID			;
					v_idx2.t_st_id			=	string(record->T_ST_ID)	;
					v_idx2.t_tt_id			=	string(record->T_TT_ID)	;
					v_idx2.t_is_cash 		=	record->T_IS_CASH 		;
					v_idx2.t_qty			=	record->T_QTY			;
					v_idx2.t_exec_name		=	string(record->T_EXEC_NAME);
					v_idx2.t_trade_price	=	record->T_TRADE_PRICE	;

					void* txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);
					tbl_trade(1)->insert(txn, Encode(k), Encode(obj_buf, v));
					tbl_t_ca_id_index(1)->insert(txn, Encode(k_idx1), Encode(obj_buf, v_idx1));
					tbl_t_s_symb_index(1)->insert(txn, Encode(k_idx2), Encode(obj_buf, v_idx2));
					db->commit_txn(txn);
				}

				rows=tradeHistoryBuffer.getSize();
				for(int i=0; i<rows; i++){
					PTRADE_HISTORY_ROW record = tradeHistoryBuffer.get(i);
					trade_history::key k;
					trade_history::value v;
					string obj_buf;

					k.th_t_id = record->TH_T_ID;
					k.th_dts	= CDateTimeToTimeT( record->TH_DTS );

					v.th_st_id = string( record->TH_ST_ID );

					void* txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);
					tbl_trade_history(1)->insert(txn, Encode(k), Encode(obj_buf, v));
					db->commit_txn(txn);
				}

				rows=settlementBuffer.getSize();
				for(int i=0; i<rows; i++){
					PSETTLEMENT_ROW record = settlementBuffer.get(i);
					settlement::key k;
					settlement::value v;
					string obj_buf;

					k.se_t_id				=	record->SE_T_ID;

					v.se_cash_type		=	string(record->SE_CASH_TYPE);
					v.se_cash_due_date	=	CDateTimeToTimeT(record->SE_CASH_DUE_DATE);
					v.se_amt				=	record->SE_AMT;


					void* txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);
					tbl_settlement(1)->insert(txn, Encode(k), Encode(obj_buf, v));
					db->commit_txn(txn);
				}

				rows=cashTransactionBuffer.getSize();
				for(int i=0; i<rows; i++){
					PCASH_TRANSACTION_ROW record = cashTransactionBuffer.get(i);
					cash_transaction::key k;
					cash_transaction::value v;
					string obj_buf;

					k.ct_t_id			= record->CT_T_ID;

					v.ct_dts			= CDateTimeToTimeT(record->CT_DTS);
					v.ct_amt			= record->CT_AMT;
					v.ct_name			= string(record->CT_NAME);

					void* txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);
					tbl_cash_transaction(1)->insert(txn, Encode(k), Encode(obj_buf, v));
					db->commit_txn(txn);
				}

				rows=holdingHistoryBuffer.getSize();
				for(int i=0; i<rows; i++){
					PHOLDING_HISTORY_ROW record = holdingHistoryBuffer.get(i);
					holding_history::key k;
					holding_history::value v;
					string obj_buf;

					k.hh_t_id				= record->HH_T_ID;
					v.hh_h_t_id			= record->HH_H_T_ID;
					v.hh_before_qty		= record->HH_BEFORE_QTY;
					v.hh_after_qty		= record->HH_AFTER_QTY;

					void* txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);
					tbl_holding_history(1)->insert(txn, Encode(k), Encode(obj_buf, v));
					db->commit_txn(txn);
				}
			}
		}


		void populate_broker()
		{	
			while(brokerBuffer.hasMoreToRead()) {
				brokerBuffer.reset();
				bool hasNext;
				do {
					hasNext= pGenerateAndLoad->hasNextBroker();
					PBROKER_ROW record = pGenerateAndLoad->getBrokerRow();
					brokerBuffer.append(record);
				} while((hasNext && brokerBuffer.hasSpace()));
				brokerBuffer.setMoreToRead(hasNext);
				int rows=brokerBuffer.getSize();
				for(int i=0; i<rows; i++){
					PBROKER_ROW record = brokerBuffer.get(i);
					broker::key k;
					broker::value v;

					b_name_index::key k_idx;
					b_name_index::value v_idx;
					string obj_buf;

					k.b_id				= record->B_ID;
					v.b_st_id				= string(record->B_ST_ID);
					v.b_name				= string(record->B_NAME);
					v.b_num_trades		= record->B_NUM_TRADES;
					v.b_comm_total		= record->B_COMM_TOTAL;

					k_idx.b_name		= string(record->B_NAME );
					k_idx.b_id			= record->B_ID;

					void* txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);
					tbl_broker(1)->insert(txn, Encode(k), Encode(obj_buf, v));
					tbl_b_name_index(1)->insert(txn, Encode(k_idx), Encode(obj_buf, v_idx));
					db->commit_txn(txn);
				}
			}
		}


		void populate_holding_summary()
		{	
			while(holdingSummaryBuffer.hasMoreToRead()){
				holdingSummaryBuffer.reset();
				bool hasNext;
				do {
					hasNext= pGenerateAndLoad->hasNextHoldingSummary();
					PHOLDING_SUMMARY_ROW record = pGenerateAndLoad->getHoldingSummaryRow();
					holdingSummaryBuffer.append(record);
				} while((hasNext && holdingSummaryBuffer.hasSpace()));
				holdingSummaryBuffer.setMoreToRead(hasNext);
				int rows=holdingSummaryBuffer.getSize();
				for(int i=0; i<rows; i++){
					PHOLDING_SUMMARY_ROW record = holdingSummaryBuffer.get(i);
					holding_summary::key k;
					holding_summary::value v;
					string obj_buf;

					k.hs_ca_id		= record->HS_CA_ID;
					k.hs_s_symb		= string(record->HS_S_SYMB);
					v.hs_qty			= record->HS_QTY;

					void* txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);
					tbl_holding_summary(1)->insert(txn, Encode(k), Encode(obj_buf, v));
					db->commit_txn(txn);
				}
			}
		}


		void populate_holding()
		{	
			while(holdingBuffer.hasMoreToRead()){
				holdingBuffer.reset();
				bool hasNext;
				do {
					hasNext= pGenerateAndLoad->hasNextHolding();
					PHOLDING_ROW record = pGenerateAndLoad->getHoldingRow();
					holdingBuffer.append(record);
				} while((hasNext && holdingBuffer.hasSpace()));
				holdingBuffer.setMoreToRead(hasNext);

				int rows=holdingBuffer.getSize();
				for(int i=0; i<rows; i++){
					PHOLDING_ROW record = holdingBuffer.get(i);
					holding::key k;
					holding::value v;
					string obj_buf;

					k.h_ca_id		= record->H_CA_ID;
					k.h_s_symb	= string(record->H_S_SYMB);
					k.h_dts		= CDateTimeToTimeT(record->H_DTS);
					v.h_t_id		= record->H_T_ID;
					v.h_price		= record->H_PRICE;
					v.h_qty		= record->H_QTY;

					void* txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_DEFAULT);
					tbl_holding(1)->insert(txn, Encode(k), Encode(obj_buf, v));
					db->commit_txn(txn);
				}
			}
		}

	private:
		ssize_t partition_id;
};


class tpce_bench_runner : public bench_runner {
	private:

		static bool
			IsTableReadOnly(const char *name)
			{
				// TODO. 
				return false;
			}

		static bool
			IsTableAppendOnly(const char *name)
			{
				// TODO.
				return true;
			}

		static vector<abstract_ordered_index *>
			OpenTablesForTablespace(abstract_db *db, const char *name, size_t expected_size)
			{
				const string s_name(name);
				vector<abstract_ordered_index *> ret(NumPartitions());
				abstract_ordered_index *idx = db->open_index(s_name, expected_size, false );
				for (size_t i = 0; i < NumPartitions(); i++)
					ret[i] = idx;
				return ret;
			}

	public:
		tpce_bench_runner(abstract_db *db)
			: bench_runner(db)
		{

#define OPEN_TABLESPACE_X(x) \
			partitions[#x] = OpenTablesForTablespace(db, #x, sizeof(x));

			TPCE_TABLE_LIST(OPEN_TABLESPACE_X);

#undef OPEN_TABLESPACE_X

			for (auto &t : partitions) {
				auto v = unique_filter(t.second);
				for (size_t i = 0; i < v.size(); i++)
					open_tables[t.first + "_" + to_string(i)] = v[i];
			}
		}

	protected:
		virtual vector<bench_loader *>
			make_loaders()
			{
				vector<bench_loader *> ret;

				// FIXME. what seed values should be passed?
				ret.push_back(new tpce_charge_loader(235443, db, open_tables, partitions, -1));
				ret.push_back(new tpce_commission_rate_loader(89785943, db, open_tables, partitions, -1));
				ret.push_back(new tpce_exchange_loader(129856349, db, open_tables, partitions, -1));
				ret.push_back(new tpce_industry_loader(923587856425, db, open_tables, partitions, -1));
				ret.push_back(new tpce_sector_loader(2343352, db, open_tables, partitions, -1));
				ret.push_back(new tpce_status_type_loader(235443, db, open_tables, partitions, -1));
				ret.push_back(new tpce_tax_rate_loader(89785943, db, open_tables, partitions, -1));
				ret.push_back(new tpce_trade_type_loader(129856349, db, open_tables, partitions, -1));
				ret.push_back(new tpce_zip_code_loader(923587856425, db, open_tables, partitions, -1));
				ret.push_back(new tpce_address_loader(923587856425, db, open_tables, partitions, -1));
				ret.push_back(new tpce_customer_loader(923587856425, db, open_tables, partitions, -1));
				ret.push_back(new tpce_ca_and_ap_loader(923587856425, db, open_tables, partitions, -1));
				ret.push_back(new tpce_customer_taxrate_loader(923587856425, db, open_tables, partitions, -1));
				ret.push_back(new tpce_wl_and_wi_loader(923587856425, db, open_tables, partitions, -1));
				ret.push_back(new tpce_company_loader(923587856425, db, open_tables, partitions, -1));
				ret.push_back(new tpce_company_competitor_loader(923587856425, db, open_tables, partitions, -1));
				ret.push_back(new tpce_daily_market_loader(923587856425, db, open_tables, partitions, -1));
				ret.push_back(new tpce_financial_loader(923587856425, db, open_tables, partitions, -1));
				ret.push_back(new tpce_last_trade_loader(923587856425, db, open_tables, partitions, -1));
				ret.push_back(new tpce_ni_and_nx_loader(923587856425, db, open_tables, partitions, -1));
				ret.push_back(new tpce_security_loader(923587856425, db, open_tables, partitions, -1));
				ret.push_back(new tpce_growing_loader(923587856425, db, open_tables, partitions, -1));

				return ret;
			}

		virtual vector<bench_worker *>
			make_workers()
			{
				const unsigned alignment = coreid::num_cpus_online();
				const int blockstart =
					coreid::allocate_contiguous_aligned_block(nthreads, alignment);
				ALWAYS_ASSERT(blockstart >= 0);
				ALWAYS_ASSERT((blockstart % alignment) == 0);
				fast_random r(23984543);
				vector<bench_worker *> ret;
				static bool const NO_PIN_WH = false;
				if (NO_PIN_WH) {
					for (size_t i = 0; i < nthreads; i++)
						ret.push_back(
								new tpce_worker(
									blockstart + i,
									r.next(), db, open_tables, partitions,
									&barrier_a, &barrier_b,
									1, NumPartitions() + 1));
				}
				else if (NumPartitions() <= nthreads) {
					for (size_t i = 0; i < nthreads; i++)
						ret.push_back(
								new tpce_worker(
									blockstart + i,
									r.next(), db, open_tables, partitions,
									&barrier_a, &barrier_b,
									(i % NumPartitions()) + 1, (i % NumPartitions()) + 2));
				} else {
					auto N = NumPartitions();
					auto T = nthreads;
					// try this in python: [i*N//T for i in range(T+1)]
					for (size_t i = 0; i < nthreads; i++) {
						const unsigned wstart = i*N/T;
						const unsigned wend   = (i + 1)*N/T;
						ret.push_back(
								new tpce_worker(
									blockstart + i,
									r.next(), db, open_tables, partitions,
									&barrier_a, &barrier_b, wstart+1, wend+1));
					}
				}
				return ret;
			}

	private:
		map<string, vector<abstract_ordered_index *>> partitions;
};


// Benchmark entry function
void tpce_do_test(abstract_db *db, int argc, char **argv)
{
	int customers = 0;
	int working_days = 0;
	int scaling_factor_tpce = scale_factor;
	char* egen_dir = NULL;
	char sfe_str[8], wd_str[8], cust_str[8];
	memset(sfe_str,0,8);
	memset(wd_str,0,8);
	memset(cust_str,0,8);
	sprintf(sfe_str, "%d",scaling_factor_tpce);

	// parse options
	optind = 1;
	while (1) {
		static struct option long_options[] =
		{
			{"workload-mix"                     , required_argument , 0                                     , 'w'} ,
			{"egen-dir"                         , required_argument , 0                                     , 'e'} ,
			{"customers"                        , required_argument , 0                                     , 'c'} ,
			{"working-days"                     , required_argument , 0                                     , 'd'} ,
			{0, 0, 0, 0}
		};
		int option_index = 0;
		int c = getopt_long(argc, argv, "r:", long_options, &option_index);
		if (c == -1)
			break;
		switch (c) {
			case 0:
				if (long_options[option_index].flag != 0)
					break;
				abort();
				break;

			case 'c':
				strncpy( cust_str, optarg, 8 );
				customers = atoi(cust_str );
				break;
			case 'd':
				strncpy( wd_str, optarg, 8 );
				working_days = atoi(wd_str );
				break;
			case 'e':
				egen_dir = optarg;
				break;
			case 'w':
				{
					const vector<string> toks = split(optarg, ',');
					ALWAYS_ASSERT(toks.size() == ARRAY_NELEMS(g_txn_workload_mix));
					unsigned s = 0;
					for (size_t i = 0; i < toks.size(); i++) {
						unsigned p = strtoul(toks[i].c_str(), nullptr, 10);
						ALWAYS_ASSERT(p >= 0 && p <= 100);
						s += p;
						g_txn_workload_mix[i] = p;
					}
					ALWAYS_ASSERT(s == 100);
				}
				break;

			case '?':
				/* getopt_long already printed an error message. */
				exit(1);

			default:
				abort();
		}
	}


	const char * params[] = {"to_skip", "-i", egen_dir, "-l", "NULL", "-f", sfe_str, "-w", wd_str, "-c", cust_str, "-t", cust_str }; 
	egen_init(13,  (char **)params);      

	//Initialize Client Transaction Input generator
	m_TxnInputGenerator = transactions_input_init(customers, scaling_factor_tpce , working_days);

	unsigned int seed = AutoRand();
	setRNGSeeds(m_TxnInputGenerator, seed);

	m_CDM = data_maintenance_init(customers, scaling_factor_tpce, working_days);

	//Initialize Market side
	MarketFeedInputBuffer = new MFBuffer();
	TradeResultInputBuffer = new TRBuffer();

	meesut = new CMEESUT();
	meesut->setMFQueue(MarketFeedInputBuffer);
	meesut->setTRQueue(TradeResultInputBuffer);
	mee = market_init( working_days*8, meesut, AutoRand()); 		

	if (verbose) {
		cerr << "tpce settings:" << endl;
		cerr << "  workload_mix                 : " <<
			format_list(g_txn_workload_mix,
					g_txn_workload_mix + ARRAY_NELEMS(g_txn_workload_mix)) << endl;
		cerr << "  scale factor                 :" << " " << sfe_str << endl;
		cerr << "  working days                 :" << " " << wd_str << endl;
		cerr << "  customers                    :" << " " << cust_str << endl;
	}

	tpce_bench_runner r(db);
	r.run();
}
