/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-kits -- Benchmark implementations for Shore-MT
   
                       Copyright (c) 2007-2009
      Data Intensive Applications and Systems Labaratory (DIAS)
               Ecole Polytechnique Federale de Lausanne
   
                         All Rights Reserved.
   
   Permission to use, copy, modify and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies of
   the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.
   
   This code is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
   DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
   RESULTING FROM THE USE OF THIS SOFTWARE.
*/

/** @file:   shore_tpce_egen.h
 *
 *  @brief:  Structures for buffering egen tuples
 *
 *  @author: Djordje Jevdjic, Apr 2010
 */

#ifndef __SHORE_TPCE_EGEN_H
#define __SHORE_TPCE_EGEN_H

#include "./EGenLoader_stdafx.h"
#include "./EGenStandardTypes.h"
#include "./EGenTables_stdafx.h"
#include "./Table_Defs.h"

namespace TPCE
{
template <typename T>
class EgenTupleContainer 
{
  int size;
  int capacity;
  long cnt;
  T* buffer;
  bool moreToRead;

  
public:
    int getSize() {return size;}
    void setSize(int k) {size = k;}
    int getCapacity() {return capacity;}
    int getRowSize() {return sizeof(T);}
    long getCnt() {return cnt;}
   
    EgenTupleContainer (int cap): size(0), capacity(cap){
      buffer = new T [capacity];
      moreToRead=true;
      cnt = 0;
    }
    T* get(int i){cnt++; return &buffer[i]; }
    void append(T* row) {memcpy(&buffer[size++],row, sizeof(T)); }
    bool hasSpace(){return (size<capacity-2);}
    bool hasMoreToRead() {return moreToRead;}
    void setMoreToRead(bool x) {moreToRead = x;}    
    void reset() {size=0;}
    void release() {/*delete [] buffer;*/}
    void newLoadUnit(){size=0; moreToRead=true;} 
};

class AccountPermissionBuffer: public EgenTupleContainer<ACCOUNT_PERMISSION_ROW>{
  public:
    AccountPermissionBuffer(int c): EgenTupleContainer<ACCOUNT_PERMISSION_ROW>(c){}
};
class CustomerBuffer: public EgenTupleContainer<CUSTOMER_ROW>{
  public:
    CustomerBuffer(int c): EgenTupleContainer<CUSTOMER_ROW>(c){}
};
class CustomerAccountBuffer: public EgenTupleContainer<CUSTOMER_ACCOUNT_ROW>{
  public:
    CustomerAccountBuffer(int c): EgenTupleContainer<CUSTOMER_ACCOUNT_ROW>(c){}
};
class CustomerTaxrateBuffer: public EgenTupleContainer<CUSTOMER_TAXRATE_ROW>{
  public:
    CustomerTaxrateBuffer(int c): EgenTupleContainer<CUSTOMER_TAXRATE_ROW>(c){}
};
class HoldingBuffer: public EgenTupleContainer<HOLDING_ROW>{
  public:
    HoldingBuffer(int c): EgenTupleContainer<HOLDING_ROW>(c){}
};
class HoldingHistoryBuffer: public EgenTupleContainer<HOLDING_HISTORY_ROW>{
  public:
    HoldingHistoryBuffer(int c): EgenTupleContainer<HOLDING_HISTORY_ROW>(c){}
};
class HoldingSummaryBuffer: public EgenTupleContainer<HOLDING_SUMMARY_ROW>{
  public:
    HoldingSummaryBuffer(int c): EgenTupleContainer<HOLDING_SUMMARY_ROW>(c){}
};
class WatchItemBuffer: public EgenTupleContainer<WATCH_ITEM_ROW>{
  public:
    WatchItemBuffer(int c): EgenTupleContainer<WATCH_ITEM_ROW>(c){}
};
class WatchListBuffer: public EgenTupleContainer<WATCH_LIST_ROW>{
  public:
    WatchListBuffer(int c): EgenTupleContainer<WATCH_LIST_ROW>(c){}
};

class BrokerBuffer: public EgenTupleContainer<BROKER_ROW>{
  public:
    BrokerBuffer(int c): EgenTupleContainer<BROKER_ROW>(c){}
};
class CashTransactionBuffer: public EgenTupleContainer<CASH_TRANSACTION_ROW>{
  public:
    CashTransactionBuffer(int c): EgenTupleContainer<CASH_TRANSACTION_ROW>(c){}
};
class ChargeBuffer: public EgenTupleContainer<CHARGE_ROW>{
  public:
    ChargeBuffer(int c): EgenTupleContainer<CHARGE_ROW>(c){}
};
class CommissionRateBuffer: public EgenTupleContainer<COMMISSION_RATE_ROW>{
  public:
    CommissionRateBuffer(int c): EgenTupleContainer<COMMISSION_RATE_ROW>(c){}
};
class SettlementBuffer: public EgenTupleContainer<SETTLEMENT_ROW>{
  public:
    SettlementBuffer(int c): EgenTupleContainer<SETTLEMENT_ROW>(c){}
};
class TradeBuffer: public EgenTupleContainer<TRADE_ROW>{
  public:
    TradeBuffer(int c): EgenTupleContainer<TRADE_ROW>(c){}
};
class TradeHistoryBuffer: public EgenTupleContainer<TRADE_HISTORY_ROW>{
  public:
    TradeHistoryBuffer(int c): EgenTupleContainer<TRADE_HISTORY_ROW>(c){}
};
class TradeRequestBuffer: public EgenTupleContainer<TRADE_REQUEST_ROW>{
  public:
    TradeRequestBuffer(int c): EgenTupleContainer<TRADE_REQUEST_ROW>(c){}
};
class TradeTypeBuffer: public EgenTupleContainer<TRADE_TYPE_ROW>{
  public:
    TradeTypeBuffer(int c): EgenTupleContainer<TRADE_TYPE_ROW>(c){}
};
class CompanyBuffer: public EgenTupleContainer<COMPANY_ROW>{
  public:
    CompanyBuffer(int c): EgenTupleContainer<COMPANY_ROW>(c){}
};
class CompanyCompetitorBuffer: public EgenTupleContainer<COMPANY_COMPETITOR_ROW>{
  public:
    CompanyCompetitorBuffer(int c): EgenTupleContainer<COMPANY_COMPETITOR_ROW>(c){}
};
class DailyMarketBuffer: public EgenTupleContainer<DAILY_MARKET_ROW>{
  public:
    DailyMarketBuffer(int c): EgenTupleContainer<DAILY_MARKET_ROW>(c){}
};
class ExchangeBuffer: public EgenTupleContainer<EXCHANGE_ROW>{
  public:
    ExchangeBuffer(int c): EgenTupleContainer<EXCHANGE_ROW>(c){}
};
class FinancialBuffer: public EgenTupleContainer<FINANCIAL_ROW>{
  public:
    FinancialBuffer(int c): EgenTupleContainer<FINANCIAL_ROW>(c){}
};
class IndustryBuffer: public EgenTupleContainer<INDUSTRY_ROW>{
  public:
    IndustryBuffer(int c): EgenTupleContainer<INDUSTRY_ROW>(c){}
};
class LastTradeBuffer: public EgenTupleContainer<LAST_TRADE_ROW>{
  public:
    LastTradeBuffer(int c): EgenTupleContainer<LAST_TRADE_ROW>(c){}
};

class NewsItemBuffer: public EgenTupleContainer<NEWS_ITEM_ROW>{
  public:
    NewsItemBuffer(int c): EgenTupleContainer<NEWS_ITEM_ROW>(c){}
};

class NewsXRefBuffer: public EgenTupleContainer<NEWS_XREF_ROW>{
  public:
    NewsXRefBuffer(int c): EgenTupleContainer<NEWS_XREF_ROW>(c){}
};

class SectorBuffer: public EgenTupleContainer<SECTOR_ROW>{
  public:
    SectorBuffer(int c): EgenTupleContainer<SECTOR_ROW>(c){}
};

class SecurityBuffer: public EgenTupleContainer<SECURITY_ROW>{
  public:
    SecurityBuffer(int c): EgenTupleContainer<SECURITY_ROW>(c){}
};


class AddressBuffer: public EgenTupleContainer<ADDRESS_ROW>{
  public:
    AddressBuffer(int c): EgenTupleContainer<ADDRESS_ROW>(c){}
};
class StatusTypeBuffer: public EgenTupleContainer<STATUS_TYPE_ROW>{
  public:
    StatusTypeBuffer(int c): EgenTupleContainer<STATUS_TYPE_ROW>(c){}
};

class TaxrateBuffer: public EgenTupleContainer<TAXRATE_ROW>{
  public:
    TaxrateBuffer(int c): EgenTupleContainer<TAXRATE_ROW>(c){}
};

class ZipCodeBuffer: public EgenTupleContainer<ZIP_CODE_ROW>{
  public:
    ZipCodeBuffer(int c): EgenTupleContainer<ZIP_CODE_ROW>(c){}
};
}
#endif


