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
 * - Sergey Vasilevskiy
 */

/*
*   This file contains a class that acts as a client to the table
*   generation classes (EGenTables) and to the loader classes (EGenBaseLoader).
*   It provides routines for generating and loading the table data or its subset.
*/

#ifndef EGEN_GENERATE_AND_LOAD_H
#define EGEN_GENERATE_AND_LOAD_H

#include "EGenStandardTypes.h"
#include "EGenGenerateAndLoadBaseOutput.h"
#include "MiscConsts.h"
#include "InputFlatFilesStructure.h"
#include "BaseLoaderFactory.h"
#include "BaseLogger.h"

namespace TPCE
{



class CGenerateAndLoad
{
    // Structure containing references to input files loaded into memory
    CInputFiles                 m_inputFiles;
    // Ordinal position (1-based) of the first customer in the sequence
    TIdent                      m_iStartFromCustomer;
    // The number of customers to generate from the starting position
    TIdent                      m_iCustomerCount;
    // Total number of customers in the database
    TIdent                      m_iTotalCustomers;
    // Number of customers in one load unit for generating initial trades
    UINT                        m_iLoadUnitSize;
    // Number of customers per 1 tpsE
    UINT                        m_iScaleFactor;
    // Time period for which to generate initial trades
    UINT                        m_iHoursOfInitialTrades;
    // External loader factory to create table loaders
    CBaseLoaderFactory*         m_pLoaderFactory;
    // External class used to output load progress
    CGenerateAndLoadBaseOutput* m_pOutput;
    // Input flat file directory for tables loaded via flat files
    char                        m_szInDir[iMaxPath];
    // Logger instance
    CBaseLogger*                m_pLogger;
    // Parameter instance
    CLoaderSettings             m_LoaderSettings;
    // Whether to use cache when generating initial population
    bool                        m_bCacheEnabled;


   	//structures for tables

	CAddressTable*				addressTable;
	CChargeTable*				chargeTable;
	CCommissionRateTable*			commissionRateTable;
	CCompanyTable*				companyTable;
	CCompanyCompetitorTable*		companyCompetitorTable;
	CCustomerTable*				customerTable;
	CCustomerAccountsAndPermissionsTable*   customerAccountsAndPermissionsTable;
	CCustomerTaxratesTable*			customerTaxratesTable;
	CDailyMarketTable*			dailyMarketTable;
	CExchangeTable*				exchangeTable;
	CFinancialTable*			financialTable;
	
	CIndustryTable*				industryTable;
	CLastTradeTable*			lastTradeTable;
	CNewsItemAndXRefTable*			newsItemAndXRefTable;
	CSectorTable*				sectorTable;
	CSecurityTable*				securityTable;
	CStatusTypeTable*			statusTypeTable;
	CTaxrateTable*				taxrateTable;
	CTradeTypeTable*			tradeTypeTable;
	CWatchListsAndItemsTable*		watchListsAndItemsTable;
	CZipCodeTable*				zipCodeTable;

	//holding & trade related tables 
	CTradeGen*				pTradeGen;	



public:
    /*
    *   Constructor.
    *
    *  PARAMETERS:
    *           IN  inputFiles          - in-memory representation of input flat files
    *           IN  iCustomerCount      - number of customers to build (for this class instance)
    *           IN  iStartFromCustomer  - first customer id
    *           IN  iTotalCustomers     - total number of customers in the database
    *           IN  iLoadUnitSize       - minimal number of customers that can be build (should always be 1000)
    *           IN  iScaleFactor        - number of customers for 1tpsE
    *           IN  iDaysOfInitialTrades- number of 8-hour days of initial trades per customer
    *           IN  pLoaderFactory      - factory to create loader classes
    *           IN  pLogger             - parameter logging interface
    *           IN  pOutput             - interface to output information to a user during the build process
    *           IN  szInDir             - input flat file directory needed for tables loaded from flat files
    *           IN  bCacheEnabled           - whether or not to use caching during data generation
    *
    *  RETURNS:
    *           not applicable.
    */
    CGenerateAndLoad(CInputFiles                inputFiles,
                     TIdent                      iCustomerCount,
                     TIdent                      iStartFromCustomer,
                     TIdent                      iTotalCustomers,
                     UINT                        iLoadUnitSize,
                     UINT                        iScaleFactor,
                     UINT                        iDaysOfInitialTrades,
                     CBaseLoaderFactory*         pLoaderFactory,
                     CBaseLogger*                pLogger,
                     CGenerateAndLoadBaseOutput* pOutput,
                     char*                       szInDir,
                     bool                        bCacheEnabled = false
                    );

    /*
    *   Generate and load ADDRESS table.
    *
    *   PARAMETERS:
    *           none.
    *
    *   RETURNS:
    *           none.
    */
    void GenerateAndLoadAddress();
    /*
    *   Generate and load CHARGE table.
    *
    *   PARAMETERS:
    *           none.
    *
    *   RETURNS:
    *           none.
    */
    void GenerateAndLoadCharge();
    /*
    *   Generate and load COMMISSION_RATE table.
    *
    *   PARAMETERS:
    *           none.
    *
    *   RETURNS:
    *           none.
    */
    void GenerateAndLoadCommissionRate();
    /*
    *   Generate and load COMPANY_COMPETITOR table.
    *
    *   PARAMETERS:
    *           none.
    *
    *   RETURNS:
    *           none.
    */
    void GenerateAndLoadCompanyCompetitor();
    /*
    *   Generate and load COMPANY table.
    *
    *   PARAMETERS:
    *           none.
    *
    *   RETURNS:
    *           none.
    */
    void GenerateAndLoadCompany();
    /*
    *   Generate and load CUSTOMER_ACCOUNT, ACCOUNT_PERMISSION table.
    *
    *   PARAMETERS:
    *           none.
    *
    *   RETURNS:
    *           none.
    */
    void GenerateAndLoadCustomerAccountAndAccountPermission();
    /*
    *   Generate and load CUSTOMER table.
    *
    *   PARAMETERS:
    *           none.
    *
    *   RETURNS:
    *           none.
    */
    void GenerateAndLoadCustomer();
    /*
    *   Generate and load CUSTOMER_TAXRATE table.
    *
    *   PARAMETERS:
    *           none.
    *
    *   RETURNS:
    *           none.
    */
    void GenerateAndLoadCustomerTaxrate();
    /*
    *   Generate and load DAILY_MARKET table.
    *
    *   PARAMETERS:
    *           none.
    *
    *   RETURNS:
    *           none.
    */
    void GenerateAndLoadDailyMarket();
    /*
    *   Generate and load EXCHANGE table.
    *
    *   PARAMETERS:
    *           none.
    *
    *   RETURNS:
    *           none.
    */
    void GenerateAndLoadExchange();
    /*
    *   Generate and load FINANCIAL table.
    *
    *   PARAMETERS:
    *           none.
    *
    *   RETURNS:
    *           none.
    */
    void GenerateAndLoadFinancial();
    /*
    *   Generate and load HOLDING, HOLDING_HISTORY, TRADE, TRADE_HISTORY, SETTLEMENT, CASH_TRANSACTION, BROKER table.
    *
    *   PARAMETERS:
    *           none.
    *
    *   RETURNS:
    *           none.
    */
    void GenerateAndLoadHoldingAndTrade();
    /*
    *   Generate and load INDUSTRY table.
    *
    *   PARAMETERS:
    *           none.
    *
    *   RETURNS:
    *           none.
    */
    void GenerateAndLoadIndustry();
    /*
    *   Generate and load LAST_TRADE table.
    *
    *   PARAMETERS:
    *           none.
    *
    *   RETURNS:
    *           none.
    */
    void GenerateAndLoadLastTrade();
    /*
    *   Generate and load NEWS_ITEM, NEWS_XREF table.
    *
    *   PARAMETERS:
    *           none.
    *
    *   RETURNS:
    *           none.
    */
    void GenerateAndLoadNewsItemAndNewsXRef();
    /*
    *   Generate and load SECTOR table.
    *
    *   PARAMETERS:
    *           none.
    *
    *   RETURNS:
    *           none.
    */
    void GenerateAndLoadSector();
    /*
    *   Generate and load SECURITY table.
    *
    *   PARAMETERS:
    *           none.
    *
    *   RETURNS:
    *           none.
    */
    void GenerateAndLoadSecurity();
    /*
    *   Generate and load STATUS_TYPE table.
    *
    *   PARAMETERS:
    *           none.
    *
    *   RETURNS:
    *           none.
    */
    void GenerateAndLoadStatusType();
    /*
    *   Generate and load TAXRATE table.
    *
    *   PARAMETERS:
    *           none.
    *
    *   RETURNS:
    *           none.
    */
    void GenerateAndLoadTaxrate();
    /*
    *   Generate and load TRADE_TYPE table.
    *
    *   PARAMETERS:
    *           none.
    *
    *   RETURNS:
    *           none.
    */
    void GenerateAndLoadTradeType();
    /*
    *   Generate and load WATCH_LIST, WATCH_ITEM table.
    *
    *   PARAMETERS:
    *           none.
    *
    *   RETURNS:
    *           none.
    */
    void GenerateAndLoadWatchListAndWatchItem();
    /*
    *   Generate and load ZIP_CODE table.
    *
    *   PARAMETERS:
    *           none.
    *
    *   RETURNS:
    *           none.
    */
    void GenerateAndLoadZipCode();

    /*
    *   Generate and load All tables that are constant in size.
    *
    *   Spec definition: Fixed tables.
    *
    *   PARAMETERS:
    *           none.
    *
    *   RETURNS:
    *           none.
    */
    void GenerateAndLoadFixedTables();

    /*
    *   Generate and load All tables (except BROKER) that scale with the size of
    *   the CUSTOMER table, but do not grow in runtime.
    *
    *   Spec definition: Scaling tables.
    *
    *   PARAMETERS:
    *           none.
    *
    *   RETURNS:
    *           none.
    */
    void GenerateAndLoadScalingTables();

    /*
    *   Generate and load All trade related tables and BROKER (included here to
    *   facilitate generation of a consistent database).
    *
    *   Spec definition: Growing tables.
    *
    *   PARAMETERS:
    *           none.
    *
    *   RETURNS:
    *           none.
    */
    void GenerateAndLoadGrowingTables();




	void InitAddress();	
	void InitCharge();
	void InitCommissionRate();
	void InitCompanyCompetitor();	
	void InitCompany();
	void InitCustomerAccountAndAccountPermission();
	void InitCustomer();
	void InitCustomerTaxrate();
	void InitDailyMarket();
	void InitExchange();
	void InitFinancial();
	void InitHoldingAndTrade();
	void InitIndustry();
	void InitLastTrade();
	void InitNewsItemAndNewsXRef();
	void InitSector();
	void InitSecurity();
	void InitStatusType();
	void InitTaxrate();
	void InitTradeType();
	void InitWatchListAndWatchItem();
	void InitZipCode();

	void ReleaseAddress();	
	void ReleaseCharge();
	void ReleaseCommissionRate();
	void ReleaseCompanyCompetitor();	
	void ReleaseCompany();
	void ReleaseCustomerAccountAndAccountPermission();
	void ReleaseCustomer();
	void ReleaseCustomerTaxrate();
	void ReleaseDailyMarket();
	void ReleaseExchange();
	void ReleaseFinancial();
	void ReleaseHoldingAndTrade();
	void ReleaseIndustry();
	void ReleaseLastTrade();
	void ReleaseNewsItemAndNewsXRef();
	void ReleaseSector();
	void ReleaseSecurity();
	void ReleaseStatusType();
	void ReleaseTaxrate();
	void ReleaseTradeType();
	void ReleaseWatchListAndWatchItem();
	void ReleaseZipCode();

	PADDRESS_ROW getAddressRow();
	PCHARGE_ROW getChargeRow();
	PCOMMISSION_RATE_ROW getCommissionRateRow();
	PCOMPANY_ROW getCompanyRow();
	PCOMPANY_COMPETITOR_ROW getCompanyCompetitorRow();
	PCUSTOMER_ROW getCustomerRow();
	PCUSTOMER_ACCOUNT_ROW getCustomerAccountRow();
	PACCOUNT_PERMISSION_ROW getAccountPermissionRow(int i);
	PCUSTOMER_TAXRATE_ROW getCustomerTaxrateRow(int i);
	PDAILY_MARKET_ROW getDailyMarketRow();
	PEXCHANGE_ROW getExchangeRow();
	PFINANCIAL_ROW getFinancialRow();
	PINDUSTRY_ROW getIndustryRow();
	PLAST_TRADE_ROW getLastTradeRow();
	PNEWS_ITEM_ROW getNewsItemRow(); 
	PNEWS_XREF_ROW getNewsXRefRow();
	PSECTOR_ROW getSectorRow();
	PSECURITY_ROW getSecurityRow();
	PSTATUS_TYPE_ROW getStatusTypeRow();
	PTAXRATE_ROW getTaxrateRow();
	PTRADE_TYPE_ROW getTradeTypeRow();
	PWATCH_LIST_ROW getWatchListRow();
	PWATCH_ITEM_ROW getWatchItemRow(int i);	
  	PZIP_CODE_ROW getZipCodeRow(); 



	PTRADE_ROW getTradeRow();
	PTRADE_REQUEST_ROW getTradeRequestRow();
	PTRADE_HISTORY_ROW getTradeHistoryRow(int i);
	PCASH_TRANSACTION_ROW getCashTransactionRow();
	PSETTLEMENT_ROW getSettlementRow ();
	PHOLDING_HISTORY_ROW getHoldingHistoryRow(int i);
	PHOLDING_SUMMARY_ROW getHoldingSummaryRow();
	PBROKER_ROW getBrokerRow();
	PHOLDING_ROW getHoldingRow();



	bool hasNextAddress();	
	bool isLastCharge();
	bool isLastCommissionRate();
	bool hasNextCompanyCompetitor();	
	bool hasNextCompany();
	bool hasNextCustomerAccount();
	int  PermissionsPerCustomer();
	bool hasNextCustomer();
	bool hasNextCustomerTaxrate();
	int  getTaxratesCount();
	bool hasNextDailyMarket();
	bool isLastExchange();
	bool hasNextFinancial();
	bool isLastIndustry();
	bool hasNextLastTrade();
	bool hasNextNewsItemAndNewsXRef();
	bool isLastSector();
	bool hasNextSecurity();
	bool isLastStatusType();
	bool hasNextTaxrate();
	bool isLastTradeType(); 
	bool hasNextWatchList();
	int ItemsPerWatchList();
	bool hasNextZipCode();

	bool hasNextLoadUnit();
	bool hasNextTrade();
	bool shouldProcessTradeRequestRow();
	int  getTradeHistoryRowCount();
	bool shouldProcessCashTransactionRow();
	bool shouldProcessSettlementRow();
	bool hasNextHolding();
	int  getHoldingHistoryRowCount();
	bool hasNextHoldingSummary();
	bool hasNextBroker();


	// Generate and load all tables whose size is independent of the number of customers.
	void InitFixedTables();

	// Generate and load all tables (except BROKER) that scale with
	// the number of customers, but do not grow during runtime.
	void InitScalingTables();

	// Generate and load trade related tables and BROKER.
	void InitGrowingTables();



};

}   // namespace TPCE

#endif //EGEN_GENERATE_AND_LOAD_H
