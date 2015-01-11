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

#include "EGenGenerateAndLoad_stdafx.h"
#include "DriverParamSettings.h"
#include "stdio.h"

#ifdef COMPILE_FLAT_FILE_LOAD 
FILE *fsec, *fhs;
#endif
using namespace TPCE;

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
*
*  RETURNS:
*           not applicable.
*/
CGenerateAndLoad::CGenerateAndLoad(CInputFiles                  inputFiles,
                                  TIdent                        iCustomerCount,
                                  TIdent                        iStartFromCustomer,
                                  TIdent                        iTotalCustomers,
                                  UINT                          iLoadUnitSize,
                                  UINT                          iScaleFactor,
                                  UINT                          iDaysOfInitialTrades,
                                  CBaseLoaderFactory*           pLoaderFactory,
                                  CBaseLogger*                  pLogger,
                                  CGenerateAndLoadBaseOutput*   pOutput,
                                  char*                         szInDir,
                                  bool                          bCacheEnabled
                                 )
: m_inputFiles(inputFiles)
, m_iStartFromCustomer(iStartFromCustomer)
, m_iCustomerCount(iCustomerCount)
, m_iTotalCustomers(iTotalCustomers)
, m_iLoadUnitSize(iLoadUnitSize)
, m_iScaleFactor(iScaleFactor)
, m_iHoursOfInitialTrades(iDaysOfInitialTrades*HoursPerWorkDay)
, m_pLoaderFactory(pLoaderFactory)
, m_pOutput(pOutput)
, m_pLogger(pLogger)
, m_LoaderSettings(iTotalCustomers, iTotalCustomers, iStartFromCustomer, iCustomerCount, iScaleFactor, iDaysOfInitialTrades )
, m_bCacheEnabled(bCacheEnabled)
{
    // Copy input flat file directory needed for tables loaded from flat files.
    strncpy( m_szInDir, szInDir, sizeof(m_szInDir));

    // Log Parameters
    m_pLogger->SendToLogger(m_LoaderSettings);
}

/*
*   Generate and load ADDRESS table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CGenerateAndLoad::GenerateAndLoadAddress()
{
    bool                        bRet;
    CAddressTable               Table(m_inputFiles, m_iCustomerCount, m_iStartFromCustomer,
                                      // do not generate exchange and company addresses
                                      // if the starting customer is not 1
                                      m_iStartFromCustomer != iDefaultStartFromCustomer);
    CBaseLoader<ADDRESS_ROW>*   pLoad = m_pLoaderFactory->CreateAddressLoader();
    INT64                       iCnt=0;

    m_pOutput->OutputStart("Generating ADDRESS table...");

    pLoad->Init();

    do
    {
        bRet = Table.GenerateNextRecord();

        pLoad->WriteNextRecord(Table.GetRow());

        if (++iCnt % 20000 == 0)
        {   //output progress
            m_pOutput->OutputProgress(".");
        }

    } while (bRet);

    pLoad->FinishLoad();    //commit
    delete pLoad;

    m_pOutput->OutputComplete("loaded.");
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and load CHARGE table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CGenerateAndLoad::GenerateAndLoadCharge()
{
    bool                        bEndOfFile;
    CChargeTable                Table( m_szInDir );
    CBaseLoader<CHARGE_ROW>*    pLoad = m_pLoaderFactory->CreateChargeLoader();

    m_pOutput->OutputStart("Generating CHARGE table...");

    pLoad->Init();

    bEndOfFile = Table.GenerateNextRecord();
    while( !bEndOfFile )
    {
        pLoad->WriteNextRecord(Table.GetRow());
        bEndOfFile = Table.GenerateNextRecord();
    }
    pLoad->FinishLoad();    //commit
    delete pLoad;

    m_pOutput->OutputComplete("loaded.");
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and load COMMISSION_RATE table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CGenerateAndLoad::GenerateAndLoadCommissionRate()
{
    bool                                bEndOfFile;
    CCommissionRateTable                Table( m_szInDir );
    CBaseLoader<COMMISSION_RATE_ROW>*   pLoad = m_pLoaderFactory->CreateCommissionRateLoader();

    m_pOutput->OutputStart("Generating COMMISSION_RATE table...");

    pLoad->Init();

    bEndOfFile = Table.GenerateNextRecord();
    while( !bEndOfFile )
    {
        pLoad->WriteNextRecord(Table.GetRow());
        bEndOfFile = Table.GenerateNextRecord();
    }
    pLoad->FinishLoad();    //commit
    delete pLoad;

    m_pOutput->OutputComplete("loaded.");
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and load COMPANY table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CGenerateAndLoad::GenerateAndLoadCompany()
{
    bool                            bRet;
    CCompanyTable                   Table(m_inputFiles, m_iCustomerCount, m_iStartFromCustomer);
    CBaseLoader<COMPANY_ROW>*       pLoad = m_pLoaderFactory->CreateCompanyLoader();

    m_pOutput->OutputStart("Generating COMPANY table...");

    pLoad->Init();

    do
    {
        bRet = Table.GenerateNextRecord();

        pLoad->WriteNextRecord(Table.GetRow());

    } while (bRet);

    pLoad->FinishLoad();    //commit

    delete pLoad;

    m_pOutput->OutputComplete("loaded.");
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and load COMPANY_COMPETITOR table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CGenerateAndLoad::GenerateAndLoadCompanyCompetitor()
{
    bool                                    bRet;
    CCompanyCompetitorTable                 Table(m_inputFiles, m_iCustomerCount, m_iStartFromCustomer);
    CBaseLoader<COMPANY_COMPETITOR_ROW>*    pLoad = m_pLoaderFactory->CreateCompanyCompetitorLoader();

    m_pOutput->OutputStart("Generating COMPANY_COMPETITOR table...");

    pLoad->Init();

    do
    {
        bRet = Table.GenerateNextRecord();

        pLoad->WriteNextRecord(Table.GetRow());

    } while (bRet);

    pLoad->FinishLoad();    //commit

    delete pLoad;

    m_pOutput->OutputComplete("loaded.");
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and load CUSTOMER table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CGenerateAndLoad::GenerateAndLoadCustomer()
{
    bool                        bRet;
    CCustomerTable              Table(m_inputFiles, m_iCustomerCount, m_iStartFromCustomer);
    CBaseLoader<CUSTOMER_ROW>*  pLoad = m_pLoaderFactory->CreateCustomerLoader();
    INT64                       iCnt=0;

    m_pOutput->OutputStart("Generating CUSTOMER table...");

    pLoad->Init();

    do
    {
        bRet = Table.GenerateNextRecord();

        pLoad->WriteNextRecord(Table.GetRow());

        if (++iCnt % 20000 == 0)
        {
            m_pOutput->OutputProgress("."); //output progress
        }

    } while (bRet);

    pLoad->FinishLoad();    //commit

    delete pLoad;

    m_pOutput->OutputComplete("loaded.");
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and load CUSTOMER_ACCOUNT, ACCOUNT_PERMISSION table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CGenerateAndLoad::GenerateAndLoadCustomerAccountAndAccountPermission()
{
    bool                                    bRet;
    CCustomerAccountsAndPermissionsTable    Table(m_inputFiles, m_iLoadUnitSize, m_iCustomerCount, m_iStartFromCustomer);
    CBaseLoader<CUSTOMER_ACCOUNT_ROW>*      pCALoad = m_pLoaderFactory->CreateCustomerAccountLoader();
    CBaseLoader<ACCOUNT_PERMISSION_ROW>*    pAPLoad = m_pLoaderFactory->CreateAccountPermissionLoader();
    INT64                                   iCnt=0;
    UINT                                    i;

    m_pOutput->OutputStart("Generating CUSTOMER_ACCOUNT table and ACCOUNT_PERMISSION table...");

    pCALoad->Init();
    pAPLoad->Init();

    do
    {
        bRet = Table.GenerateNextRecord();

        pCALoad->WriteNextRecord(Table.GetCARow());

        for(i=0; i<Table.GetCAPermsCount(); ++i)
        {

            pAPLoad->WriteNextRecord(Table.GetAPRow(i));

        }

        if (++iCnt % 10000 == 0)
        {
            m_pOutput->OutputProgress("."); //output progress
        }

        // Commit rows every so often
        if (iCnt % 10000 == 0)
        {
            pCALoad->Commit();
            pAPLoad->Commit();
        }

    } while (bRet);
    pCALoad->FinishLoad();  //commit
    pAPLoad->FinishLoad();  //commit
    delete pCALoad;
    delete pAPLoad;

    m_pOutput->OutputComplete("loaded.");
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and load CUSTOMER_TAXRATE table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CGenerateAndLoad::GenerateAndLoadCustomerTaxrate()
{
    bool                                bRet;
    CCustomerTaxratesTable              Table(m_inputFiles, m_iCustomerCount, m_iStartFromCustomer);
    CBaseLoader<CUSTOMER_TAXRATE_ROW>*  pLoad = m_pLoaderFactory->CreateCustomerTaxrateLoader();
    INT64                               iCnt=0;
    UINT                                 i;

    m_pOutput->OutputStart("Generating CUSTOMER_TAX_RATE table...");

    pLoad->Init();

    do
    {
        bRet = Table.GenerateNextRecord();

        for (i=0; i<Table.GetTaxRatesCount(); ++i)
        {
            pLoad->WriteNextRecord(Table.GetRowByIndex(i));

            if (++iCnt % 20000 == 0)
            {
                m_pOutput->OutputProgress("."); //output progress
            }
        }
    } while (bRet);

    pLoad->FinishLoad();    //commit

    delete pLoad;

    m_pOutput->OutputComplete("loaded.");
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and load DAILY_MARKET table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CGenerateAndLoad::GenerateAndLoadDailyMarket()
{
    bool                            bRet;
    CDailyMarketTable               Table(m_inputFiles, m_iCustomerCount, m_iStartFromCustomer);
    CBaseLoader<DAILY_MARKET_ROW>*  pLoad = m_pLoaderFactory->CreateDailyMarketLoader();
    INT64                           iCnt=0;

    m_pOutput->OutputStart("Generating DAILY_MARKET table...");

    pLoad->Init();

    do
    {
        bRet = Table.GenerateNextRecord();

        pLoad->WriteNextRecord(Table.GetRow());

        if (++iCnt % 20000 == 0)
        {
            m_pOutput->OutputProgress("."); //output progress
        }

        if (iCnt % 6525 == 0)
        {
            pLoad->Commit();    // commit rows every 5 securities (1305 rows/security)
        }

    } while (bRet);

    pLoad->FinishLoad();    //commit

    delete pLoad;

    m_pOutput->OutputComplete("loaded.");
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and load EXCHANGE table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CGenerateAndLoad::GenerateAndLoadExchange()
{
    bool                        bEndOfFile;
    CExchangeTable              Table( m_szInDir, m_iTotalCustomers );
    CBaseLoader<EXCHANGE_ROW>*  pLoad = m_pLoaderFactory->CreateExchangeLoader();

    m_pOutput->OutputStart("Generating EXCHANGE table...");

    pLoad->Init();

    bEndOfFile = Table.GenerateNextRecord();
    while( !bEndOfFile )
    {
        pLoad->WriteNextRecord(Table.GetRow());
        bEndOfFile = Table.GenerateNextRecord();
    }
    pLoad->FinishLoad();    //commit
    delete pLoad;

    m_pOutput->OutputComplete("loaded.");
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and load FINANCIAL table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CGenerateAndLoad::GenerateAndLoadFinancial()
{
    bool                            bRet;
    CFinancialTable                 Table(m_inputFiles, m_iCustomerCount, m_iStartFromCustomer);
    CBaseLoader<FINANCIAL_ROW>*     pLoad = m_pLoaderFactory->CreateFinancialLoader();
    INT64                           iCnt=0;

    m_pOutput->OutputStart("Generating FINANCIAL table...");

    pLoad->Init();

    do
    {
        bRet = Table.GenerateNextRecord();

        pLoad->WriteNextRecord(Table.GetRow());

        if (++iCnt % 20000 == 0)
        {
            m_pOutput->OutputProgress("."); //output progress
        }

        if (iCnt % 5000 == 0)
        {
            pLoad->Commit();    // commit rows every 250 companies (20 rows/company)
        }

    } while (bRet);

    pLoad->FinishLoad();    //commit

    delete pLoad;

    m_pOutput->OutputComplete("loaded.");
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and load HOLDING, HOLDING_HISTORY, TRADE, TRADE_HISTORY, SETTLEMENT, CASH_TRANSACTION, BROKER table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CGenerateAndLoad::GenerateAndLoadHoldingAndTrade()
{
    bool                                bRet;
    CTradeGen*                          pTradeGen;

    CBaseLoader<HOLDING_ROW>*           pHoldingsLoad;
    CBaseLoader<HOLDING_HISTORY_ROW>*   pHoldingHistoryLoad;
    CBaseLoader<HOLDING_SUMMARY_ROW>*   pHoldingSummaryLoad;
    CBaseLoader<TRADE_ROW>*             pTradesLoad;
    // Not loading TRADE_REQUEST table (it'll be quickly populated during a run)
    //CBaseLoader<TRADE_REQUEST_ROW>*       pRequestsLoad;
    CBaseLoader<SETTLEMENT_ROW>*        pSettlementLoad;
    CBaseLoader<TRADE_HISTORY_ROW>*     pHistoryLoad;
    CBaseLoader<CASH_TRANSACTION_ROW>*  pCashLoad;
    CBaseLoader<BROKER_ROW>*            pBrokerLoad;
    int                                 iCnt=0;
    int                                 i;
    int                                 iCurrentLoadUnit = 1;
    char                                szCurrentLoadUnit[11];

    pHoldingsLoad = m_pLoaderFactory->CreateHoldingLoader();
    pHoldingHistoryLoad = m_pLoaderFactory->CreateHoldingHistoryLoader();
    pHoldingSummaryLoad = m_pLoaderFactory->CreateHoldingSummaryLoader();
    pTradesLoad = m_pLoaderFactory->CreateTradeLoader();
    // Not loading TRADE_REQUEST table (it'll be quickly populated during a run)
    //pRequestsLoad = m_pLoaderFactory->CreateTradeRequestLoader();
    pSettlementLoad = m_pLoaderFactory->CreateSettlementLoader();
    pHistoryLoad = m_pLoaderFactory->CreateTradeHistoryLoader();
    pCashLoad = m_pLoaderFactory->CreateCashTransactionLoader();
    pBrokerLoad = m_pLoaderFactory->CreateBrokerLoader();

    m_pOutput->OutputStart("Generating TRADE, SETTLEMENT, TRADE HISTORY, CASH TRANSACTION, "
                            "HOLDING_HISTORY, HOLDING_SUMMARY, HOLDING, and BROKER tables...");

    pTradeGen = new CTradeGen(m_inputFiles, m_iCustomerCount, m_iStartFromCustomer,
                              m_iTotalCustomers, m_iLoadUnitSize,
                              m_iScaleFactor, m_iHoursOfInitialTrades,
                              m_bCacheEnabled
                             );

    // Generate and load one load unit at a time.
    //
    do
    {
        pTradesLoad->Init();
        pSettlementLoad->Init();
        pHistoryLoad->Init();
        pCashLoad->Init();
        pBrokerLoad->Init();
        pHoldingHistoryLoad->Init();
        pHoldingsLoad->Init();
        pHoldingSummaryLoad->Init();
        // Not loading TRADE_REQUEST table
        //pRequestsLoad->Init();

        // Generate and load trades for this load unit.
        //
        do
        {
            bRet = pTradeGen->GenerateNextTrade();

            pTradesLoad->WriteNextRecord(pTradeGen->GetTradeRow());

            for ( i=0; i<pTradeGen->GetTradeHistoryRowCount(); ++i)
            {
                pHistoryLoad->WriteNextRecord(pTradeGen->GetTradeHistoryRow(i));
            }

            if ( pTradeGen->GetSettlementRowCount() )
            {
                pSettlementLoad->WriteNextRecord(pTradeGen->GetSettlementRow());
            }

            if ( pTradeGen->GetCashTransactionRowCount() )
            {
                pCashLoad->WriteNextRecord(pTradeGen->GetCashTransactionRow());
            }

            for ( i=0; i<pTradeGen->GetHoldingHistoryRowCount(); ++i)
            {
                pHoldingHistoryLoad->WriteNextRecord(pTradeGen->GetHoldingHistoryRow(i));
            }

            /*if ((pTradeGen->GetTradeRow())->m_iTradeStatus == eCompleted) // Not loading TRADE_REQUEST table
            {
            pRequestsLoad->WriteNextRecord(pTradeGen->GetTradeRequestRow());
            }*/

            if (++iCnt % 10000 == 0)
            {
                m_pOutput->OutputProgress("."); //output progress
            }

            // Commit rows every so often
            if (iCnt % 10000 == 0)
            {
                pTradesLoad->Commit();          //commit
                pSettlementLoad->Commit();      //commit
                pHistoryLoad->Commit();         //commit
                pCashLoad->Commit();
                pHoldingHistoryLoad->Commit();  //commit
                // Not loading TRADE_REQUEST table
                //pRequestsLoad->Commit();      //commit
            }

        } while (bRet);

        // After trades generate and load BROKER table.
        //
        do
        {
            bRet = pTradeGen->GenerateNextBrokerRecord();

            pBrokerLoad->WriteNextRecord(pTradeGen->GetBrokerRow());

            // Commit rows every so often
            if (++iCnt % 10000 == 0)
            {
                pBrokerLoad->Commit();      //commit
            }
        } while (bRet);

        m_pOutput->OutputProgress("t");

        //  Now generate and load HOLDING_SUMMARY rows for this load unit.
        //
        do
        {
            bRet = pTradeGen->GenerateNextHoldingSummaryRow();

            pHoldingSummaryLoad->WriteNextRecord(pTradeGen->GetHoldingSummaryRow());

            if (++iCnt % 10000 == 0)
            {
                m_pOutput->OutputProgress("."); //output progress
            }

            // Commit rows every so often
            if (iCnt % 10000 == 0)
            {
                pHoldingSummaryLoad->Commit();      //commit
            }
        } while (bRet);

        //  Now generate and load holdings for this load unit.
        //
        do
        {
            bRet = pTradeGen->GenerateNextHolding();

            pHoldingsLoad->WriteNextRecord(pTradeGen->GetHoldingRow());

            if (++iCnt % 10000 == 0)
            {
                m_pOutput->OutputProgress("."); //output progress
            }

            // Commit rows every so often
            if (iCnt % 10000 == 0)
            {
                pHoldingsLoad->Commit();        //commit
            }
        } while (bRet);

        pTradesLoad->FinishLoad();          //commit
        pSettlementLoad->FinishLoad();      //commit
        pHistoryLoad->FinishLoad();         //commit
        pCashLoad->FinishLoad();            //commit
        pBrokerLoad->FinishLoad();          //commit
        pHoldingHistoryLoad->FinishLoad();  //commit
        pHoldingsLoad->FinishLoad();        //commit
        pHoldingSummaryLoad->FinishLoad();  //commit
        // Not loading TRADE_REQUEST table
        //pRequestsLoad->FinishLoad();      //commit

        // Output unit number for information
        snprintf(szCurrentLoadUnit, sizeof(szCurrentLoadUnit), "%d", iCurrentLoadUnit++);

        m_pOutput->OutputProgress(szCurrentLoadUnit);

    } while (pTradeGen->InitNextLoadUnit());

    delete pHoldingsLoad;
    delete pHoldingHistoryLoad;
    delete pHoldingSummaryLoad;
    delete pTradesLoad;
    // Not loading TRADE_REQUEST table
    //delete pRequestsLoad;
    delete pSettlementLoad;
    delete pHistoryLoad;
    delete pCashLoad;
    delete pBrokerLoad;

    delete pTradeGen;

    m_pOutput->OutputComplete(".loaded.");
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and load INDUSTRY table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CGenerateAndLoad::GenerateAndLoadIndustry()
{
    bool                        bEndOfFile;
    CIndustryTable              Table( m_szInDir );
    CBaseLoader<INDUSTRY_ROW>*  pLoad = m_pLoaderFactory->CreateIndustryLoader();

    m_pOutput->OutputStart("Generating INDUSTRY table...");

    pLoad->Init();

    bEndOfFile = Table.GenerateNextRecord();
    while( !bEndOfFile )
    {
        pLoad->WriteNextRecord(Table.GetRow());
        bEndOfFile = Table.GenerateNextRecord();
    }
    pLoad->FinishLoad();    //commit
    delete pLoad;

    m_pOutput->OutputComplete("loaded.");
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and load LAST_TRADE table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CGenerateAndLoad::GenerateAndLoadLastTrade()
{
    bool    bRet;
    CLastTradeTable                 Table(m_inputFiles, m_iCustomerCount, m_iStartFromCustomer
                                          , m_iHoursOfInitialTrades);
    CBaseLoader<LAST_TRADE_ROW>*    pLoad = m_pLoaderFactory->CreateLastTradeLoader();

    m_pOutput->OutputStart("Generating LAST TRADE table...");

    pLoad->Init();

    do
    {
        bRet = Table.GenerateNextRecord();

        pLoad->WriteNextRecord(Table.GetRow());

    } while (bRet);

    pLoad->FinishLoad();    //commit

    delete pLoad;

    m_pOutput->OutputComplete("loaded.");
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and load NEWS_ITEM, NEWS_XREF table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CGenerateAndLoad::GenerateAndLoadNewsItemAndNewsXRef()
{
    bool                            bRet;
    // allocated on the heap because contains 100KB item
    CNewsItemAndXRefTable*          pTable = new CNewsItemAndXRefTable( m_inputFiles,
                                                                        m_iCustomerCount,
                                                                        m_iStartFromCustomer,
                                                                        m_iHoursOfInitialTrades);
    CBaseLoader<NEWS_ITEM_ROW>*     pNewsItemLoad = m_pLoaderFactory->CreateNewsItemLoader();
    CBaseLoader<NEWS_XREF_ROW>*     pNewsXRefLoad = m_pLoaderFactory->CreateNewsXRefLoader();
    INT64                           iCnt=0;

    m_pOutput->OutputStart("Generating NEWS_ITEM and NEWS_XREF table...");

    pNewsItemLoad->Init();
    pNewsXRefLoad->Init();

    do
    {
        bRet = pTable->GenerateNextRecord();

        pNewsItemLoad->WriteNextRecord(pTable->GetNewsItemRow());

        pNewsXRefLoad->WriteNextRecord(pTable->GetNewsXRefRow());

        if (++iCnt % 1000 == 0) // output progress every 1000 rows because each row generation takes a lot of time
        {
            m_pOutput->OutputProgress("."); //output progress

            pNewsItemLoad->Commit();
            pNewsXRefLoad->Commit();
        }

    } while (bRet);
    pNewsItemLoad->FinishLoad();    //commit
    pNewsXRefLoad->FinishLoad();    //commit
    delete pNewsItemLoad;
    delete pNewsXRefLoad;
    delete pTable;

    m_pOutput->OutputComplete("loaded.");
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and load SECTOR table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CGenerateAndLoad::GenerateAndLoadSector()
{
    bool                        bEndOfFile;
    CSectorTable                Table( m_szInDir );
    CBaseLoader<SECTOR_ROW>*    pLoad = m_pLoaderFactory->CreateSectorLoader();

    m_pOutput->OutputStart("Generating SECTOR table...");

    pLoad->Init();

    bEndOfFile = Table.GenerateNextRecord();
    while( !bEndOfFile )
    {
        pLoad->WriteNextRecord(Table.GetRow());
        bEndOfFile = Table.GenerateNextRecord();
    }
    pLoad->FinishLoad();    //commit
    delete pLoad;

    m_pOutput->OutputComplete("loaded.");
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and load SECURITY table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CGenerateAndLoad::GenerateAndLoadSecurity()
{
    bool                            bRet;
    CSecurityTable                  Table(m_inputFiles, m_iCustomerCount, m_iStartFromCustomer);
    CBaseLoader<SECURITY_ROW>*      pLoad = m_pLoaderFactory->CreateSecurityLoader();
    INT64                           iCnt=0;

    m_pOutput->OutputStart("Generating SECURITY table...");

    pLoad->Init();

    do
    {
        bRet = Table.GenerateNextRecord();

        pLoad->WriteNextRecord(Table.GetRow());

        if (++iCnt % 20000 == 0)
        {
            m_pOutput->OutputProgress("."); //output progress
        }

    } while (bRet);

    pLoad->FinishLoad();    //commit

    delete pLoad;

    m_pOutput->OutputComplete("loaded.");
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and load STATUS_TYPE table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CGenerateAndLoad::GenerateAndLoadStatusType()
{
    bool                            bEndOfFile;
    CStatusTypeTable                Table( m_szInDir );
    CBaseLoader<STATUS_TYPE_ROW>*   pLoad = m_pLoaderFactory->CreateStatusTypeLoader();

    m_pOutput->OutputStart("Generating STATUS_TYPE table...");

    pLoad->Init();

    bEndOfFile = Table.GenerateNextRecord();
    while( !bEndOfFile )
    {
        pLoad->WriteNextRecord(Table.GetRow());
        bEndOfFile = Table.GenerateNextRecord();
    }
    pLoad->FinishLoad();    //commit
    delete pLoad;

    m_pOutput->OutputComplete("loaded.");
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and load TAXRATE table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CGenerateAndLoad::GenerateAndLoadTaxrate()
{
    bool                        bMoreRecords;
    CTaxrateTable               Table( m_inputFiles );
    CBaseLoader<TAXRATE_ROW>*   pLoad = m_pLoaderFactory->CreateTaxrateLoader();

    m_pOutput->OutputStart("Generating TAXRATE table...");

    pLoad->Init();

    do
    {
        bMoreRecords = Table.GenerateNextRecord();

        pLoad->WriteNextRecord(Table.GetRow());

    } while( bMoreRecords );

    pLoad->FinishLoad();    //commit
    delete pLoad;

    m_pOutput->OutputComplete("loaded.");
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and load TRADE_TYPE table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CGenerateAndLoad::GenerateAndLoadTradeType()
{
    bool                            bEndOfFile;
    CTradeTypeTable                 Table( m_szInDir );
    CBaseLoader<TRADE_TYPE_ROW>*    pLoad = m_pLoaderFactory->CreateTradeTypeLoader();

    m_pOutput->OutputStart("Generating TRADE_TYPE table...");

    pLoad->Init();

    bEndOfFile = Table.GenerateNextRecord();
    while( !bEndOfFile )
    {
        pLoad->WriteNextRecord(Table.GetRow());
        bEndOfFile = Table.GenerateNextRecord();
    }
    pLoad->FinishLoad();    //commit
    delete pLoad;

    m_pOutput->OutputComplete("loaded.");
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and load WATCH_LIST, WATCH_ITEM table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CGenerateAndLoad::GenerateAndLoadWatchListAndWatchItem()
{
    bool                            bRet;
    CWatchListsAndItemsTable        Table(m_inputFiles, m_iCustomerCount, m_iStartFromCustomer);
    CBaseLoader<WATCH_LIST_ROW>*    pWatchListsLoad = m_pLoaderFactory->CreateWatchListLoader();
    CBaseLoader<WATCH_ITEM_ROW>*    pWatchItemsLoad = m_pLoaderFactory->CreateWatchItemLoader();
    INT64                           iCnt=0;
    UINT                            i;

    m_pOutput->OutputStart("Generating WATCH_LIST table and WATCH_ITEM table...");

    pWatchListsLoad->Init();
    pWatchItemsLoad->Init();

    do
    {
        bRet = Table.GenerateNextRecord();

        pWatchListsLoad->WriteNextRecord(Table.GetWLRow());

        for (i=0; i<Table.GetWICount(); ++i)
        {
            pWatchItemsLoad->WriteNextRecord(Table.GetWIRow(i));

            if (++iCnt % 20000 == 0)
            {
                m_pOutput->OutputProgress("."); //output progress

                pWatchListsLoad->Commit();  //commit
                pWatchItemsLoad->Commit();  //commit
            }
        }
    } while (bRet);

    pWatchListsLoad->FinishLoad();  //commit
    pWatchItemsLoad->FinishLoad();  //commit

    delete pWatchListsLoad;
    delete pWatchItemsLoad;

    m_pOutput->OutputComplete("loaded.");
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and load ZIP_CODE table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CGenerateAndLoad::GenerateAndLoadZipCode()
{
    CZipCodeTable               Table( m_inputFiles );
    CBaseLoader<ZIP_CODE_ROW>*  pLoad = m_pLoaderFactory->CreateZipCodeLoader();

    m_pOutput->OutputStart("Generating ZIP_CODE table...");

    pLoad->Init();

    while( Table.GenerateNextRecord() )
    {
        pLoad->WriteNextRecord(Table.GetRow());
    }
    pLoad->FinishLoad();    //commit
    delete pLoad;

    m_pOutput->OutputComplete("loaded.");
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

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
void CGenerateAndLoad::GenerateAndLoadFixedTables()
{
    // Tables from flat files first
    GenerateAndLoadCharge();
    GenerateAndLoadCommissionRate();
    GenerateAndLoadExchange();
    GenerateAndLoadIndustry();
    GenerateAndLoadSector();
    GenerateAndLoadStatusType();
    GenerateAndLoadTaxrate();
    GenerateAndLoadTradeType();
    GenerateAndLoadZipCode();
}

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
void CGenerateAndLoad::GenerateAndLoadScalingTables()
{
    // Customer-related tables
    //
    GenerateAndLoadAddress();
    GenerateAndLoadCustomer();
    GenerateAndLoadCustomerAccountAndAccountPermission();
    GenerateAndLoadCustomerTaxrate();
    GenerateAndLoadWatchListAndWatchItem();

    // Now security/company related tables
    //
    GenerateAndLoadCompany();
    GenerateAndLoadCompanyCompetitor();
    GenerateAndLoadDailyMarket();
    GenerateAndLoadFinancial();
    GenerateAndLoadLastTrade();
    GenerateAndLoadNewsItemAndNewsXRef();
    GenerateAndLoadSecurity();
}

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
void CGenerateAndLoad::GenerateAndLoadGrowingTables()
{
    GenerateAndLoadHoldingAndTrade();
}






// ADDRESS

void CGenerateAndLoad::InitAddress(){

	addressTable= new CAddressTable(m_inputFiles, m_iCustomerCount, m_iStartFromCustomer, m_iStartFromCustomer != iDefaultStartFromCustomer);
											// do not generate exchange and company addresses
											// if the starting customer is not 1
}

void CGenerateAndLoad::ReleaseAddress(){
	delete addressTable;
}

PADDRESS_ROW CGenerateAndLoad::getAddressRow()
{
	return (PADDRESS_ROW)addressTable->GetRow();
}


bool CGenerateAndLoad::hasNextAddress()
{
	return addressTable->GenerateNextRecord();
}


// CHARGE
void CGenerateAndLoad::InitCharge()
{
	chargeTable	= new CChargeTable( m_szInDir );
}

void CGenerateAndLoad::ReleaseCharge(){
	delete chargeTable;
}


PCHARGE_ROW CGenerateAndLoad::getChargeRow() 
{	
	return (PCHARGE_ROW)chargeTable->GetRow();
}

bool CGenerateAndLoad::isLastCharge()
{
	return chargeTable->GenerateNextRecord();
}


// COMMISSION_RATE
void CGenerateAndLoad::InitCommissionRate()
{
	commissionRateTable		= new		CCommissionRateTable( m_szInDir );

}
void CGenerateAndLoad::ReleaseCommissionRate(){
	delete commissionRateTable;
}
PCOMMISSION_RATE_ROW CGenerateAndLoad::getCommissionRateRow() 
{	
	return (PCOMMISSION_RATE_ROW)commissionRateTable->GetRow();
}

bool CGenerateAndLoad::isLastCommissionRate()
{
	return commissionRateTable->GenerateNextRecord();
}


// COMPANY
void CGenerateAndLoad::InitCompany()
{
	companyTable = new	CCompanyTable(m_inputFiles, m_iCustomerCount, m_iStartFromCustomer);
}
void CGenerateAndLoad::ReleaseCompany(){
	delete companyTable;
}

PCOMPANY_ROW CGenerateAndLoad::getCompanyRow() 
{	
	return (PCOMPANY_ROW)companyTable->GetRow();
}

bool CGenerateAndLoad::hasNextCompany()
{
	return companyTable->GenerateNextRecord();
}


// COMPANY_COMPETITOR
void CGenerateAndLoad::InitCompanyCompetitor()
{
	companyCompetitorTable=new				CCompanyCompetitorTable(m_inputFiles, m_iCustomerCount, m_iStartFromCustomer);

}
void CGenerateAndLoad::ReleaseCompanyCompetitor(){
	delete companyCompetitorTable;
}

PCOMPANY_COMPETITOR_ROW CGenerateAndLoad::getCompanyCompetitorRow() 
{	
	return (PCOMPANY_COMPETITOR_ROW)companyCompetitorTable->GetRow();
}

bool CGenerateAndLoad::hasNextCompanyCompetitor()
{
	return companyCompetitorTable->GenerateNextRecord();
}


// CUSTOMER
void CGenerateAndLoad::InitCustomer()
{
	customerTable = new CCustomerTable(m_inputFiles, m_iCustomerCount, m_iStartFromCustomer);
}

void CGenerateAndLoad::ReleaseCustomer(){
	delete customerTable;
}

PCUSTOMER_ROW CGenerateAndLoad::getCustomerRow() 
{	
	return (PCUSTOMER_ROW)customerTable->GetRow();
}

bool CGenerateAndLoad::hasNextCustomer()
{
	return customerTable->GenerateNextRecord();
}



// CUSTOMER_ACCOUNT and ACCOUNT_PERMISSION

void CGenerateAndLoad::InitCustomerAccountAndAccountPermission(){
	customerAccountsAndPermissionsTable	=new CCustomerAccountsAndPermissionsTable(m_inputFiles, m_iLoadUnitSize, m_iCustomerCount, m_iStartFromCustomer);
}

void CGenerateAndLoad::ReleaseCustomerAccountAndAccountPermission(){
	delete customerAccountsAndPermissionsTable;
}

PCUSTOMER_ACCOUNT_ROW CGenerateAndLoad::getCustomerAccountRow() 
{	
	return (PCUSTOMER_ACCOUNT_ROW)customerAccountsAndPermissionsTable->GetCARow();
}

bool CGenerateAndLoad::hasNextCustomerAccount()
{
	return customerAccountsAndPermissionsTable->GenerateNextRecord();
}



PACCOUNT_PERMISSION_ROW CGenerateAndLoad::getAccountPermissionRow(int i) 
{	
	return (PACCOUNT_PERMISSION_ROW)customerAccountsAndPermissionsTable->GetAPRow(i);
}

int CGenerateAndLoad::PermissionsPerCustomer()
{
	return customerAccountsAndPermissionsTable->GetCAPermsCount();
}

/* logic
do
	{
		bRet = Table.GenerateNextRecord();

		pCALoad->WriteNextRecord(Table.GetCARow());

		for(i=0; i<Table.GetCAPermsCount(); ++i)
		{
			pAPLoad->WriteNextRecord(Table.GetAPRow(i));
			
		}

		if (++iCnt % 10000 == 0)
		{
			m_pOutput->OutputProgress(".");	//output progress
		}

		// Commit rows every so often
		if (iCnt % 10000 == 0)
		{
			pCALoad->Commit();
			pAPLoad->Commit();
		}

	} while (bRet);
*/



// CUSTOMER_TAXRATE
void CGenerateAndLoad::InitCustomerTaxrate()
{
	customerTaxratesTable	= new			CCustomerTaxratesTable(m_inputFiles, m_iCustomerCount, m_iStartFromCustomer);

/* logic
	do
	{
		bRet = Table.GenerateNextRecord();

		for (i=0; i<Table.GetTaxRatesCount(); ++i)
		{
			pLoad->WriteNextRecord(Table.GetRow(i));

			if (++iCnt % 20000 == 0)
			{
				m_pOutput->OutputProgress(".");	//output progress
			}			
		}
	} while (bRet);
*/
}
void CGenerateAndLoad::ReleaseCustomerTaxrate(){
	delete customerTaxratesTable;
}

bool CGenerateAndLoad::hasNextCustomerTaxrate()
{
	return customerTaxratesTable->GenerateNextRecord();
}
int CGenerateAndLoad::getTaxratesCount()
{
	return customerTaxratesTable->GetTaxRatesCount();
}

PCUSTOMER_TAXRATE_ROW CGenerateAndLoad::getCustomerTaxrateRow(int i) 
{	
	return (PCUSTOMER_TAXRATE_ROW)customerTaxratesTable->GetRowByIndex(i);
}



// DAILY_MARKET
void CGenerateAndLoad::InitDailyMarket()
{
	dailyMarketTable = new				CDailyMarketTable(m_inputFiles, m_iCustomerCount, m_iStartFromCustomer);
}
void CGenerateAndLoad::ReleaseDailyMarket(){
	delete dailyMarketTable;
}

PDAILY_MARKET_ROW CGenerateAndLoad::getDailyMarketRow() 
{	
	return (PDAILY_MARKET_ROW)dailyMarketTable->GetRow();
}

bool CGenerateAndLoad::hasNextDailyMarket()
{
	return dailyMarketTable->GenerateNextRecord();
}

// EXCHANGE
void CGenerateAndLoad::InitExchange()
{
	exchangeTable = new		CExchangeTable( m_szInDir, m_iTotalCustomers );
}
void CGenerateAndLoad::ReleaseExchange(){
	delete exchangeTable;
}

PEXCHANGE_ROW CGenerateAndLoad::getExchangeRow() 
{	
	return (PEXCHANGE_ROW)exchangeTable->GetRow();
}

bool CGenerateAndLoad::isLastExchange()
{
	return exchangeTable->GenerateNextRecord();
}


// FINANCIAL
void CGenerateAndLoad::InitFinancial()
{
	financialTable		= new		CFinancialTable(m_inputFiles, m_iCustomerCount, m_iStartFromCustomer);	

}

void CGenerateAndLoad::ReleaseFinancial(){
	delete financialTable;
}

PFINANCIAL_ROW CGenerateAndLoad::getFinancialRow() 
{	
	return (PFINANCIAL_ROW)financialTable->GetRow();
}

bool CGenerateAndLoad::hasNextFinancial()
{
	return financialTable->GenerateNextRecord();
}


// HOLDING, HOLDING_HISTORY, HOLDING_SUMMARY, TRADE, TRADE_HISTORY, SETTLEMENT, CASH_TRANSACTION, BROKER
void CGenerateAndLoad::InitHoldingAndTrade()
{

       printf("\n\n customers= %lld, loadunit= %d\n\n", m_iTotalCustomers, m_iLoadUnitSize);

	pTradeGen = new CTradeGen(m_inputFiles, m_iCustomerCount, m_iStartFromCustomer, 
							  m_iTotalCustomers, m_iLoadUnitSize,
						  m_iScaleFactor, m_iHoursOfInitialTrades);
#ifdef COMPILE_FLAT_FILE_LOAD 	
	fhs=fopen("hsummary.txt","wt");
#endif
}

void CGenerateAndLoad::ReleaseHoldingAndTrade(){
	delete pTradeGen;
#ifdef COMPILE_FLAT_FILE_LOAD 
	fclose(fhs);
#endif
}

bool CGenerateAndLoad::hasNextLoadUnit()
{
	/*
	bool x=false;
	x =pTradeGen->InitNextLoadUnit();
	return x; 
	*/

	return pTradeGen->InitNextLoadUnit();
}

//TRADE
bool CGenerateAndLoad::hasNextTrade()
{
	return pTradeGen->GenerateNextTrade();
}

PTRADE_ROW CGenerateAndLoad::getTradeRow()
{
	return (PTRADE_ROW)pTradeGen->GetTradeRow();
}

//TRADE_REQUEST
bool CGenerateAndLoad::shouldProcessTradeRequestRow()
{
//return ((pTradeGen->GetTradeRow())->m_iTradeStatus == eCompleted);
	return false;
}

PTRADE_REQUEST_ROW CGenerateAndLoad::getTradeRequestRow()
{
//	return pTradeGen->GetTradeRequestRow();
	return 0;
}

//TRADE_HISTORY
PTRADE_HISTORY_ROW CGenerateAndLoad::getTradeHistoryRow(int i)
{
	return (PTRADE_HISTORY_ROW)pTradeGen->GetTradeHistoryRow(i);
}

int CGenerateAndLoad::getTradeHistoryRowCount()
{
	return pTradeGen->GetTradeHistoryRowCount();
}

//CASH_TRANSACTION
bool CGenerateAndLoad::shouldProcessCashTransactionRow()
{
	return  (pTradeGen->GetCashTransactionRowCount() != 0);
}

PCASH_TRANSACTION_ROW CGenerateAndLoad::getCashTransactionRow()
{
	return (PCASH_TRANSACTION_ROW)pTradeGen->GetCashTransactionRow();
}

//SETTLEMENT
bool CGenerateAndLoad::shouldProcessSettlementRow()
{
	return (pTradeGen->GetSettlementRowCount() != 0);

}
PSETTLEMENT_ROW CGenerateAndLoad::CGenerateAndLoad::getSettlementRow ()
{
	return (PSETTLEMENT_ROW)pTradeGen->GetSettlementRow();
}


//HOLDING
bool CGenerateAndLoad::hasNextHolding()
{
	return pTradeGen->GenerateNextHolding();
}

PHOLDING_ROW CGenerateAndLoad::getHoldingRow()
{
	return (PHOLDING_ROW)pTradeGen->GetHoldingRow();
}

//HOLDING_HISTORY
PHOLDING_HISTORY_ROW CGenerateAndLoad::getHoldingHistoryRow(int i)
{
  return (PHOLDING_HISTORY_ROW)pTradeGen->GetHoldingHistoryRow(i);
}

int CGenerateAndLoad::getHoldingHistoryRowCount()
{
	return pTradeGen->GetHoldingHistoryRowCount();
}

//HOLDING_SUMMARY
bool  CGenerateAndLoad::hasNextHoldingSummary()
{
	
	return pTradeGen->GenerateNextHoldingSummaryRow();

}

PHOLDING_SUMMARY_ROW CGenerateAndLoad::getHoldingSummaryRow()
{
	PHOLDING_SUMMARY_ROW xx= (PHOLDING_SUMMARY_ROW)pTradeGen->GetHoldingSummaryRow();
#ifdef COMPILE_FLAT_FILE_LOAD 
	fprintf(fhs,"%lld|%s|%d\n",xx->HS_CA_ID, xx->HS_S_SYMB, xx->HS_QTY); 
#endif
	return xx; 
}

//BROKER
bool CGenerateAndLoad::hasNextBroker()
{
	return pTradeGen->GenerateNextBrokerRecord();
}

PBROKER_ROW CGenerateAndLoad::getBrokerRow()
{
	return (PBROKER_ROW )pTradeGen->GetBrokerRow();																		
}

/* logic - to be implemented in transaction!
{
							
	
	// Not loading TRADE_REQUEST table (it'll be quickly populated during a run)

	// Generate and load one load unit at a time.
	do
	{
		// Generate and load trades for this load unit.
		//
		do
		{	
			bRet = pTradeGen->GenerateNextTrade();

			pTradesLoad->WriteNextRecord(pTradeGen->GetTradeRow());

			for ( i=0; i<pTradeGen->GetTradeHistoryRowCount(); ++i)
			{
				pHistoryLoad->WriteNextRecord(pTradeGen->GetTradeHistoryRow(i));
			}

			if ( pTradeGen->GetSettlementRowCount() )
			{
				pSettlementLoad->WriteNextRecord(pTradeGen->GetSettlementRow());
			}

			if ( pTradeGen->GetCashTransactionRowCount() )
			{
				pCashLoad->WriteNextRecord(pTradeGen->GetCashTransactionRow());
			}

			for ( i=0; i<pTradeGen->GetHoldingHistoryRowCount(); ++i)
			{
				pHoldingHistoryLoad->WriteNextRecord(pTradeGen->GetHoldingHistoryRow(i));
			}

			//if ((pTradeGen->GetTradeRow())->m_iTradeStatus == eCompleted)	// Not loading TRADE_REQUEST table
			//{
			//pRequestsLoad->WriteNextRecord(pTradeGen->GetTradeRequestRow());
			//}

			if (++iCnt % 10000 == 0)
			{
				m_pOutput->OutputProgress(".");	//output progress
			}

			// Commit rows every so often
			if (iCnt % 10000 == 0)
			{
				pTradesLoad->Commit();			//commit		
				pSettlementLoad->Commit();		//commit
				pHistoryLoad->Commit();			//commit
				pCashLoad->Commit();
				pHoldingHistoryLoad->Commit();	//commit
				// Not loading TRADE_REQUEST table
				//pRequestsLoad->Commit();		//commit
			}

		} while (bRet);		

		// After trades generate and load BROKER table.
		//
		do
		{newsItemBuffer
			bRet = pTradeGen->GenerateNextBrokerRecord();

			pBrokerLoad->WriteNextRecord(pTradeGen->GetBrokerRow());

			// Commit rows every so often
			if (++iCnt % 10000 == 0)
			{
				pBrokerLoad->Commit();		//commit						
			}
		} while (bRet);

		m_pOutput->OutputProgress("t");

		//	Now generate and load HOLDING_SUMMARY rows for this load unit.
		//
		do 
		{
			bRet = pTradeGen->GenerateNextHoldingSummaryRow();

			pHoldingSummaryLoad->WriteNextRecord(pTradeGen->GetHoldingSummaryRow());

			if (++iCnt % 10000 == 0)
			{
				m_pOutput->OutputProgress(".");	//output progress
			}

			// Commit rows every so often
			if (iCnt % 10000 == 0)
			{
				pHoldingSummaryLoad->Commit();		//commit						
			}
		} while (bRet);		

		//	Now generate and load holdings for this load unit.
		//
		do 
		{
			bRet = pTradeGen->GenerateNextHolding();

			pHoldingsLoad->WriteNextRecord(pTradeGen->GetHoldingRow());

			if (++iCnt % 10000 == 0)
			{
				m_pOutput->OutputProgress(".");	//output progress
			}

			// Commit rows every so often
			if (iCnt % 10000 == 0)
			{
				pHoldingsLoad->Commit();		//commit						
			}
		} while (bRet);		

		// Output unit number for information
		sprintf(szCurrentLoadUnit, "%d", iCurrentLoadUnit++);

		m_pOutput->OutputProgress(szCurrentLoadUnit);

	} while (pTradeGen->InitNextLoadUnit());		


	delete pTradeGen;

	m_pOutput->OutputComplete(".loaded.");
	m_pOutput->OutputNewline();
	m_pOutput->OutputNewline();
}
*/






// INDUSTRY
void CGenerateAndLoad::InitIndustry()
{
	industryTable = new	CIndustryTable( m_szInDir );
}
void CGenerateAndLoad::ReleaseIndustry(){
	delete industryTable;
}

PINDUSTRY_ROW CGenerateAndLoad::getIndustryRow() 
{	
	return (PINDUSTRY_ROW)industryTable->GetRow();
}

bool CGenerateAndLoad::isLastIndustry()
{
	return industryTable->GenerateNextRecord();
}


// LAST_TRADE
void CGenerateAndLoad::InitLastTrade()
{
	lastTradeTable	= new	CLastTradeTable(m_inputFiles, m_iCustomerCount, m_iStartFromCustomer , m_iHoursOfInitialTrades);
}
void CGenerateAndLoad::ReleaseLastTrade(){
	delete lastTradeTable;
}

PLAST_TRADE_ROW CGenerateAndLoad::getLastTradeRow() 
{	
	return (PLAST_TRADE_ROW)lastTradeTable->GetRow();
}

bool CGenerateAndLoad::hasNextLastTrade()
{
	return lastTradeTable->GenerateNextRecord();
}



// NEWS_ITEM, NEWS_XREF
void CGenerateAndLoad::InitNewsItemAndNewsXRef()
{
	newsItemAndXRefTable  = new  CNewsItemAndXRefTable(m_inputFiles, m_iCustomerCount,  m_iStartFromCustomer, m_iHoursOfInitialTrades);
}

void CGenerateAndLoad::ReleaseNewsItemAndNewsXRef(){
	delete newsItemAndXRefTable;
}

PNEWS_ITEM_ROW CGenerateAndLoad::getNewsItemRow() 
{	
	return (PNEWS_ITEM_ROW)newsItemAndXRefTable->GetNewsItemRow();
}

PNEWS_XREF_ROW CGenerateAndLoad::getNewsXRefRow() 
{	
	return (PNEWS_XREF_ROW)newsItemAndXRefTable->GetNewsXRefRow();
}

bool CGenerateAndLoad::hasNextNewsItemAndNewsXRef()
{
	return newsItemAndXRefTable->GenerateNextRecord();
}


// SECTOR
void CGenerateAndLoad::InitSector()
{
	sectorTable	= new		CSectorTable( m_szInDir );
}

void CGenerateAndLoad::ReleaseSector(){
	delete sectorTable;
}

PSECTOR_ROW CGenerateAndLoad::getSectorRow() 
{	
	return (PSECTOR_ROW)sectorTable->GetRow();
}

bool CGenerateAndLoad::isLastSector()
{
	return sectorTable->GenerateNextRecord();
}


// SECURITY
void CGenerateAndLoad::InitSecurity()
{
	securityTable = new	CSecurityTable(m_inputFiles, m_iCustomerCount, m_iStartFromCustomer);
#ifdef COMPILE_FLAT_FILE_LOAD 
	fsec = fopen ("security.txt", "wt");
#endif
}
void CGenerateAndLoad::ReleaseSecurity(){
	delete securityTable;
#ifdef COMPILE_FLAT_FILE_LOAD 
	fclose(fsec);
#endif
}

PSECURITY_ROW CGenerateAndLoad::getSecurityRow() 
{	
	PSECURITY_ROW x = (PSECURITY_ROW)securityTable->GetRow();
#ifdef COMPILE_FLAT_FILE_LOAD 
	fprintf(fsec,"%s|%s|%s|%s\n",x->S_SYMB, x->S_ISSUE, x->S_ST_ID, x->S_NAME); 
#endif
	return x;
}

bool CGenerateAndLoad::hasNextSecurity()
{
	return securityTable->GenerateNextRecord();
}

// STATUS_TYPE
void CGenerateAndLoad::InitStatusType()
{
	statusTypeTable = new	CStatusTypeTable( m_szInDir );
}
void CGenerateAndLoad::ReleaseStatusType(){
	delete statusTypeTable;
}

PSTATUS_TYPE_ROW CGenerateAndLoad::getStatusTypeRow() 
{	
	return (PSTATUS_TYPE_ROW)statusTypeTable->GetRow();
}

bool CGenerateAndLoad::isLastStatusType()
{
	return statusTypeTable->GenerateNextRecord();
}


// TAXRATE
void CGenerateAndLoad::InitTaxrate(){
	taxrateTable = new CTaxrateTable( m_inputFiles );
}

void CGenerateAndLoad::ReleaseTaxrate(){
	delete taxrateTable;
}

PTAXRATE_ROW CGenerateAndLoad::getTaxrateRow() 
{	
	return (PTAXRATE_ROW)taxrateTable->GetRow();
}

bool CGenerateAndLoad::hasNextTaxrate()
{
	return taxrateTable->GenerateNextRecord();
}



// TRADE_TYPE
void CGenerateAndLoad::InitTradeType()
{
	tradeTypeTable = new  CTradeTypeTable( m_szInDir );
}

void CGenerateAndLoad::ReleaseTradeType(){
	delete tradeTypeTable;
}

PTRADE_TYPE_ROW CGenerateAndLoad::getTradeTypeRow() 
{	
	return (PTRADE_TYPE_ROW)tradeTypeTable->GetRow();
}

bool CGenerateAndLoad::isLastTradeType()
{
	return tradeTypeTable->GenerateNextRecord();
}



// WATCH_LIST and WATCH_ITEM
void CGenerateAndLoad::InitWatchListAndWatchItem()
{
	watchListsAndItemsTable = new   CWatchListsAndItemsTable(m_inputFiles, m_iCustomerCount, m_iStartFromCustomer);
}

void CGenerateAndLoad::ReleaseWatchListAndWatchItem(){
	delete watchListsAndItemsTable;
}

bool CGenerateAndLoad::hasNextWatchList()
{
	return watchListsAndItemsTable->GenerateNextRecord();
}


PWATCH_LIST_ROW CGenerateAndLoad::getWatchListRow() 
{	
	return (PWATCH_LIST_ROW)watchListsAndItemsTable->GetWLRow();
}

int CGenerateAndLoad::ItemsPerWatchList(){
 return watchListsAndItemsTable->GetWICount();
}


PWATCH_ITEM_ROW CGenerateAndLoad::getWatchItemRow(int i) 
{	
	return (PWATCH_ITEM_ROW)watchListsAndItemsTable->GetWIRow(i);
}
/* logic

    do
    {
        bRet = Table.GenerateNextRecord();

        pWatchListsLoad->WriteNextRecord(Table.GetWLRow());

        for (i=0; i<Table.GetWICount(); ++i)
        {
            pWatchItemsLoad->WriteNextRecord(Table.GetWIRow(i));

            if (++iCnt % 20000 == 0)
            {
                m_pOutput->OutputProgress("."); //output progress

                pWatchListsLoad->Commit();  //commit
                pWatchItemsLoad->Commit();  //commit
            }
        }
    } while (bRet);
*/


// ZIP_CODE
void CGenerateAndLoad::InitZipCode()
{
	zipCodeTable = new				CZipCodeTable( m_inputFiles );
}

void CGenerateAndLoad::ReleaseZipCode(){
	delete zipCodeTable;
}

PZIP_CODE_ROW CGenerateAndLoad::getZipCodeRow() 
{	
	return (PZIP_CODE_ROW)zipCodeTable->GetRow();
}

bool CGenerateAndLoad::hasNextZipCode()
{
	return zipCodeTable->GenerateNextRecord();
}



// All tables that are constant in size
//
// Spec definition: Fixed tables.
//
void CGenerateAndLoad::InitFixedTables()
{
	// Tables from flat files first
	InitCharge();
	InitCommissionRate();
	InitExchange();
	InitIndustry();
	InitSector();
	InitStatusType();
	InitTaxrate();
	InitTradeType();
	InitZipCode();	
}

// All tables (except BROKER) that scale with the size of 
// the CUSTOMER table, but do not grow in runtime. 
//
// Spec definition: Scaling tables.
//
void CGenerateAndLoad::InitScalingTables()
{
	// Customer-related tables
	//
	InitAddress();	
	InitCustomer();
	InitCustomerAccountAndAccountPermission();
	InitCustomerTaxrate();
	InitWatchListAndWatchItem();

	// Now security/company related tables
	//
	InitCompany();
	InitCompanyCompetitor();
	InitDailyMarket();
	InitFinancial();
	InitLastTrade();
	InitNewsItemAndNewsXRef();
	InitSecurity();
}

// All trade related tables and BROKER (included here to
// facilitate generation of a consistent database).
//
// Spec definition: Growing tables.
//
void CGenerateAndLoad::InitGrowingTables()
{
	InitHoldingAndTrade();
}
