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
 * - Sergey Vasilevskiy, Matt Emmerton
 */

/******************************************************************************
*   Description:        Superstructure that contains all the input flat files used
*                       by the loader and the driver.
******************************************************************************/

#include "EGenTables_stdafx.h"

namespace TPCE
{

// Initialization Method
// eType:   Driver Type (EGen, CE, MEE, DM)
// szPathName:  C-string of fully qualified pathname to EGen input files.  Trailing slash optional.
bool CInputFiles::Initialize(eDriverType eType, TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount, const char *szPathName)
{
    eOutputVerbosity eOutput = (eType == eDriverEGenLoader) ? eOutputVerbose : eOutputQuiet;
    char    szFileName[iMaxPath];
    char    *pStartInFileName;  // start of the filename part in the szFileName buffer
    size_t iDirLen;
    size_t iFileNameMaxLen;

    // Load the input file directory into the input file name array
    // and set a pointer to the location in the input file name array
    // just after where the directory name ended. This location is then
    // used for loading particular file names into the array. This
    // optimization assumes that all input files are in the same
    // directory.

    strncpy(szFileName, szPathName, iMaxPath);
    iDirLen = strlen( szFileName );
    pStartInFileName = (char *)&szFileName[iDirLen];
    if (*pStartInFileName != '/' && *pStartInFileName != '\\') {
        strncat(szFileName, "/", std::min(sizeof(szFileName), iDirLen - 1));
        pStartInFileName++;
        iDirLen++;
    }
    iFileNameMaxLen = iMaxPath - iDirLen - 1;

    if (eOutput == eOutputVerbose) { cout<<"Loading input files:"<<endl<<endl<<flush; }
    CDateTime t1;

    //
    // Input Files required by All Driver Types (EGen, CE, MEE, DM)
    //

    if (eOutput == eOutputVerbose) { cout<<"\tSecurity..."; }
    strncpy(pStartInFileName, "Security.txt", iFileNameMaxLen);
    Securities = new CSecurityFile(szFileName, iConfiguredCustomerCount, iActiveCustomerCount);
    if (eOutput == eOutputVerbose) { cout<<".............loaded."<<endl<<flush; }

    if (eOutput == eOutputVerbose) { cout<<"\tStatusType..."; }
    strncpy(pStartInFileName, "StatusType.txt", iFileNameMaxLen);
    StatusType = new TStatusTypeFile(szFileName);
    if (eOutput == eOutputVerbose) { cout<<"...........loaded."<<endl<<flush; }

    //
    // Input files required by EGen, CE, MEE
    //

    if (eType != eDriverDM)
    {
        if (eOutput == eOutputVerbose) { cout<<"\tTradeType..."; }
        strncpy(pStartInFileName, "TradeType.txt", iFileNameMaxLen);
        TradeType = new TTradeTypeFile(szFileName);
        if (eOutput == eOutputVerbose) { cout<<"............loaded."<<endl<<flush; }
    }

    //
    // Input files required by EGen, CE, DM
    //

    if (eType != eDriverMEE)
    {
        if (eOutput == eOutputVerbose) { cout<<"\tCompany..."; }
        strncpy(pStartInFileName, "Company.txt", iFileNameMaxLen);
        Company = new CCompanyFile(szFileName, iConfiguredCustomerCount, iActiveCustomerCount);
        if (eOutput == eOutputVerbose) { cout<<"..............loaded."<<endl<<flush; }

        if (eOutput == eOutputVerbose) { cout<<"\tExchange..."; }
        strncpy(pStartInFileName, "Exchange.txt", iFileNameMaxLen);
        Exchange = new TExchangeFile(szFileName);
        if (eOutput == eOutputVerbose) { cout<<".............loaded."<<endl<<flush; }

        if (eOutput == eOutputVerbose) { cout<<"\tTaxRatesDivision..."; }
        strncpy(pStartInFileName, "TaxRatesDivision.txt", iFileNameMaxLen);
        TaxRatesDivision = new CInputFileNoWeight<TTaxRateInputRow>(szFileName);
        if (eOutput == eOutputVerbose) { cout<<".....loaded."<<endl<<flush; }
    }

    //
    // Input Files required by EGen, CE
    //

    if (eType != eDriverMEE && eType != eDriverDM)
    {
        if (eOutput == eOutputVerbose) { cout<<"\tFemaleFirstName..."; }
        strncpy(pStartInFileName, "FemaleFirstName.txt", iFileNameMaxLen);
        FemaleFirstNames = new TFemaleFirstNamesFile(szFileName);
        if (eOutput == eOutputVerbose) { cout<<"......loaded."<<endl<<flush; }

        if (eOutput == eOutputVerbose) { cout<<"\tIndustry..."; }
        strncpy(pStartInFileName, "Industry.txt", iFileNameMaxLen);
        Industry = new TIndustryFile(szFileName);
        if (eOutput == eOutputVerbose) { cout<<".............loaded."<<endl<<flush; }

        if (eOutput == eOutputVerbose) { cout<<"\tLastName..."; }
        strncpy(pStartInFileName, "LastName.txt", iFileNameMaxLen);
        LastNames = new TLastNamesFile(szFileName);
        if (eOutput == eOutputVerbose) { cout<<".............loaded."<<endl<<flush; }

        if (eOutput == eOutputVerbose) { cout<<"\tMaleFirstName..."; }
        strncpy(pStartInFileName, "MaleFirstName.txt", iFileNameMaxLen);
        MaleFirstNames = new TMaleFirstNamesFile(szFileName);
        if (eOutput == eOutputVerbose) { cout<<"........loaded."<<endl<<flush; }

        if (eOutput == eOutputVerbose) { cout<<"\tSector..."; }
        strncpy(pStartInFileName, "Sector.txt", iFileNameMaxLen);
        Sectors = new TSectorFile(szFileName);
        if (eOutput == eOutputVerbose) { cout<<"...............loaded."<<endl<<flush; }
    }

    //
    // Input Files required by EGenLoader
    //

    if (eType != eDriverMEE && eType != eDriverDM && eType != eDriverCE && eType != eDriverAll)
    {
        if (eOutput == eOutputVerbose) { cout<<"\tAreaCode..."; }
        strncpy(pStartInFileName, "AreaCode.txt", iFileNameMaxLen);
        AreaCodes = new TAreaCodeFile(szFileName);
        if (eOutput == eOutputVerbose) { cout<<".............loaded."<<endl<<flush; }

        if (eOutput == eOutputVerbose) { cout<<"\tCharge..."; }
        strncpy(pStartInFileName, "Charge.txt", iFileNameMaxLen);
        Charge = new TChargeFile(szFileName);
        if (eOutput == eOutputVerbose) { cout<<"...............loaded."<<endl<<flush; }

        if (eOutput == eOutputVerbose) { cout<<"\tCommissionRate..."; }
        strncpy(pStartInFileName, "CommissionRate.txt", iFileNameMaxLen);
        CommissionRate = new TCommissionRateFile(szFileName);
        if (eOutput == eOutputVerbose) { cout<<".......loaded."<<endl<<flush; }

        if (eOutput == eOutputVerbose) { cout<<"\tCompanyCompetitor.."; }
        strncpy(pStartInFileName, "CompanyCompetitor.txt", iFileNameMaxLen);
        CompanyCompetitor = new CCompanyCompetitorFile(szFileName, iConfiguredCustomerCount, iActiveCustomerCount);
        if (eOutput == eOutputVerbose) { cout<<".....loaded."<<endl<<flush; }

        if (eOutput == eOutputVerbose) { cout<<"\tCompanySPRate..."; }
        strncpy(pStartInFileName, "CompanySPRate.txt", iFileNameMaxLen);
        CompanySPRate = new TCompanySPRateFile(szFileName);
        if (eOutput == eOutputVerbose) { cout<<"........loaded."<<endl<<flush; }

        if (eOutput == eOutputVerbose) { cout<<"\tLastName..."; }
        strncpy(pStartInFileName, "LastName.txt", iFileNameMaxLen);
        News = new TNewsFile(szFileName);
        if (eOutput == eOutputVerbose) { cout<<".............loaded."<<endl<<flush; }

        if (eOutput == eOutputVerbose) { cout<<"\tStreetName..."; }
        strncpy(pStartInFileName, "StreetName.txt", iFileNameMaxLen);
        Street = new TStreetNamesFile(szFileName);
        if (eOutput == eOutputVerbose) { cout<<"...........loaded."<<endl<<flush; }

        if (eOutput == eOutputVerbose) { cout<<"\tStreetSuffix..."; }
        strncpy(pStartInFileName, "StreetSuffix.txt", iFileNameMaxLen);
        StreetSuffix = new TStreetSuffixFile(szFileName);
        if (eOutput == eOutputVerbose) { cout<<".........loaded."<<endl<<flush; }

        if (eOutput == eOutputVerbose) { cout<<"\tTaxableAccountName..."; }
        strncpy(pStartInFileName, "TaxableAccountName.txt", iFileNameMaxLen);
        TaxableAccountName = new TTaxableAccountNameFile(szFileName);
        if (eOutput == eOutputVerbose) { cout<<"...loaded."<<endl<<flush; }

        if (eOutput == eOutputVerbose) { cout<<"\tNonTaxableAccountName..."; }
        strncpy(pStartInFileName, "NonTaxableAccountName.txt", iFileNameMaxLen);
        NonTaxableAccountName = new TNonTaxableAccountNameFile(szFileName);
        if (eOutput == eOutputVerbose) { cout<<"loaded."<<endl<<flush; }

        if (eOutput == eOutputVerbose) { cout<<"\tTaxRatesCountry..."; }
        strncpy(pStartInFileName, "TaxRatesCountry.txt", iFileNameMaxLen);
        TaxRatesCountry = new CInputFileNoWeight<TTaxRateInputRow>(szFileName);
        if (eOutput == eOutputVerbose) { cout<<"......loaded."<<endl<<flush; }

        if (eOutput == eOutputVerbose) { cout<<"\tZipCode..."; }
        strncpy(pStartInFileName, "ZipCode.txt", iFileNameMaxLen);
        ZipCode = new TZipCodeFile(szFileName);
        if (eOutput == eOutputVerbose) { cout<<"..............loaded."<<endl<<flush; }
    }

    CDateTime t2;
    if (eOutput == eOutputVerbose) { cout<<endl<<"Loading complete ("<<t2-t1<<"sec)."<<endl<<endl; }

    szFileName[iDirLen] = '\0';

    return true;
}

}   // namespace TPCE
